"""
Reads a CSV that analyst uploads to genomic_set_upload bucket.
And insert to relevant genomic tables.
"""
import collections
import csv
import datetime
import logging
import os
import pytz

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file, list_blobs
from rdr_service.dao.genomics_dao import GenomicSetDao, GenomicSetMemberDao
from rdr_service.model.genomics import GenomicSet, GenomicSetMember
from rdr_service.genomic_enums import GenomicSetStatus, GenomicSetMemberStatus
from rdr_service.offline.sql_exporter import SqlExporter

_US_CENTRAL = pytz.timezone("US/Central")
_BATCH_SIZE = 1000
# The timestamp found at the end of input CSV files.
INPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
_INPUT_CSV_TIME_FORMAT_LENGTH = 18
_CSV_SUFFIX_LENGTH = 4
_RESULT_FILE_SUFFIX = "Validation-Result"
_MAX_INPUT_AGE = datetime.timedelta(hours=24)


class DataError(RuntimeError):
    """Bad genomic data during import.

  Args:
    msg: Passed through to superclass.
    external: If True, this error should be reported to external partners (Analyst).
  """

    def __init__(self, msg, external=False):
        super(DataError, self).__init__(msg)
        self.external = external


class FileNotFoundError(RuntimeError):
    """genomic set file not found during import.

  Args:
    msg: Passed through to superclass.
  """

    # pylint: disable=redefined-builtin
    # TODO: Redefines the built-in FileNotFoundError
    def __init__(self, msg):
        super(FileNotFoundError, self).__init__(msg)


def read_genomic_set_from_bucket():
    try:
        csv_file_cloud_path, csv_filename, timestamp = get_last_genomic_set_file_info()
    except FileNotFoundError:
        logging.info("File not found")
        return None
    now = clock.CLOCK.now()
    if now - timestamp > _MAX_INPUT_AGE:
        logging.info(
            "Input %r (timestamp %s UTC) is > 24h old (relative to %s UTC), not importing."
            % (csv_filename, timestamp, now)
        )
        return None
    if _is_filename_exist(csv_filename):
        raise DataError("This file %s has already been processed" % csv_filename, external=True)
    with open_cloud_file(csv_file_cloud_path) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=",")
        genomic_set_id = _save_genomic_set_from_csv(csv_reader, csv_filename, timestamp)
        return genomic_set_id


def get_last_genomic_set_file_info():
    """Finds the latest CSV & updates/inserts relevant genomic tables from its rows."""
    bucket_name = config.getSetting(config.GENOMIC_SET_BUCKET_NAME)  # raises if missing
    csv_file_cloud_path, csv_filename = _open_latest_genomic_set_file(bucket_name)

    timestamp = timestamp_from_filename(csv_filename)

    return csv_file_cloud_path, csv_filename, timestamp


def timestamp_from_filename(csv_filename):
    if len(csv_filename) < _INPUT_CSV_TIME_FORMAT_LENGTH + _CSV_SUFFIX_LENGTH:
        raise DataError("Can't parse time from CSV filename: %s" % csv_filename)
    time_suffix = csv_filename[
        len(csv_filename)
        - (_INPUT_CSV_TIME_FORMAT_LENGTH + _CSV_SUFFIX_LENGTH)
        - 1 : len(csv_filename)
        - _CSV_SUFFIX_LENGTH
    ]
    try:
        timestamp = datetime.datetime.strptime(time_suffix, INPUT_CSV_TIME_FORMAT)
    except ValueError:
        raise DataError("Can't parse time from CSV filename: %s" % csv_filename)
    # Assume file times are in Central time (CST or CDT); convert to UTC.
    return _US_CENTRAL.localize(timestamp).astimezone(pytz.utc).replace(tzinfo=None)


def _open_latest_genomic_set_file(cloud_bucket_name):
    """Returns an open stream for the most recently created CSV in the given bucket."""
    blob_name = _find_latest_genomic_set_csv(cloud_bucket_name)
    file_name = os.path.basename(blob_name)
    cloud_path = os.path.normpath(cloud_bucket_name + '/' + blob_name)
    logging.info("Opening latest samples CSV in %r: %r.", cloud_bucket_name, cloud_path)
    return cloud_path, file_name


def _find_latest_genomic_set_csv(cloud_bucket_name):
    """Returns the full path (including bucket name) of the most recently created CSV in the bucket.

  Raises:
    RuntimeError: if no CSVs are found in the cloud storage bucket.
  """
    bucket_stat_list = list_blobs("/" + cloud_bucket_name)
    if not bucket_stat_list:
        raise FileNotFoundError("No files in cloud bucket %r." % cloud_bucket_name)

    # GCS does not really have the concept of directories (it's just a filename convention), so all
    # directory listings are recursive and we must filter out subdirectory contents.
    bucket_stat_list = [
        s
        for s in bucket_stat_list
        if s.name.lower().endswith(".csv") and "%s" % _RESULT_FILE_SUFFIX not in s.name
    ]
    if not bucket_stat_list:
        raise FileNotFoundError("No CSVs in cloud bucket %r (all files: %s)." % (cloud_bucket_name, bucket_stat_list))
    bucket_stat_list.sort(key=lambda s: s.updated)
    return bucket_stat_list[-1].name


class CsvColumns(object):
    """Names of CSV columns that we read from the genomic set upload."""

    GENOMIC_SET_NAME = "genomic_set_name"
    GENOMIC_SET_CRITERIA = "genomic_set_criteria"
    PID = "pid"
    BIOBANK_ORDER_ID = "biobank_order_id"
    NY_FLAG = "ny_flag"
    SEX_AT_BIRTH = "sex_at_birth"
    GENOME_TYPE = "genome_type"

    # Note: Please ensure changes to the CSV format are reflected in test data.
    ALL = (GENOMIC_SET_NAME, GENOMIC_SET_CRITERIA, PID, BIOBANK_ORDER_ID, NY_FLAG, SEX_AT_BIRTH, GENOME_TYPE)


def _is_filename_exist(csv_filename):
    set_dao = GenomicSetDao()
    if set_dao.get_one_by_file_name(csv_filename):
        return True
    else:
        return False


def _save_genomic_set_from_csv(csv_reader, csv_filename, timestamp):
    """Inserts GenomicSet and GenomicSetMember from a csv.DictReader."""
    missing_cols = set(CsvColumns.ALL) - set(csv_reader.fieldnames)
    if missing_cols:
        raise DataError("CSV is missing columns %s, had columns %s." % (missing_cols, csv_reader.fieldnames))
    member_dao = GenomicSetMemberDao()
    genomic_set_id = None
    try:
        members = []
        rows = list(csv_reader)
        for i, row in enumerate(rows):
            if i == 0:
                if row[CsvColumns.GENOMIC_SET_NAME] and row[CsvColumns.GENOMIC_SET_CRITERIA]:
                    genomic_set = _insert_genomic_set_from_row(row, csv_filename, timestamp)
                    genomic_set_id = genomic_set.id
                else:
                    raise DataError("CSV is missing columns genomic_set_name or genomic_set_criteria")
            member = _create_genomic_set_member_from_row(genomic_set_id, row)
            members.append(member)
            if len(members) >= _BATCH_SIZE:
                member_dao.upsert_all(members)
                members = []

        if members:
            member_dao.upsert_all(members)

        member_dao.update_biobank_id(genomic_set_id)

        return genomic_set_id
    except ValueError as e:
        raise DataError(e)


def _insert_genomic_set_from_row(row, csv_filename, timestamp):
    """Creates a new GenomicSet object from a CSV row.

  Raises:
    DataError if the row is invalid.
  Returns:
    A new GenomicSet.
  """
    genomic_set_name = (row[CsvColumns.GENOMIC_SET_NAME],)

    set_dao = GenomicSetDao()
    genomic_set_version = set_dao.get_new_version_number(genomic_set_name)
    kwargs = dict(
        genomicSetName=genomic_set_name,
        genomicSetCriteria=row[CsvColumns.GENOMIC_SET_CRITERIA],
        genomicSetFile=csv_filename,
        genomicSetFileTime=timestamp,
        genomicSetStatus=GenomicSetStatus.UNSET,
        genomicSetVersion=genomic_set_version,
    )

    genomic_set = GenomicSet(**kwargs)
    set_dao.insert(genomic_set)

    return genomic_set


def _create_genomic_set_member_from_row(genomic_set_id, row):
    """Creates a new GenomicSetMember object from a CSV row.

  Raises:
    DataError if the row is invalid.
  Returns:
    A new GenomicSetMember.
  """
    kwargs = dict(
        genomicSetId=genomic_set_id,
        validationStatus=GenomicSetMemberStatus.UNSET,
        participantId=row[CsvColumns.PID],
        sexAtBirth=row[CsvColumns.SEX_AT_BIRTH],
        genomeType=row[CsvColumns.GENOME_TYPE],
        nyFlag=1 if row[CsvColumns.NY_FLAG] == "Y" else 0,
        biobankOrderId=row[CsvColumns.BIOBANK_ORDER_ID],
    )

    genomic_set_member = GenomicSetMember(**kwargs)

    return genomic_set_member


def create_genomic_set_status_result_file(genomic_set_id):
    set_dao = GenomicSetDao()
    genomic_set = set_dao.get(genomic_set_id)
    _create_and_upload_result_file(genomic_set)


def _transform_result_row_for_export(row):
    Row = collections.namedtuple("Row", list(row.keys()))
    original = Row(*row)
    status = GenomicSetMemberStatus(original.status)
    flags = GenomicSetMember.validationFlags.type.process_result_value(original.invalid_reason, None)
    kwargs = dict(
        dict(list(row.items())), status=str(status).lower(), invalid_reason=", ".join(map(str, flags)) if flags else ""
    )
    return Row(**kwargs)


def _create_and_upload_result_file(genomic_set):
    result_filename = genomic_set.genomicSetFile.replace(".", "-" + _RESULT_FILE_SUFFIX + ".")
    bucket_name = config.getSetting(config.GENOMIC_SET_BUCKET_NAME)
    exporter = SqlExporter(bucket_name)
    export_sql = """
    SELECT
      :genomic_set_name AS genomic_set_name,
      :genomic_set_criteria AS genomic_set_criteria,
      participant_id AS pid,
      CASE
        WHEN ny_flag IS TRUE THEN 'Y' ELSE 'N'
      END AS ny_flag,
      sex_at_birth,
      genome_type,
      validation_status as status,
      validation_flags as invalid_reason
    FROM genomic_set_member
    WHERE genomic_set_id=:genomic_set_id
    ORDER BY id
  """
    query_params = {
        "genomic_set_name": genomic_set.genomicSetName,
        "genomic_set_criteria": genomic_set.genomicSetCriteria,
        "genomic_set_id": genomic_set.id,
    }
    exporter.run_export(result_filename, export_sql, query_params, transformf=_transform_result_row_for_export)
