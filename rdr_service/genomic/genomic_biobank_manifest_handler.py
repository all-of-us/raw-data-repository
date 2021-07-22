"""
Create a genomic biobank manifest CSV file and uploads to biobank samples bucket subfolders.
"""

import collections
import csv
import datetime
import logging
import os
import pytz

from ..genomic_enums import GenomicWorkflowState
from .genomic_set_file_handler import DataError, timestamp_from_filename
from rdr_service import clock, config
from rdr_service.api_util import list_blobs, open_cloud_file
from rdr_service.config import GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME, GENOMIC_BIOBANK_MANIFEST_RESULT_FOLDER_NAME
from rdr_service.dao.genomics_dao import GenomicSetMemberDao
from rdr_service.offline.sql_exporter import SqlExporter

_US_CENTRAL = pytz.timezone("US/Central")
_UTC = pytz.utc
OUTPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
_MANIFEST_FILE_NAME_PREFIX = "Genomic-Manifest-AoU"
_MAX_INPUT_AGE = datetime.timedelta(hours=24)
# sample suffix: -v12019-04-05-00-30-10.csv
_RESULT_CSV_FILE_SUFFIX_LENGTH = 26
BIOBANK_ID_PREFIX = "A" if config.GAE_PROJECT == "all-of-us-rdr-prod" else "T"


def process_genomic_manifest_result_file_from_bucket():
    bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)
    result_folder_name = config.getSetting(GENOMIC_BIOBANK_MANIFEST_RESULT_FOLDER_NAME)

    bucket_stat_list = list_blobs(bucket_name)
    if not bucket_stat_list:
        logging.info("No files in cloud bucket %r." % bucket_name)
        return None
    bucket_stat_list = [
        s for s in bucket_stat_list if s.name.lower().endswith(".csv") and "%s" % result_folder_name in s.name
    ]
    if not bucket_stat_list:
        logging.info(
            "No CSVs in cloud bucket %r folder %r (all files: %s)."
            % (bucket_name, result_folder_name, bucket_stat_list)
        )
        return None

    bucket_stat_list.sort(key=lambda s: s.updated)
    path = os.path.normpath(bucket_name + '/' + bucket_stat_list[-1].name)
    filename = os.path.basename(path)
    logging.info("Opening latest genomic manifest result CSV in %r: %r.", bucket_name + "/" + result_folder_name, path)
    timestamp = timestamp_from_filename(filename)
    now = clock.CLOCK.now()
    if now - timestamp > _MAX_INPUT_AGE:
        logging.info(
            "Input %r (timestamp %s UTC) is > 24h old (relative to %s UTC), not processing."
            % (filename, timestamp, now)
        )
        print(
            (
                "Input %r (timestamp %s UTC) is > 24h old (relative to %s UTC), not processing."
                % (filename, timestamp, now)
            )
        )
        return None

    genomic_set_id = _get_genomic_set_id_from_filename(filename)
    with open_cloud_file(path) as csv_file:
        update_package_id_from_manifest_result_file(genomic_set_id, csv_file)


def _get_genomic_set_id_from_filename(csv_filename):
    prefix_part = csv_filename[0 : (len(csv_filename) - _RESULT_CSV_FILE_SUFFIX_LENGTH)]
    genomic_set_id = prefix_part[(prefix_part.rfind("-") + 1) : len(prefix_part)]
    return genomic_set_id


class CsvColumns(object):
    VALUE = "value"
    BIOBANK_ID = "biobank_id"
    SEX_AT_BIRTH = "sex_at_birth"
    GENOME_TYPE = "genome_type"
    NY_FLAG = "ny_flag"
    REQUEST_ID = "request_id"
    PACKAGE_ID = "package_id"

    ALL = (VALUE, SEX_AT_BIRTH, GENOME_TYPE, NY_FLAG, REQUEST_ID, PACKAGE_ID)


def update_package_id_from_manifest_result_file(genomic_set_id, csv_file):
    csv_reader = csv.DictReader(csv_file, delimiter=",")
    missing_cols = set(CsvColumns.ALL) - set(csv_reader.fieldnames)
    if len(csv_reader.fieldnames) == 1:
        csv_file.seek(0, 0)
        csv_reader = csv.DictReader(csv_file, delimiter="\t")
        missing_cols = set(CsvColumns.ALL) - set(csv_reader.fieldnames)
    if missing_cols:
        raise DataError("CSV is missing columns %s, had columns %s." % (missing_cols, csv_reader.fieldnames))

    ClientIdPackageIdPair = collections.namedtuple(
        "ClientIdPackageIdPair", ["biobank_id", "genome_type", "client_id", "package_id"]
    )
    update_queue = collections.deque()

    dao = GenomicSetMemberDao()

    try:
        rows = list(csv_reader)
        for row in rows:
            if row[CsvColumns.VALUE] and row[CsvColumns.PACKAGE_ID] and row[CsvColumns.BIOBANK_ID]:
                biobank_id = (
                    row[CsvColumns.BIOBANK_ID][len(BIOBANK_ID_PREFIX) :]
                    if row[CsvColumns.BIOBANK_ID].startswith(BIOBANK_ID_PREFIX)
                    else row[CsvColumns.BIOBANK_ID]
                )
                update_queue.append(
                    ClientIdPackageIdPair(
                        biobank_id, row[CsvColumns.GENOME_TYPE], row[CsvColumns.VALUE], row[CsvColumns.PACKAGE_ID]
                    )
                )

        dao.bulk_update_package_id(genomic_set_id, update_queue)

    except ValueError as e:
        raise DataError(e)


def create_and_upload_genomic_biobank_manifest_file(
        genomic_set_id,
        timestamp=None,
        bucket_name=None,
        cohort_id=None,
        saliva=False,
        filename=None,
        prefix=None,
        project=None
    ):

    _where = 'WHERE genomic_workflow_state=:workflow_state'
    clauses = {
        'default': {
            'text': _where + ' AND genomic_set_id=:genomic_set_id',
            'state': int(GenomicWorkflowState.AW0_READY)
        },
        'long_read': {
            'text':  _where,
            'state': int(GenomicWorkflowState.LR_PENDING)
        }
    }
    clause = clauses[project] if project else clauses['default']

    result_filename = filename if filename is not None \
        else _get_output_manifest_file_name(genomic_set_id, timestamp, cohort_id, saliva)

    if bucket_name is None:
        bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)
    exporter = SqlExporter(bucket_name)
    export_sql = """
      SELECT
        '' as value,
        collection_tube_id,
        CONCAT(:prefix, biobank_id) as biobank_id,
        sex_at_birth,
        genome_type,
        CASE
          WHEN ny_flag IS TRUE THEN 'Y' ELSE 'N'
        END AS ny_flag,
        CASE
          WHEN validation_status = 1 THEN 'Y' ELSE 'N'
        END AS validation_passed,
        ai_an
      FROM genomic_set_member
      {clause}
      ORDER BY id
    """.format(
        clause=clause['text']
    )

    query_params = {
        "genomic_set_id": genomic_set_id,
        "prefix": prefix or BIOBANK_ID_PREFIX,
        "workflow_state": clause['state']
    }

    exporter.run_export(result_filename, export_sql, query_params)


def _get_output_manifest_file_name(genomic_set_id, timestamp=None, cohort_id=None, saliva=False):
    file_timestamp = timestamp if timestamp else clock.CLOCK.now()
    now_cdt_str = (
        _UTC.localize(file_timestamp).astimezone(_US_CENTRAL).replace(tzinfo=None).strftime(OUTPUT_CSV_TIME_FORMAT)
    )
    cohort = f"_{cohort_id}" if cohort_id else ""
    saliva = "_saliva" if saliva else ""
    folder_name = config.getSetting(GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME)
    full_name = f'{folder_name}/{_MANIFEST_FILE_NAME_PREFIX}-{now_cdt_str}{cohort}{saliva}-{str(genomic_set_id)}.csv'
    return full_name
