"""
Reads a CSV that analyst uploads to genomic_set_upload bucket.
And insert to relevant genomic tables.
"""

import csv
import datetime
import logging
import pytz
from cloudstorage import cloudstorage_api
import clock
import config
from offline.sql_exporter import SqlExporter
from dao.genomics_dao import GenomicSetDao, GenomicSetMemberDao
from model.genomics import GenomicSet, GenomicSetMember, GenomicSetStatus, GenomicValidationStatus

_US_CENTRAL = pytz.timezone('US/Central')
_BATCH_SIZE = 1000
# The timestamp found at the end of input CSV files.
INPUT_CSV_TIME_FORMAT = '%Y-%m-%d-%H-%M-%S'
_INPUT_CSV_TIME_FORMAT_LENGTH = 18
_CSV_SUFFIX_LENGTH = 4

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

def read_genomic_set_from_bucket():
  csv_file, csv_filename, timestamp = get_last_genomic_set_file_info()
  if _is_filename_exist(csv_filename):
    raise DataError(
      'This file %s has already been processed' % csv_filename, external=True)
  now = clock.CLOCK.now()
  if now - timestamp > _MAX_INPUT_AGE:
    logging.info('Input %r (timestamp %s UTC) is > %s h old (relative to %s UTC), not importing.'
                 % (_MAX_INPUT_AGE, csv_filename, timestamp, now))
    return

  csv_reader = csv.DictReader(csv_file, delimiter=',')
  written = _save_genomic_set_from_csv(csv_reader, csv_filename, timestamp)

  return written, timestamp

def get_last_genomic_set_file_info():
  """Finds the latest CSV & updates/inserts relevant genomic tables from its rows."""
  bucket_name = config.getSetting(config.GENOMIC_SET_BUCKET_NAME)  # raises if missing
  csv_file, csv_filename = _open_latest_genomic_set_file(bucket_name)
  timestamp = _timestamp_from_filename(csv_filename)

  return csv_file, csv_filename, timestamp

def _timestamp_from_filename(csv_filename):
  if len(csv_filename) < _INPUT_CSV_TIME_FORMAT_LENGTH + _CSV_SUFFIX_LENGTH:
    raise DataError("Can't parse time from CSV filename: %s" % csv_filename)
  time_suffix = csv_filename[len(csv_filename) - (_INPUT_CSV_TIME_FORMAT_LENGTH +
                                                  _CSV_SUFFIX_LENGTH) - 1:
                    len(csv_filename) - _CSV_SUFFIX_LENGTH]
  try:
    timestamp = datetime.datetime.strptime(time_suffix, INPUT_CSV_TIME_FORMAT)
  except ValueError:
    raise DataError("Can't parse time from CSV filename: %s" % csv_filename)
  # Assume file times are in Central time (CST or CDT); convert to UTC.
  return _US_CENTRAL.localize(timestamp).astimezone(pytz.utc).replace(tzinfo=None)


def _open_latest_genomic_set_file(cloud_bucket_name):
  """Returns an open stream for the most recently created CSV in the given bucket."""
  path = _find_latest_genomic_set_csv(cloud_bucket_name)
  filename = path.replace('/' + cloud_bucket_name + '/', '')
  logging.info('Opening latest samples CSV in %r: %r.', cloud_bucket_name, path)
  return cloudstorage_api.open(path), filename


def _find_latest_genomic_set_csv(cloud_bucket_name):
  """Returns the full path (including bucket name) of the most recently created CSV in the bucket.

  Raises:
    RuntimeError: if no CSVs are found in the cloud storage bucket.
  """
  bucket_stat_list = cloudstorage_api.listbucket('/' + cloud_bucket_name)
  if not bucket_stat_list:
    raise DataError('No files in cloud bucket %r.' % cloud_bucket_name)
  # GCS does not really have the concept of directories (it's just a filename convention), so all
  # directory listings are recursive and we must filter out subdirectory contents.
  bucket_stat_list = [s for s in bucket_stat_list if s.filename.lower().endswith('.csv')]
  if not bucket_stat_list:
    raise DataError(
        'No CSVs in cloud bucket %r (all files: %s).' % (cloud_bucket_name, bucket_stat_list))
  bucket_stat_list.sort(key=lambda s: s.st_ctime)
  return bucket_stat_list[-1].filename


class CsvColumns(object):
  """Names of CSV columns that we read from the genomic set upload."""
  GENOMIC_SET_NAME = 'genomic_set_name'
  GENOMIC_SET_CRITERIA = 'genomic_set_criteria'
  PID = 'pid'
  BIOBANK_ORDER_ID = 'biobank_order_id'
  NY_FLAG = 'ny_flag'
  SEX_AT_BIRTH = 'sex_at_birth'
  GENOME_TYPE = 'genome_type'

  # Note: Please ensure changes to the CSV format are reflected in test data.
  ALL = (GENOMIC_SET_NAME, GENOMIC_SET_CRITERIA, PID, BIOBANK_ORDER_ID, NY_FLAG, SEX_AT_BIRTH,
         GENOME_TYPE)

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
    raise DataError(
        'CSV is missing columns %s, had columns %s.' % (missing_cols, csv_reader.fieldnames))
  member_dao = GenomicSetMemberDao()
  written = 0
  try:
    members = []
    rows = list(csv_reader)
    for i, row in enumerate(rows):
      if i == 0:
        if row[CsvColumns.GENOMIC_SET_NAME] and row[CsvColumns.GENOMIC_SET_CRITERIA]:
          genomic_set = _insert_genomic_set_from_row(row, csv_filename, timestamp)
        else:
          raise DataError('CSV is missing columns genomic_set_name or genomic_set_criteria')
      member = _create_genomic_set_member_from_row(genomic_set.id, row)
      members.append(member)
      if len(members) >= _BATCH_SIZE:
        written += member_dao.upsert_all(members)
        members = []

    if members:
      written += member_dao.upsert_all(members)

    return written
  except ValueError, e:
    raise DataError(e)

def _insert_genomic_set_from_row(row, csv_filename, timestamp):
  """Creates a new GenomicSet object from a CSV row.

  Raises:
    DataError if the row is invalid.
  Returns:
    A new GenomicSet.
  """
  now = clock.CLOCK.now()
  genomic_set_name = row[CsvColumns.GENOMIC_SET_NAME],

  set_dao = GenomicSetDao()
  genomic_set_version = set_dao.get_new_version_number(genomic_set_name)
  kwargs = dict(
    genomicSetName=genomic_set_name,
    genomicSetCriteria=row[CsvColumns.GENOMIC_SET_CRITERIA],
    genomicSetFile=csv_filename,
    genomicSetFileTime=timestamp,
    genomicSetStatus=GenomicSetStatus.UNSET,
    genomicSetVersion=genomic_set_version,
    created=now,
    modified=now
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
  now = clock.CLOCK.now()

  kwargs = dict(
    genomicSetId=genomic_set_id,
    created=now,
    modified=now,
    validationStatus=GenomicValidationStatus.UNSET,
    participantId=row[CsvColumns.PID],
    sexAtBirth=row[CsvColumns.SEX_AT_BIRTH],
    genomeType=row[CsvColumns.GENOME_TYPE],
    nyFlag=1 if row[CsvColumns.NY_FLAG] == 'Y' else 0,
    biobankOrderId=row[CsvColumns.BIOBANK_ORDER_ID]
  )

  genomic_set_member = GenomicSetMember(**kwargs)

  return genomic_set_member

def create_genomic_set_status_result_file(genomic_set_id):
  set_dao = GenomicSetDao()
  genomic_set = set_dao.get(genomic_set_id)
  _create_and_upload_result_file(genomic_set)

def _create_and_upload_result_file(genomic_set):
  result_filename = genomic_set.genomicSetFile.replace('.', '-Validation-Result.')
  bucket_name = config.getSetting(config.GENOMIC_SET_BUCKET_NAME)
  exporter = SqlExporter(bucket_name)
  export_sql = """
    SELECT 
      :genomic_set_name AS genomic_set_name,
      :genomic_set_criteria AS genomic_set_criteria,
      participant_id AS pid,
      biobank_order_id,
      ny_flag,
      sex_at_birth,
      genome_type,
      CASE
        WHEN validation_status=1 THEN 'valid' ELSE 'invalid'
      END AS status,
      CASE
        WHEN validation_status=0 OR validation_status IS NULL THEN :status_unset
        WHEN validation_status=1 THEN :status_valid
        WHEN validation_status=2 THEN :status_invalid_biobank_order
        WHEN validation_status=3 THEN :status_invalid_ny_zipcode
        WHEN validation_status=4 THEN :status_invalid_sex_at_birth
        WHEN validation_status=5 THEN :status_invalid_genome_type
        WHEN validation_status=6 THEN :status_invalid_consent
        WHEN validation_status=7 THEN :status_invalid_withdraw_status
        WHEN validation_status=8 THEN :status_invalid_age
        WHEN validation_status=9 THEN :status_invalid_dup_participant
        ELSE :status_unknown
      END AS invalid_reason
    FROM genomic_set_member
    WHERE genomic_set_id=:genomic_set_id
    ORDER BY id
  """
  query_params = {'genomic_set_name': genomic_set.genomicSetName,
                  'genomic_set_criteria': genomic_set.genomicSetCriteria,
                  'genomic_set_id': genomic_set.id,
                  'status_unset': str(GenomicValidationStatus.UNSET),
                  'status_valid': '',
                  'status_invalid_biobank_order':
                    str(GenomicValidationStatus.INVALID_BIOBANK_ORDER),
                  'status_invalid_ny_zipcode': str(GenomicValidationStatus.INVALID_NY_ZIPCODE),
                  'status_invalid_sex_at_birth': str(GenomicValidationStatus.INVALID_SEX_AT_BIRTH),
                  'status_invalid_genome_type': str(GenomicValidationStatus.INVALID_GENOME_TYPE),
                  'status_invalid_consent': str(GenomicValidationStatus.INVALID_CONSENT),
                  'status_invalid_withdraw_status':
                    str(GenomicValidationStatus.INVALID_WITHDRAW_STATUS),
                  'status_invalid_age': str(GenomicValidationStatus.INVALID_AGE),
                  'status_invalid_dup_participant':
                    str(GenomicValidationStatus.INVALID_DUP_PARTICIPANT),
                  'status_unknown': 'UNKNOWN'
                  }
  exporter.run_export(result_filename, export_sql, query_params)

