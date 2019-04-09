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
from code_constants import RACE_QUESTION_CODE, RACE_AIAN_CODE, PPI_SYSTEM
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.code_dao import CodeDao
from dao.database_utils import replace_isodate, parse_datetime
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample
from model.config_utils import from_client_biobank_id, get_biobank_id_prefix
from model.participant import Participant
from dao.participant_dao import ParticipantDao
from offline.sql_exporter import SqlExporter
from participant_enums import OrganizationType, BiobankOrderStatus, get_sample_status_enum_value

_INPUT_TIMESTAMP_FORMAT = '%Y/%m/%d %H:%M:%S'  # like 2016/11/30 14:32:18
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
    external: If True, this error should be reported to external partners (Biobank). Externally
        reported DataErrors are only reported if biobank recipients are in the config.
  """
  def __init__(self, msg, external=False):
    super(DataError, self).__init__(msg)
    self.external = external


def read_genomic_set_from_bucket():
  csv_file, csv_filename, timestamp = get_last_biobank_sample_file_info()

  now = clock.CLOCK.now()
  if now - timestamp > _MAX_INPUT_AGE:
    raise DataError(
        'Input %r (timestamp %s UTC) is > 24h old (relative to %s UTC), not importing.'
        % (csv_filename, timestamp, now),
        external=True)

  csv_reader = csv.DictReader(csv_file, delimiter='\t')
  written = _upsert_genomic_set_from_csv(csv_reader)

  return written, timestamp

def get_last_biobank_sample_file_info():
  """Finds the latest CSV & updates/inserts relevant genomic tables from its rows."""
  bucket_name = config.getSetting(config.GENOMIC_SET_BUCKET_NAME)  # raises if missing
  csv_file, csv_filename = _open_latest_samples_file(bucket_name)
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


def _open_latest_samples_file(cloud_bucket_name):
  """Returns an open stream for the most recently created CSV in the given bucket."""
  path = _find_latest_samples_csv(cloud_bucket_name)
  logging.info('Opening latest samples CSV in %r: %r.', cloud_bucket_name, path)
  return cloudstorage_api.open(path), path


def _find_latest_samples_csv(cloud_bucket_name):
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
  STATUS = 'status'
  INVALID_REASON = 'invalid_reason'

  # Note: Please ensure changes to the CSV format are reflected in fake_genomic set_generator.
  ALL = (GENOMIC_SET_NAME, GENOMIC_SET_CRITERIA, PID, BIOBANK_ORDER_ID, NY_FLAG, SEX_AT_BIRTH,
         GENOME_TYPE, STATUS, INVALID_REASON)

def _upsert_genomic_set_from_csv(csv_reader):
  """Inserts/updates GenomicSet and GenomicSetMember from a csv.DictReader."""
  missing_cols = set(CsvColumns.ALL) - set(csv_reader.fieldnames)
  if missing_cols:
    raise DataError(
        'CSV is missing columns %s, had columns %s.' % (missing_cols, csv_reader.fieldnames))
  member_dao = None  # TODO
  written = 0
  try:
    members = []
    with ParticipantDao().session() as session:
      for row in csv_reader:
        if row[CsvColumns.GENOMIC_SET_NAME] and row[CsvColumns.GENOMIC_SET_CRITERIA]:
          _create_genomic_set_from_row(row)
        member = _create_genomic_set_member_from_row(row)
        if member:
          members.append(member)
          if len(members) >= _BATCH_SIZE:
            written += member_dao.upsert_all(members)
            members = []

      if members:
        written += member_dao.upsert_all(members)

    return written
  except ValueError, e:
    raise DataError(e)

def _parse_timestamp(row, key, sample):
  str_val = row[key]
  if str_val:
    try:
      naive = datetime.datetime.strptime(str_val, _INPUT_TIMESTAMP_FORMAT)
    except ValueError, e:
      raise DataError(
          'Sample %r for %r has bad timestamp %r: %s'
          % (sample.biobankStoredSampleId, sample.biobankId, str_val, e.message))
    # Assume incoming times are in Central time (CST or CDT). Convert to UTC for storage, but drop
    # tzinfo since storage is naive anyway (to make stored/fetched values consistent).
    return _US_CENTRAL.localize(naive).astimezone(pytz.utc).replace(tzinfo=None)
  return None

def _create_genomic_set_from_row(row):
  """Creates a new GenomicSet object from a CSV row.

  Raises:
    DataError if the row is invalid.
  Returns:
    A new GenomicSet.
  """
  #TODO

def _create_genomic_set_member_from_row(row):
  """Creates a new GenomicSetMember object from a CSV row.

  Raises:
    DataError if the row is invalid.
  Returns:
    A new GenomicSetMember.
  """
  #TODO

def update_genomic_set_status_to_bucket_file(genomic_set_id):

  #TODO
