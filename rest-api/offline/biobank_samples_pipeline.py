"""Reads a CSV that Biobank uploads to GCS and upserts to the BiobankStoredSample table.

Also updates ParticipantSummary data related to samples.
"""

import csv
import datetime
import logging

from cloudstorage import cloudstorage_api
from werkzeug.exceptions import BadRequest

import config
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample
from model.utils import from_client_biobank_id


def upsert_from_latest_csv():
  """Main entry point: Finds the latest CSV & updates/inserts BiobankStoredSamples from its rows."""
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
  csv_file = _open_latest_samples_file(bucket_name)
  csv_reader = csv.DictReader(csv_file, delimiter='\t')
  _upsert_samples_from_csv(csv_reader)
  ParticipantSummaryDao().update_from_biobank_stored_samples()


def _open_latest_samples_file(cloud_bucket_name):
  """Returns an open stream for the most recently created CSV in the given bucket."""
  path = _find_latest_samples_csv(cloud_bucket_name)
  logging.info('Opening latest samples CSV in %r: %r.', cloud_bucket_name, path)
  return cloudstorage_api.open(path)


def _find_latest_samples_csv(cloud_bucket_name):
  """Returns the full path (including bucket name) of the most recently created CSV in the bucket.

  Raises:
    RuntimeError: if no CSVs are found in the cloud storage bucket.
  """
  bucket_stat_list = cloudstorage_api.listbucket('/' + cloud_bucket_name)
  if not bucket_stat_list:
    raise RuntimeError('No files in cloud bucket %r.' % cloud_bucket_name)
  bucket_stat_list = [s for s in bucket_stat_list if s.filename.lower().endswith('.csv')]
  if not bucket_stat_list:
    raise RuntimeError(
        'No CSVs in cloud bucket %r (all files: %s).' % (cloud_bucket_name, bucket_stat_list))
  bucket_stat_list.sort(key=lambda s: s.st_ctime)
  return bucket_stat_list[-1].filename


class _Columns(object):
  """Names of CSV columns that we read from the Biobank samples upload."""
  SAMPLE_ID = 'Sample Id'
  PARENT_ID = 'Parent Sample Id'
  CONFIRMED_DATE = 'Sample Confirmed Date'
  EXTERNAL_PARTICIPANT_ID = 'External Participant Id'
  TEST_CODE = 'Test Code'
  ALL = frozenset([SAMPLE_ID, PARENT_ID, CONFIRMED_DATE, EXTERNAL_PARTICIPANT_ID, TEST_CODE])


def _upsert_samples_from_csv(csv_reader):
  """Inserts/updates BiobankStoredSamples from a csv.DictReader."""
  missing_cols = _Columns.ALL - set(csv_reader.fieldnames)
  if missing_cols:
    raise RuntimeError(
        'CSV is missing columns %s, had columns %s.' % (missing_cols, csv_reader.fieldnames))
  samples_dao = BiobankStoredSampleDao()
  samples_dao.upsert_batched(
      (s for s in (_create_sample_from_row(row) for row in csv_reader) if s is not None))


# TODO(mwf) Have Biobank switch to a timestamp format with time zone information (pref. isoformat).
_TIMESTAMP_FORMAT = '%Y/%m/%d %H:%M:%S'  # like 2016/11/30 14:32:18


def _create_sample_from_row(row):
  """Returns a new BiobankStoredSample object from a CSV row, or None if the row is invalid."""
  biobank_id_str = row[_Columns.EXTERNAL_PARTICIPANT_ID]
  try:
    biobank_id = from_client_biobank_id(biobank_id_str)
  except BadRequest, e:
    logging.error('Bad external participant ID (Biobank ID) %r: %s', biobank_id_str, e.message)
    return None
  sample = BiobankStoredSample(
      biobankStoredSampleId=row[_Columns.SAMPLE_ID],
      biobankId=biobank_id,
      test=row[_Columns.TEST_CODE])
  if row[_Columns.PARENT_ID]:
    # Skip child samples.
    return None
  confirmed_str = row[_Columns.CONFIRMED_DATE]
  if confirmed_str:
    try:
      sample.confirmed = datetime.datetime.strptime(confirmed_str, _TIMESTAMP_FORMAT)
    except ValueError, e:
      logging.error(
          'Skipping sample %r for %r with bad confirmed timestamp %r: %s',
          sample.biobankStoredSampleId, sample.biobankId, confirmed_str, e.message)
      return None
  return sample
