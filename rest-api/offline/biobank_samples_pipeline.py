"""Reads a CSV that Biobank uploads to GCS and upserts to the BiobankStoredSample table.

Also updates ParticipantSummary data related to samples.
"""

import csv
import datetime
import logging
import pytz

from cloudstorage import cloudstorage_api
from werkzeug.exceptions import BadRequest

import config
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample
from model.utils import from_client_biobank_id


def upsert_from_latest_csv():
  """Finds the latest CSV & updates/inserts BiobankStoredSamples from its rows."""
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
  csv_file = _open_latest_samples_file(bucket_name)
  csv_reader = csv.DictReader(csv_file, delimiter='\t')
  written, skipped = _upsert_samples_from_csv(csv_reader)
  ParticipantSummaryDao().update_from_biobank_stored_samples()
  return written, skipped


_CREATE_VIEW_MYSQL = """CREATE OR REPLACE ALGORITHM=TEMPTABLE VIEW reconciliation_data AS
  SELECT
    CASE
      WHEN orders.participant_id IS NOT NULL THEN orders.participant_id
      ELSE samples.participant_id
      END participant_id,

    orders.test order_test,
    COUNT(biobank_order_id) num_orders,
    GROUP_CONCAT(biobank_order_id),
    MAX(collected),
    MAX(finalized),
    GROUP_CONCAT(source_site_value),

    samples.test sample_test,
    COUNT(biobank_stored_sample_id) num_samples,
    GROUP_CONCAT(biobank_stored_sample_id),
    MAX(confirmed),

    MAX(TIMESTAMPDIFF(HOUR, confirmed, collected)) elapsed_hours
  FROM
   (SELECT
      participant_id,
      test,
      biobank_order_id,
      collected,
      finalized,
      source_site_value
    FROM
      biobank_order
    JOIN
      biobank_ordered_sample
    ON
      biobank_ordered_sample.order_id = biobank_order.biobank_order_id
    ) orders
  JOIN
   (SELECT
      participant_id,
      biobank_stored_sample_id,
      test,
      confirmed
    FROM
      biobank_stored_sample
    LEFT JOIN
      participant
    ON
      biobank_stored_sample.biobank_id = participant.biobank_id
    ) samples
  ON
    samples.participant_id = orders.participant_id
    AND samples.test = orders.test
  GROUP BY
    orders.participant_id, orders.test, samples.participant_id, samples.test
"""
_SELECT_FROM_VIEW_SQL = """
  SELECT * FROM reconciliation_data
"""

def write_reconciliation_report():
  """Writes order/sample reconciliation reports to GCS."""
  # Note that due to syntax differences, the query runs on MySQL only (not SQLite / unit tests).
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
  # Pick a filename prefix for output.
  # Open gcloud files and attach DictWriters for outputs.
  # Write out reports:
  #   Define a temporary table view `report_data` which contains report columns from the RDR:
  #      *BiobankStoredSample.biobankId (JOIN w/ BiobankOrder.participantId)
  #      *BiobankOrderedSample.test (JOIN w/ BiobankStoredSample.test)
  #       BiobankOrder.biobankorderId (may be a list of the same participant/test happened >1x)
  #       BiobankStoredSample.biobankStoredSampleId (may be a list)
  #       BiobankOrder.sourceSiteValue (ANY_VALUE)
  #       BiobankOrderedSample.finalized (ALL(IS NOT NULL))
  #    and derived elapsed time:
  #       elapsed_hours = MAX(BiobankStoredSample.confirmedDate) - MAX(BiobankOrderedSample.collected)
  #  Then generate reports from the above:
  #      SELECT * FROM report_data WHERE
  #          length of ID lists match AND
  #          BiobankStoredSample.test IS NOT NULL -> samples_received.csv;
  #      SELECT * FROM report_data WHERE elapsed_hours > 24 -> samples_gt_24h.csv;
  #      SELECT * FROM report_data WHERE
  #          length of ID lists doesn't match OR
  #          (BiobankOrderedSample.finalized IS NOT NULL
  #           AND BiobankStoredSample.test IS NULL) -> samples_not_received.csv;
  from dao import database_factory
  db = database_factory.get_database()
  session = db.make_session()
  session.execute(_CREATE_VIEW_MYSQL)
  for line in session.execute(_SELECT_FROM_VIEW_SQL):
    print line
  session.close()


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
  return samples_dao.upsert_batched(
      (s for s in (_create_sample_from_row(row) for row in csv_reader) if s is not None))


# Biobank provides timestamps without time zone info, which should be in central time (see DA-235).
_INPUT_TIMESTAMP_FORMAT = '%Y/%m/%d %H:%M:%S'  # like 2016/11/30 14:32:18
_US_CENTRAL = pytz.timezone('US/Central')


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
      confirmed_naive = datetime.datetime.strptime(confirmed_str, _INPUT_TIMESTAMP_FORMAT)
    except ValueError, e:
      logging.error(
          'Skipping sample %r for %r with bad confirmed timestamp %r: %s',
          sample.biobankStoredSampleId, sample.biobankId, confirmed_str, e.message)
      return None
    # Assume incoming times are in Central time (CST or CDT). Convert to UTC for storage, but drop
    # tzinfo since storage is naive anyway (to make stored/fetched values consistent).
    sample.confirmed = _US_CENTRAL.localize(
        confirmed_naive).astimezone(pytz.utc).replace(tzinfo=None)
  return sample
