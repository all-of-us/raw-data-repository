"""Reads a CSV that Biobank uploads to GCS and upserts to the BiobankStoredSample table.

Also updates ParticipantSummary data related to samples.
"""

import csv
import contextlib
import datetime
import logging
import pytz

from cloudstorage import cloudstorage_api
from werkzeug.exceptions import BadRequest

from api_util import coerce_to_utc
import clock
import config
from dao import database_factory
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample
from model.utils import from_client_biobank_id, to_client_biobank_id


# Format for dates in output filenames for the reconciliation report.
_FILENAME_DATE_FORMAT = '%Y-%m-%d'
# The output of the reconciliation report goes into this subdirectory within the upload bucket.
_REPORT_SUBDIR = 'reconciliation'


def upsert_from_latest_csv():
  """Finds the latest CSV & updates/inserts BiobankStoredSamples from its rows."""
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
  csv_file = _open_latest_samples_file(bucket_name)
  csv_reader = csv.DictReader(csv_file, delimiter='\t')
  written, skipped = _upsert_samples_from_csv(csv_reader)
  ParticipantSummaryDao().update_from_biobank_stored_samples()
  return written, skipped


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
  # GCS does not really have the concept of directories (it's just a filename convention), so all
  # directory listings are recursive and we must filter out subdirectory contents.
  bucket_stat_list = [
      s for s in bucket_stat_list
      if s.filename.lower().endswith('.csv') and '/%s/' % _REPORT_SUBDIR not in s.filename]
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


def write_reconciliation_report():
  """Writes order/sample reconciliation reports to GCS."""
  now = clock.CLOCK.now()
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
  path_received, path_late, path_missing = _get_report_paths(bucket_name, now)
  with _writer_guard(path_received) as writer_received:
    with _writer_guard(path_late) as writer_late:
      with _writer_guard(path_missing) as writer_missing:
        _query_and_write_reports(writer_received, writer_late, writer_missing)


def _get_report_paths(bucket_name, report_dt):
  return [
      '/%s/%s/report_%s_%s.csv' % (
          bucket_name, _REPORT_SUBDIR, report_dt.strftime(_FILENAME_DATE_FORMAT), report_name)
      for report_name in ('received', 'over_24h', 'missing')]


@contextlib.contextmanager
def _writer_guard(path):
  """Opens CSV writer on a GCS file."""
  with cloudstorage_api.open(path, mode='w') as cloud_file:
    writer = csv.DictWriter(cloud_file, fieldnames=_CSV_COLUMN_NAMES)
    writer.writeheader()
    yield writer


def _query_and_write_reports(writer_received, writer_late, writer_missing):
  """Runs the reconciliation MySQL queries and writes result rows to the given CSV writers.

  Note that due to syntax differences, the query runs on MySQL only (not SQLite in unit tests).
  """
  with database_factory.get_database().session() as session:
    session.execute(_CREATE_ORDERS_BY_BIOBANK_ID_MYSQL)
    session.execute(_CREATE_RECONCILIATION_VIEW_MYSQL)
    for query, writer in (
        (_SELECT_FROM_VIEW_MYSQL_RECEIVED, writer_received),
        (_SELECT_FROM_VIEW_MYSQL_LATE, writer_late),
        (_SELECT_FROM_VIEW_MYSQL_MISSING, writer_missing)):
      for row in session.execute(query):
        writer.writerow(_post_process_row(row))


def _post_process_row(raw_row):
  """Formats values in an SQL result row for CSV dict output."""
  row = dict(zip(_CSV_COLUMN_NAMES, raw_row))
  row[_COLUMN_BIOBANK_ID] = to_client_biobank_id(row[_COLUMN_BIOBANK_ID])
  for k in row.keys():
    if isinstance(row[k], datetime.datetime):
      row[k] = coerce_to_utc(row[k]).isoformat()
  return row


# Names for the reconciliation_data columns in output CSVs.
_COLUMN_BIOBANK_ID = 'biobank_id'
_CSV_COLUMN_NAMES = (
  _COLUMN_BIOBANK_ID,

  'sent_test',
  'sent_count',
  'sent_order_id',
  'sent_collection_time',
  'sent_finalized_time',
  'site_id',

  'received_test',
  'received_count',
  'received_sample_id',
  'received_time',

  'elapsed_hours',
)


# Gets orders with ordered samples (child rows), and keys by biobank_id to match the desired output.
_CREATE_ORDERS_BY_BIOBANK_ID_MYSQL = """
CREATE OR REPLACE ALGORITHM=TEMPTABLE VIEW orders_by_biobank_id AS
  SELECT
    biobank_id biobank_id_from_order,
    biobank_order_id,
    source_site_value,
    biobank_ordered_sample.test order_test,
    biobank_ordered_sample.collected,
    biobank_ordered_sample.finalized
  FROM
   (SELECT
      participant.biobank_id,
      biobank_order.biobank_order_id,
      biobank_order.source_site_value
    FROM
      biobank_order
    LEFT JOIN
      participant
    ON
      biobank_order.participant_id = participant.participant_id
    ) orders_rekeyed_by_biobank_id
  JOIN
    biobank_ordered_sample
  ON
    biobank_order_id = order_id
"""


# Joins orders and samples, and computes some derived values (elapsed_hours, counts).
# MySQL does not support FULL OUTER JOIN, so instead we UNION a RIGHT and LEFT OUTER JOIN.
_CREATE_RECONCILIATION_VIEW_MYSQL = """
CREATE OR REPLACE ALGORITHM=TEMPTABLE VIEW reconciliation_data AS
  SELECT
    CASE
      WHEN biobank_id_from_order IS NOT NULL THEN biobank_id_from_order
      ELSE biobank_id
      END biobank_id,

    order_test,
    COUNT(DISTINCT biobank_order_id) orders_count,
    GROUP_CONCAT(DISTINCT biobank_order_id),
    MAX(collected),
    MAX(finalized) finalized,
    GROUP_CONCAT(DISTINCT source_site_value),

    test sample_test,
    COUNT(DISTINCT biobank_stored_sample_id) samples_count,
    GROUP_CONCAT(DISTINCT biobank_stored_sample_id),
    MAX(confirmed),

    TIMESTAMPDIFF(HOUR, MAX(collected), MAX(confirmed)) elapsed_hours
  FROM
   (SELECT * FROM
      orders_by_biobank_id
    LEFT OUTER JOIN
      biobank_stored_sample
    ON
      biobank_stored_sample.biobank_id = biobank_id_from_order
      AND biobank_stored_sample.test = order_test
    UNION
    SELECT * FROM
      orders_by_biobank_id
    RIGHT OUTER JOIN
      biobank_stored_sample
    ON
      biobank_stored_sample.biobank_id = biobank_id_from_order
      AND biobank_stored_sample.test = order_test
    ) reconciled
  GROUP BY
    reconciled.biobank_id_from_order,
    reconciled.biobank_id,
    reconciled.order_test,
    reconciled.test
"""


# Gets all sample/order pairs where everything arrived, regardless of timing.
_SELECT_FROM_VIEW_MYSQL_RECEIVED = """
SELECT * FROM reconciliation_data
WHERE
  sample_test IS NOT NULL
  AND samples_count = orders_count
"""

# Gets orders for which the samples arrived, but they arrived late.
_SELECT_FROM_VIEW_MYSQL_LATE = 'SELECT * FROM reconciliation_data WHERE elapsed_hours > 24'


# Gets samples or orders where something has gone missing.
_SELECT_FROM_VIEW_MYSQL_MISSING = """
SELECT * FROM reconciliation_data
WHERE
  (samples_count != orders_count)
  OR
  (finalized IS NOT NULL AND sample_test IS NULL)
"""
