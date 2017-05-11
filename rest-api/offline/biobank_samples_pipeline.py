"""Reads a CSV that Biobank uploads to GCS and upserts to the BiobankStoredSample table.

Also updates ParticipantSummary data related to samples.
"""

import csv
import datetime
import logging
import pytz

from cloudstorage import cloudstorage_api

import clock
import config
from dao import database_factory
from dao.database_utils import replace_isodate
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample
from model.utils import from_client_biobank_id, get_biobank_id_prefix
from offline.sql_exporter import SqlExporter, CompositeSqlExportWriter

# Format for dates in output filenames for the reconciliation report.
_FILENAME_DATE_FORMAT = '%Y-%m-%d'
# The output of the reconciliation report goes into this subdirectory within the upload bucket.
_REPORT_SUBDIR = 'reconciliation'
_BATCH_SIZE = 1000

class DataError(RuntimeError):
  """Bad sample data during import."""


def upsert_from_latest_csv():
  """Finds the latest CSV & updates/inserts BiobankStoredSamples from its rows."""
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
  csv_file = _open_latest_samples_file(bucket_name)
  csv_reader = csv.DictReader(csv_file, delimiter='\t')
  written = _upsert_samples_from_csv(csv_reader)
  ParticipantSummaryDao().update_from_biobank_stored_samples()
  return written


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
    raise DataError('No files in cloud bucket %r.' % cloud_bucket_name)
  # GCS does not really have the concept of directories (it's just a filename convention), so all
  # directory listings are recursive and we must filter out subdirectory contents.
  bucket_stat_list = [
      s for s in bucket_stat_list
      if s.filename.lower().endswith('.csv') and '/%s/' % _REPORT_SUBDIR not in s.filename]
  if not bucket_stat_list:
    raise DataError(
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
    raise DataError(
        'CSV is missing columns %s, had columns %s.' % (missing_cols, csv_reader.fieldnames))
  samples_dao = BiobankStoredSampleDao()
  biobank_id_prefix = get_biobank_id_prefix()
  written = 0
  try:
    samples = []
    for row in csv_reader:
      sample = _create_sample_from_row(row, biobank_id_prefix)
      if sample:
        samples.append(sample)
        if len(samples) >= _BATCH_SIZE:
          written += samples_dao.upsert_all(samples)
          samples = []
    if samples:
      written += samples_dao.upsert_all(samples)
    return written
  except ValueError, e:
    raise DataError(e)


# Biobank provides timestamps without time zone info, which should be in central time (see DA-235).
_INPUT_TIMESTAMP_FORMAT = '%Y/%m/%d %H:%M:%S'  # like 2016/11/30 14:32:18
_US_CENTRAL = pytz.timezone('US/Central')


def _create_sample_from_row(row, biobank_id_prefix):
  """Creates a new BiobankStoredSample object from a CSV row.

  Raises:
    DataError if the row is invalid.
  Returns:
    A new BiobankStoredSample, or None if the row should be skipped.
  """
  biobank_id_str = row[_Columns.EXTERNAL_PARTICIPANT_ID]
  if not biobank_id_str.startswith(biobank_id_prefix):
    # This is a biobank sample for another environment. Ignore it.
    return None
  biobank_id = from_client_biobank_id(biobank_id_str)
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
      raise DataError(
          'Sample %r for %r has bad confirmed timestamp %r: %s'
          % (sample.biobankStoredSampleId, sample.biobankId, confirmed_str, e.message))
    # Assume incoming times are in Central time (CST or CDT). Convert to UTC for storage, but drop
    # tzinfo since storage is naive anyway (to make stored/fetched values consistent).
    sample.confirmed = _US_CENTRAL.localize(
        confirmed_naive).astimezone(pytz.utc).replace(tzinfo=None)
  return sample


def write_reconciliation_report():
  """Writes order/sample reconciliation reports to GCS."""
  now = clock.CLOCK.now()
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
  _query_and_write_reports(SqlExporter(bucket_name), *_get_report_paths(now))


def _get_report_paths(report_datetime):
  """Returns a triple of output filenames for samples: (received, late, missing)."""
  return [
      '%s/report_%s_%s.csv' % (
          _REPORT_SUBDIR, report_datetime.strftime(_FILENAME_DATE_FORMAT), report_name)
      for report_name in ('received', 'over_24h', 'missing')]

def _query_and_write_reports(exporter, path_received, path_late, path_missing):
  """Runs the reconciliation MySQL queries and writes result rows to the given CSV writers.

  Note that due to syntax differences, the query runs on MySQL only (not SQLite in unit tests).
  """
  # Open three files and a database session; run the reconciliation query and pipe the output
  # to the files, using per-file predicates to filter out results.
  with exporter.open_writer(path_received, _RECEIVED_PREDICATE) as received_writer, \
       exporter.open_writer(path_late, _LATE_PREDICATE) as late_writer, \
       exporter.open_writer(path_missing, _MISSING_PREDICATE) as missing_writer, \
       database_factory.get_database().session() as session:
    writer = CompositeSqlExportWriter([received_writer, late_writer, missing_writer])
    exporter.run_export_with_session(writer, session, replace_isodate(_RECONCILIATION_REPORT_SQL),
                                     {"biobank_id_prefix": get_biobank_id_prefix()})

# Indexes from the SQL query below; used in predicates.
_SENT_COUNT_INDEX = 2
_SENT_FINALIZED_INDEX = 5
_RECEIVED_TEST_INDEX = 14
_RECEIVED_COUNT_INDEX = 15
_ELAPSED_HOURS_INDEX = 18

_ORDER_JOINS = """
      biobank_order
    INNER JOIN
      participant
    ON
      biobank_order.participant_id = participant.participant_id
    INNER JOIN
      biobank_ordered_sample
    ON
      biobank_order.biobank_order_id = biobank_ordered_sample.order_id
    LEFT OUTER JOIN
      site source_site
    ON biobank_order.source_site_id = source_site.site_id
    LEFT OUTER JOIN
      hpo source_site_hpo
    ON source_site.hpo_id = source_site_hpo.hpo_id
    LEFT OUTER JOIN
      site finalized_site
    ON biobank_order.finalized_site_id = finalized_site.site_id
    LEFT OUTER JOIN
      hpo finalized_site_hpo
    ON finalized_site.hpo_id = finalized_site_hpo.hpo_id
"""

_STORED_SAMPLE_JOIN_CRITERIA = """
      biobank_stored_sample.biobank_id = participant.biobank_id
      AND biobank_stored_sample.test = biobank_ordered_sample.test
"""

# Joins orders and samples, and computes some derived values (elapsed_hours, counts).
# MySQL does not support FULL OUTER JOIN, so instead we UNION ALL a LEFT OUTER JOIN
# with a SELECT... WHERE NOT EXISTS (the latter for cases where we have a sample but no matching
# ordered sample.)
# Column order should match _*_INDEX constants above.
# Biobank ID formatting must match to_client_biobank_id.
_RECONCILIATION_REPORT_SQL = ("""
  SELECT
    CONCAT(:biobank_id_prefix, raw_biobank_id) biobank_id,
    order_test sent_test,
    COUNT(DISTINCT biobank_order_id) sent_count,
    GROUP_CONCAT(DISTINCT biobank_order_id) sent_order_id,
    ISODATE[MAX(collected)] sent_collection_time,
    ISODATE[MAX(finalized)] sent_finalized_time,
    GROUP_CONCAT(DISTINCT source_site_name) source_site_name,
    GROUP_CONCAT(DISTINCT source_site_consortium) source_site_consortium,
    GROUP_CONCAT(DISTINCT source_site_mayolink_client_number) source_site_mayolink_client_number,    
    GROUP_CONCAT(DISTINCT source_site_hpo) source_site_hpo,
    GROUP_CONCAT(DISTINCT finalized_site_name) finalized_site_name,
    GROUP_CONCAT(DISTINCT finalized_site_consortium) finalized_site_consortium,
    GROUP_CONCAT(DISTINCT finalized_site_mayolink_client_number) 
        finalized_site_mayolink_client_number,
    GROUP_CONCAT(DISTINCT finalized_site_hpo) finalized_site_hpo,
    test received_test,
    COUNT(DISTINCT biobank_stored_sample_id) received_count,
    GROUP_CONCAT(DISTINCT biobank_stored_sample_id) received_sample_id,
    ISODATE[MAX(confirmed)] received_time,
    TIMESTAMPDIFF(HOUR, MAX(collected), MAX(confirmed)) elapsed_hours
  FROM
   (SELECT
      participant.biobank_id raw_biobank_id,
      biobank_order.biobank_order_id,
      source_site.site_name source_site_name,
      source_site.consortium_name source_site_consortium,
      source_site.mayolink_client_number source_site_mayolink_client_number,
      source_site_hpo.name source_site_hpo,
      finalized_site.site_name finalized_site_name,
      finalized_site.consortium_name finalized_site_consortium,
      finalized_site.mayolink_client_number finalized_site_mayolink_client_number,
      finalized_site_hpo.name finalized_site_hpo,
      biobank_ordered_sample.test order_test,
      biobank_ordered_sample.collected,
      biobank_ordered_sample.finalized,
      biobank_stored_sample.biobank_stored_sample_id,
      biobank_stored_sample.test,
      biobank_stored_sample.confirmed
    FROM """ + _ORDER_JOINS + """
    LEFT OUTER JOIN
      biobank_stored_sample
    ON """ + _STORED_SAMPLE_JOIN_CRITERIA + """
    UNION ALL
    SELECT
      biobank_stored_sample.biobank_id raw_biobank_id,
      NULL biobank_order_id,
      NULL source_site_name,
      NULL source_site_consortium,
      NULL source_site_mayolink_client_number,
      NULL source_site_hpo,
      NULL finalized_site_name,
      NULL finalized_site_consortium,
      NULL finalized_site_mayolink_client_number,
      NULL finalized_site_hpo,
      NULL order_test,
      NULL collected,
      NULL finalized,
      biobank_stored_sample.biobank_stored_sample_id,
      biobank_stored_sample.test,
      biobank_stored_sample.confirmed
    FROM
      biobank_stored_sample
    WHERE NOT EXISTS (
      SELECT 0 FROM """ + _ORDER_JOINS + " WHERE " + _STORED_SAMPLE_JOIN_CRITERIA + """
    )
  ) reconciled
  GROUP BY
    biobank_id, order_test, test
  ORDER BY
    ISODATE[MAX(collected)], ISODATE[MAX(confirmed)], GROUP_CONCAT(DISTINCT biobank_order_id),
    GROUP_CONCAT(DISTINCT biobank_stored_sample_id)
""")

# Gets all sample/order pairs where everything arrived, regardless of timing.
_RECEIVED_PREDICATE = lambda result: (result[_RECEIVED_TEST_INDEX] and
                                      result[_SENT_COUNT_INDEX] == result[_RECEIVED_COUNT_INDEX])

# Gets orders for which the samples arrived, but they arrived late.
_LATE_PREDICATE = lambda result: (result[_ELAPSED_HOURS_INDEX] and
                                  int(result[_ELAPSED_HOURS_INDEX]) > 24)


# Gets samples or orders where something has gone missing.
_MISSING_PREDICATE = lambda result: (result[_SENT_COUNT_INDEX] != result[_RECEIVED_COUNT_INDEX] or
                                     (result[_SENT_FINALIZED_INDEX] and
                                      not result[_RECEIVED_TEST_INDEX]))
