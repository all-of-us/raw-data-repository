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
from code_constants import RACE_QUESTION_CODE, RACE_AIAN_CODE, PPI_SYSTEM
from dao import database_factory
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.code_dao import CodeDao
from dao.database_utils import replace_isodate, parse_datetime
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample
from model.config_utils import from_client_biobank_id, get_biobank_id_prefix
from offline.sql_exporter import SqlExporter, CompositeSqlExportWriter
from participant_enums import OrganizationType

# Format for dates in output filenames for the reconciliation report.
_FILENAME_DATE_FORMAT = '%Y-%m-%d'
# The output of the reconciliation report goes into this subdirectory within the upload bucket.
_REPORT_SUBDIR = 'reconciliation'
_BATCH_SIZE = 1000

# Biobank provides timestamps without time zone info, which should be in central time (see DA-235).
_INPUT_TIMESTAMP_FORMAT = '%Y/%m/%d %H:%M:%S'  # like 2016/11/30 14:32:18
_US_CENTRAL = pytz.timezone('US/Central')

# The timestamp found at the end of input CSV files.
INPUT_CSV_TIME_FORMAT = '%Y-%m-%d-%H-%M-%S'
_INPUT_CSV_TIME_FORMAT_LENGTH = 18
_CSV_SUFFIX_LENGTH = 4
_THIRTY_SIX_HOURS_AGO = datetime.timedelta(hours=36)
_MAX_INPUT_AGE = datetime.timedelta(hours=24)
_PMI_OPS_SYSTEM = 'https://www.pmi-ops.org'
_KIT_ID_SYSTEM = 'https://orders.mayomedicallaboratories.com/kit-id'
_TRACKING_NUMBER_SYSTEM = 'https://orders.mayomedicallaboratories.com/tracking-number'

class DataError(RuntimeError):
  """Bad sample data during import.

  Args:
    msg: Passed through to superclass.
    external: If True, this error should be reported to external partners (Biobank). Externally
        reported DataErrors are only reported if biobank recipients are in the config.
  """
  def __init__(self, msg, external=False):
    super(DataError, self).__init__(msg)
    self.external = external


def upsert_from_latest_csv():
  """Finds the latest CSV & updates/inserts BiobankStoredSamples from its rows."""
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
  csv_file, csv_filename = _open_latest_samples_file(bucket_name)
  timestamp = _timestamp_from_filename(csv_filename)

  now = clock.CLOCK.now()
  if now - timestamp > _MAX_INPUT_AGE:
    raise DataError(
        'Input %r (timestamp %s UTC) is > 24h old (relative to %s UTC), not importing.'
        % (csv_filename, timestamp, now),
        external=True)

  csv_reader = csv.DictReader(csv_file, delimiter='\t')
  written = _upsert_samples_from_csv(csv_reader)
  ParticipantSummaryDao().update_from_biobank_stored_samples()
  return written, timestamp


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
  BIOBANK_ORDER_IDENTIFIER = 'Sent Order Id'
  TEST_CODE = 'Test Code'
  CREATE_DATE = 'Sample Family Create Date'
  ALL = frozenset([SAMPLE_ID, PARENT_ID, CONFIRMED_DATE, EXTERNAL_PARTICIPANT_ID,
                   BIOBANK_ORDER_IDENTIFIER, TEST_CODE,
                   CREATE_DATE])


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
  if _Columns.BIOBANK_ORDER_IDENTIFIER not in row:
    return None
  biobank_id = from_client_biobank_id(biobank_id_str)
  sample = BiobankStoredSample(
      biobankStoredSampleId=row[_Columns.SAMPLE_ID],
      biobankId=biobank_id,
      biobankOrderIdentifier=row[_Columns.BIOBANK_ORDER_IDENTIFIER],
      test=row[_Columns.TEST_CODE])
  if row[_Columns.PARENT_ID]:
    # Skip child samples.
    return None
  sample.confirmed = _parse_timestamp(row, _Columns.CONFIRMED_DATE, sample)
  sample.created = _parse_timestamp(row, _Columns.CREATE_DATE, sample)
  return sample

def write_reconciliation_report(now):
  """Writes order/sample reconciliation reports to GCS."""
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
  _query_and_write_reports(SqlExporter(bucket_name, use_unicode=True), now, *_get_report_paths(now))


def _get_report_paths(report_datetime):
  """Returns a triple of output filenames for samples: (received, late, missing)."""
  return [
      '%s/report_%s_%s.csv' % (
          _REPORT_SUBDIR, report_datetime.strftime(_FILENAME_DATE_FORMAT), report_name)
      for report_name in ('received', 'over_24h', 'missing', 'withdrawals')]


def _query_and_write_reports(exporter, now, path_received, path_late, path_missing,
                             path_withdrawals):
  """Runs the reconciliation MySQL queries and writes result rows to the given CSV writers.

  Note that due to syntax differences, the query runs on MySQL only (not SQLite in unit tests).
  """
  # Gets all sample/order pairs where everything arrived, regardless of timing.
  received_predicate = lambda result: (result[_RECEIVED_TEST_INDEX] and
                                        result[_SENT_COUNT_INDEX] == result[_RECEIVED_COUNT_INDEX])

  # Gets orders for which the samples arrived, but they arrived late, within the past 7 days.
  late_predicate = lambda result: (result[_ELAPSED_HOURS_INDEX] and
                                    int(result[_ELAPSED_HOURS_INDEX]) >= 24 and
                                    in_past_week(result, now))

  # Gets samples or orders where something has gone missing within the past 7 days, and if an order
  # was placed, it was placed at least 36 hours ago.
  missing_predicate = lambda result: ((result[_SENT_COUNT_INDEX] != result[_RECEIVED_COUNT_INDEX] or
                                      (result[_SENT_FINALIZED_INDEX] and
                                      not result[_RECEIVED_TEST_INDEX])) and
                                      in_past_week(result, now,
                                      ordered_before=now - _THIRTY_SIX_HOURS_AGO))

  code_dao = CodeDao()
  race_question_code = code_dao.get_code(PPI_SYSTEM, RACE_QUESTION_CODE)
  native_american_race_code = code_dao.get_code(PPI_SYSTEM, RACE_AIAN_CODE)

  # Open three files and a database session; run the reconciliation query and pipe the output
  # to the files, using per-file predicates to filter out results.
  with exporter.open_writer(path_received, received_predicate) as received_writer, \
       exporter.open_writer(path_late, late_predicate) as late_writer, \
       exporter.open_writer(path_missing, missing_predicate) as missing_writer, \
       database_factory.get_database().session() as session:
    writer = CompositeSqlExportWriter([received_writer, late_writer, missing_writer])
    exporter.run_export_with_session(writer, session, replace_isodate(_RECONCILIATION_REPORT_SQL),
                                     {'race_question_code_id': race_question_code.codeId,
                                      'native_american_race_code_id':
                                        native_american_race_code.codeId,
                                      'biobank_id_prefix': get_biobank_id_prefix(),
                                      'pmi_ops_system': _PMI_OPS_SYSTEM,
                                      'kit_id_system': _KIT_ID_SYSTEM,
                                      'tracking_number_system': _TRACKING_NUMBER_SYSTEM})

  # Now generate the withdrawal report.
  exporter.run_export(path_withdrawals, replace_isodate(_WITHDRAWAL_REPORT_SQL),
                      {'race_question_code_id': race_question_code.codeId,
                       'native_american_race_code_id': native_american_race_code.codeId,
                       'seven_days_ago': now - datetime.timedelta(days=7),
                       'biobank_id_prefix': get_biobank_id_prefix()})

# Indexes from the SQL query below; used in predicates.
_SENT_COUNT_INDEX = 2
_SENT_COLLECTION_TIME_INDEX = 4
_SENT_FINALIZED_INDEX = 6
_RECEIVED_TEST_INDEX = 16
_RECEIVED_COUNT_INDEX = 17
# TODO: remove received time once Biobank stops using it (DA-374)
_RECEIVED_TIME_INDEX = 19
_SAMPLE_FAMILY_CREATE_TIME_INDEX = 20
_ELAPSED_HOURS_INDEX = 21

_ORDER_JOINS = """
      biobank_order
    INNER JOIN
      participant
    ON
      biobank_order.participant_id = participant.participant_id
    INNER JOIN
      biobank_order_identifier
    ON biobank_order.biobank_order_id = biobank_order_identifier.biobank_order_id
       AND biobank_order_identifier.system = :pmi_ops_system
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
      AND biobank_stored_sample.biobank_order_identifier = biobank_order_identifier.value
      AND biobank_ordered_sample.finalized IS NOT NULL
      AND biobank_stored_sample.confirmed IS NOT NULL
"""

def _get_hpo_type_sql(hpo_alias):
  result = '(CASE '
  for organization_type in OrganizationType:
    result += "WHEN %s.organization_type = %d THEN '%s' " % (hpo_alias, organization_type.number,
                                                            organization_type.name)
  result += "ELSE 'UNKNOWN' END)"
  return result

# Used in the context of queries where "participant" is the table for the participant being
# selected.
_NATIVE_AMERICAN_SQL = """
  (SELECT (CASE WHEN count(*) > 0 THEN 'Y' ELSE 'N' END)
       FROM questionnaire_response qr
       INNER JOIN questionnaire_response_answer qra
         ON qra.questionnaire_response_id = qr.questionnaire_response_id
       INNER JOIN questionnaire_question qq
         ON qra.question_id = qq.questionnaire_question_id
      WHERE qr.participant_id = participant.participant_id
        AND qq.code_id = :race_question_code_id
        AND qra.value_code_id = :native_american_race_code_id
        AND qra.end_time IS NULL) is_native_american"""

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
    SUM(finalized is not NULL) sent_count,
    biobank_order_id sent_order_id,
    ISODATE[MAX(collected)] sent_collection_time,
    ISODATE[MAX(processed)] sent_processed_time,
    ISODATE[MAX(finalized)] sent_finalized_time,
    GROUP_CONCAT(DISTINCT source_site_name) source_site_name,
    GROUP_CONCAT(DISTINCT source_site_mayolink_client_number) source_site_mayolink_client_number,
    GROUP_CONCAT(DISTINCT source_site_hpo) source_site_hpo,
    GROUP_CONCAT(DISTINCT source_site_hpo_type) source_site_hpo_type,
    GROUP_CONCAT(DISTINCT finalized_site_name) finalized_site_name,
    GROUP_CONCAT(DISTINCT finalized_site_mayolink_client_number)
        finalized_site_mayolink_client_number,
    GROUP_CONCAT(DISTINCT finalized_site_hpo) finalized_site_hpo,
    GROUP_CONCAT(DISTINCT finalized_site_hpo_type) finalized_site_hpo_type,
    GROUP_CONCAT(DISTINCT finalized_username) finalized_username,
    test received_test,
    COUNT(DISTINCT biobank_stored_sample_id) received_count,
    GROUP_CONCAT(DISTINCT biobank_stored_sample_id) received_sample_id,
    ISODATE[MAX(confirmed)] received_time,
    ISODATE[MAX(created)] 'Sample Family Create Date',
    TIMESTAMPDIFF(HOUR, MAX(collected), MAX(created)) elapsed_hours,
    GROUP_CONCAT(DISTINCT biospecimen_kit_id) biospecimen_kit_id,
    GROUP_CONCAT(DISTINCT fedex_tracking_number) fedex_tracking_number,
    GROUP_CONCAT(DISTINCT is_native_american) is_native_american,
    GROUP_CONCAT(notes_collected) notes_collected,
    GROUP_CONCAT(notes_processed) notes_processed,
    GROUP_CONCAT(notes_finalized) notes_finalized
  FROM
   (SELECT
      participant.biobank_id raw_biobank_id,
      biobank_order_identifier.value biobank_order_id,
      source_site.site_name source_site_name,
      source_site.mayolink_client_number source_site_mayolink_client_number,
      source_site_hpo.name source_site_hpo,
      """
      + _get_hpo_type_sql('source_site_hpo') + """ source_site_hpo_type,
      finalized_site.site_name finalized_site_name,
      finalized_site.mayolink_client_number finalized_site_mayolink_client_number,
      finalized_site_hpo.name finalized_site_hpo,
      """
      + _get_hpo_type_sql('finalized_site_hpo') + """ finalized_site_hpo_type,
      biobank_order.finalized_username finalized_username,
      biobank_ordered_sample.test order_test,
      biobank_ordered_sample.collected,
      biobank_ordered_sample.processed,
      biobank_ordered_sample.finalized,
      biobank_stored_sample.biobank_stored_sample_id,
      biobank_stored_sample.test,
      biobank_stored_sample.confirmed,
      biobank_stored_sample.created,
      kit_id_identifier.value biospecimen_kit_id,
      tracking_number_identifier.value fedex_tracking_number, """ + _NATIVE_AMERICAN_SQL + """,
      biobank_order.collected_note notes_collected,
      biobank_order.processed_note notes_processed,
      biobank_order.finalized_note notes_finalized
    FROM """ + _ORDER_JOINS + """
    LEFT OUTER JOIN
      biobank_stored_sample
    ON """ + _STORED_SAMPLE_JOIN_CRITERIA + """
    LEFT OUTER JOIN
      biobank_order_identifier kit_id_identifier
    ON biobank_order.biobank_order_id = kit_id_identifier.biobank_order_id
       AND kit_id_identifier.system = :kit_id_system
    LEFT OUTER JOIN
      biobank_order_identifier tracking_number_identifier
    ON biobank_order.biobank_order_id = tracking_number_identifier.biobank_order_id
       AND tracking_number_identifier.system = :tracking_number_system
    WHERE
      participant.withdrawal_time IS NULL
    UNION ALL
    SELECT
      biobank_stored_sample.biobank_id raw_biobank_id,
      biobank_stored_sample.biobank_order_identifier,
      NULL source_site_name,
      NULL source_site_mayolink_client_number,
      NULL source_site_hpo,
      NULL source_site_hpo_type,
      NULL finalized_site_name,
      NULL finalized_site_mayolink_client_number,
      NULL finalized_site_hpo,
      NULL finalized_site_hpo_type,
      NULL finalized_username,
      NULL order_test,
      NULL collected,
      NULL processed,
      NULL finalized,
      biobank_stored_sample.biobank_stored_sample_id,
      biobank_stored_sample.test,
      biobank_stored_sample.confirmed,
      biobank_stored_sample.created,
      NULL biospecimen_kit_id,
      NULL fedex_tracking_number, """ + _NATIVE_AMERICAN_SQL + """,
      NULL notes_collected,
      NULL notes_processed,
      NULL notes_finalized
    FROM
      biobank_stored_sample
      LEFT OUTER JOIN
        participant ON biobank_stored_sample.biobank_id = participant.biobank_id
    WHERE biobank_stored_sample.confirmed IS NOT NULL AND NOT EXISTS (
      SELECT 0 FROM """ + _ORDER_JOINS + " WHERE " + _STORED_SAMPLE_JOIN_CRITERIA + """
    ) AND NOT EXISTS (
      SELECT 0 FROM participant
       WHERE participant.biobank_id = biobank_stored_sample.biobank_id
         AND participant.withdrawal_time IS NOT NULL)
  ) reconciled
  GROUP BY
    biobank_id, sent_order_id, order_test, test
  ORDER BY
    ISODATE[MAX(collected)], ISODATE[MAX(confirmed)], GROUP_CONCAT(DISTINCT biobank_order_id),
    GROUP_CONCAT(DISTINCT biobank_stored_sample_id)
""")

# Generates a report on participants that have withdrawn in the past 7 days,
# including their biobank ID, withdrawal time, and whether they are Native American
# (as biobank samples for Native Americans are disposed of differently.)
_WITHDRAWAL_REPORT_SQL = ("""
  SELECT
    CONCAT(:biobank_id_prefix, participant.biobank_id) biobank_id,
    ISODATE[participant.withdrawal_time] withdrawal_time,""" + _NATIVE_AMERICAN_SQL + """
    FROM participant
   WHERE participant.withdrawal_time >= :seven_days_ago
""")


def in_past_week(result, now, ordered_before=None):
  sent_collection_time_str = result[_SENT_COLLECTION_TIME_INDEX]
  received_time_str = result[_RECEIVED_TIME_INDEX]
  max_time = None
  if sent_collection_time_str:
    max_time = parse_datetime(sent_collection_time_str)
    if ordered_before and max_time > ordered_before:
      return False
  if received_time_str:
    received_time = parse_datetime(received_time_str)
    if received_time and max_time:
      max_time = max(received_time, max_time)
    else:
      max_time = received_time
  if max_time:
    return (now - max_time).days <= 7
  return False
