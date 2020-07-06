"""Reads a CSV that Biobank uploads to GCS and upserts to the BiobankStoredSample table.

Also updates ParticipantSummary data related to samples.
"""

import csv
import datetime
import logging
import math
import os

import pytz

from rdr_service import clock, config
from rdr_service.api_util import list_blobs, open_cloud_file
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.code_constants import PPI_SYSTEM, RACE_AIAN_CODE, RACE_QUESTION_CODE
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.database_utils import parse_datetime, replace_isodate
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.config_utils import from_client_biobank_id, get_biobank_id_prefix
from rdr_service.model.participant import Participant
from rdr_service.offline.bigquery_sync import batch_rebuild_participants_task
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.participant_enums import BiobankOrderStatus, OrganizationType, get_sample_status_enum_value

# Format for dates in output filenames for the reconciliation report.
_FILENAME_DATE_FORMAT = "%Y-%m-%d"
# The output of the reconciliation report goes into this subdirectory within the upload bucket.
_REPORT_SUBDIR = "reconciliation"
_GENOMIC_SUBDIR_PREFIX = "genomic_water_line_test"
_BATCH_SIZE = 1000

# Biobank provides timestamps without time zone info, which should be in central time (see DA-235).
_INPUT_TIMESTAMP_FORMAT = "%Y/%m/%d %H:%M:%S"  # like 2016/11/30 14:32:18
_US_CENTRAL = pytz.timezone("US/Central")

# The timestamp found at the end of input CSV files.
INPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
_INPUT_CSV_TIME_FORMAT_LENGTH = 18
_CSV_SUFFIX_LENGTH = 4
_THIRTY_SIX_HOURS_AGO = datetime.timedelta(hours=36)
_MAX_INPUT_AGE = datetime.timedelta(hours=24)
_PMI_OPS_SYSTEM = "https://www.pmi-ops.org"
_CE_QUEST_SYSTEM = "http://careevolution.com/CareTask"
_KIT_ID_SYSTEM = "https://orders.mayomedicallaboratories.com/kit-id"
_TRACKING_NUMBER_SYSTEM = "https://orders.mayomedicallaboratories.com/tracking-number"


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
    csv_file_path, csv_filename, timestamp = get_last_biobank_sample_file_info()

    now = clock.CLOCK.now()
    if now - timestamp > _MAX_INPUT_AGE:
        raise DataError(
            "Input %r (timestamp %s UTC) is > 24h old (relative to %s UTC), not importing."
            % (csv_filename, timestamp, now),
            external=True,
        )
    with open_cloud_file(csv_file_path) as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter="\t")
        written = _upsert_samples_from_csv(csv_reader)

    ts = datetime.datetime.now()
    dao = ParticipantSummaryDao()
    dao.update_from_biobank_stored_samples()
    update_bigquery_sync_participants(ts, dao)

    return written, timestamp

def update_bigquery_sync_participants(ts, dao):
    """
    Update all participants modified by the biobank reconciliation process.
    :param ts: Timestamp
    :param dao: DAO Object
    """
    batch_size = 250

    with dao.session() as session:
        participants = session.query(Participant.participantId).filter(Participant.lastModified > ts).all()

        total_rows = len(participants)
        count = int(math.ceil(float(total_rows) / float(batch_size)))
        logging.info('Biobank: calculated {0} tasks from {1} records with a batch size of {2}.'.
                     format(count, total_rows, batch_size))

        count = 0
        batch_count = 0
        batch = list()

        # queue up a batch of participant ids and send them to be rebuilt.
        for p in participants:

            batch.append({'pid': p.participantId})
            count += 1

            if count == batch_size:
                payload = {'batch': batch}

                if config.GAE_PROJECT == 'localhost':
                    batch_rebuild_participants_task(payload)
                else:
                    task = GCPCloudTask('rebuild_participants_task', payload=payload, in_seconds=15,
                                        queue='resource-rebuild')
                    task.execute(quiet=True)
                batch_count += 1
                # reset for next batch
                batch = list()
                count = 0

        # send last batch if needed.
        if count:
            payload = {'batch': batch}
            batch_count += 1
            if config.GAE_PROJECT == 'localhost':
                batch_rebuild_participants_task(payload)
            else:
                task = GCPCloudTask('rebuild_participants_task', payload=payload, in_seconds=15,
                                    queue='resource-rebuild')
                task.execute(quiet=True)

        logging.info(f'Biobank: submitted {batch_count} tasks.')


def get_last_biobank_sample_file_info(monthly=False):
    """Finds the latest CSV & updates/inserts BiobankStoredSamples from its rows."""
    bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
    if monthly:
        bucket_name = bucket_name + "/60_day_manifests"

    csv_file_path, csv_filename = _open_latest_samples_file(bucket_name)
    timestamp = _timestamp_from_filename(csv_filename)

    return csv_file_path, csv_filename, timestamp


def _timestamp_from_filename(csv_filename):
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


def _open_latest_samples_file(cloud_bucket_name):
    """Returns an open stream for the most recently created CSV in the given bucket."""
    blob_name = _find_latest_samples_csv(cloud_bucket_name)
    file_name = os.path.basename(blob_name)
    path = os.path.normpath(cloud_bucket_name + '/' + blob_name)
    logging.info(f'Opening latest samples CSV in {cloud_bucket_name}: {file_name}')
    return path, file_name


def _find_latest_samples_csv(cloud_bucket_name):
    """Returns the full path (including bucket name) of the most recently created CSV in the bucket.

  Raises:
    RuntimeError: if no CSVs are found in the cloud storage bucket.
  """
    bucket_stat_list = list_blobs(cloud_bucket_name)
    if not bucket_stat_list:
        raise DataError("No files in cloud bucket %r." % cloud_bucket_name)
    # GCS does not really have the concept of directories (it's just a filename convention), so all
    # directory listings are recursive and we must filter out subdirectory contents.
    bucket_stat_list = [
        s
        for s in bucket_stat_list
        if s.name.lower().endswith(".csv")
        and "%s/" % _REPORT_SUBDIR not in s.name
        and "%s" % _GENOMIC_SUBDIR_PREFIX not in s.name
    ]
    if not bucket_stat_list:
        raise DataError("No CSVs in cloud bucket %r (all files: %s)." % (cloud_bucket_name, bucket_stat_list))
    bucket_stat_list.sort(key=lambda s: s.updated)
    return bucket_stat_list[-1].name


class CsvColumns(object):
    """Names of CSV columns that we read from the Biobank samples upload."""

    SAMPLE_ID = "Sample Id"
    PARENT_ID = "Parent Sample Id"
    CONFIRMED_DATE = "Sample Confirmed Date"
    EXTERNAL_PARTICIPANT_ID = "External Participant Id"
    BIOBANK_ORDER_IDENTIFIER = "Sent Order Id"
    TEST_CODE = "Test Code"
    CREATE_DATE = "Sample Family Create Date"
    STATUS = "Sample Disposal Status"
    DISPOSAL_DATE = "Sample Disposed Date"
    SAMPLE_FAMILY = "Sample Family Id"

    # Note: Please ensure changes to the CSV format are reflected in fake_biobanks_sample_generator.
    ALL = (
        SAMPLE_ID,
        PARENT_ID,
        CONFIRMED_DATE,
        EXTERNAL_PARTICIPANT_ID,
        BIOBANK_ORDER_IDENTIFIER,
        TEST_CODE,
        CREATE_DATE,
        STATUS,
        DISPOSAL_DATE,
        SAMPLE_FAMILY,
    )


def _upsert_samples_from_csv(csv_reader):
    """Inserts/updates BiobankStoredSamples from a csv.DictReader."""
    missing_cols = set(CsvColumns.ALL) - set(csv_reader.fieldnames)
    if missing_cols:
        raise DataError("CSV is missing columns %s, had columns %s." % (missing_cols, csv_reader.fieldnames))
    samples_dao = BiobankStoredSampleDao()
    biobank_id_prefix = get_biobank_id_prefix()
    written = 0
    try:
        samples = []
        with ParticipantDao().session() as session:

            for row in csv_reader:
                sample = _create_sample_from_row(row, biobank_id_prefix)
                if sample:
                    # DA-601 - Ensure biobank_id exists before accepting a sample record.
                    if session.query(Participant).filter(Participant.biobankId == sample.biobankId).count() < 1:
                        logging.error(
                            "Bio bank Id ({0}) does not exist in the Participant table.".format(sample.biobankId)
                        )
                        continue

                    samples.append(sample)
                    if len(samples) >= _BATCH_SIZE:
                        written += samples_dao.upsert_all(samples)
                        samples = []

            if samples:
                written += samples_dao.upsert_all(samples)

        return written
    except ValueError:
        raise DataError("Error upserting samples from CSV")


def _parse_timestamp(row, key, sample):
    str_val = row[key]
    if str_val:
        try:
            naive = datetime.datetime.strptime(str_val, _INPUT_TIMESTAMP_FORMAT)
        except ValueError:
            raise DataError(
                "Sample %r for %r has bad timestamp %r"
                % (sample.biobankStoredSampleId, sample.biobankId, str_val)
            )
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
    biobank_id_str = row[CsvColumns.EXTERNAL_PARTICIPANT_ID]
    if not biobank_id_str.startswith(biobank_id_prefix):
        # This is a biobank sample for another environment. Ignore it.
        return None
    if CsvColumns.BIOBANK_ORDER_IDENTIFIER not in row:
        return None
    biobank_id = from_client_biobank_id(biobank_id_str)
    sample = BiobankStoredSample(
        biobankStoredSampleId=row[CsvColumns.SAMPLE_ID],
        biobankId=biobank_id,
        biobankOrderIdentifier=row[CsvColumns.BIOBANK_ORDER_IDENTIFIER],
        test=row[CsvColumns.TEST_CODE],
    )
    if row[CsvColumns.PARENT_ID]:
        # Skip child samples.
        return None

    sample.confirmed = _parse_timestamp(row, CsvColumns.CONFIRMED_DATE, sample)
    sample.created = _parse_timestamp(row, CsvColumns.CREATE_DATE, sample)
    sample.status = get_sample_status_enum_value(row[CsvColumns.STATUS])
    sample.disposed = _parse_timestamp(row, CsvColumns.DISPOSAL_DATE, sample)
    sample.family_id = row[CsvColumns.SAMPLE_FAMILY]

    return sample


def write_reconciliation_report(now, report_type="daily"):
    """Writes order/sample reconciliation reports to GCS."""
    bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)  # raises if missing
    _query_and_write_reports(
        SqlExporter(bucket_name), now, report_type, *_get_report_paths(now, report_type)
    )


def _get_report_paths(report_datetime, report_type="daily"):
    """Returns a list of output filenames for samples: (received, late, missing, withdrawals, salivary_missing)."""

    report_name_suffix = ("received", "missing", "modified", "withdrawals", "salivary_missing")

    if report_type == "monthly":
        report_name_suffix = ("received_monthly", "missing_monthly", "modified_monthly", "withdrawals_monthly")

    return [
        "%s/report_%s_%s.csv" % (_REPORT_SUBDIR, report_datetime.strftime(_FILENAME_DATE_FORMAT), report_name)
        for report_name in report_name_suffix
    ]


def _query_and_write_reports(exporter, now, report_type, path_received,
                             path_missing, path_modified,
                             path_withdrawals, path_salivary_missing=None):
    """Runs the reconciliation MySQL queries and writes result rows to the given CSV writers.

  Note that due to syntax differences, the query runs on MySQL only (not SQLite in unit tests).
  """

    report_cover_range = 10
    if report_type == "monthly":
        report_cover_range = 60

    # Gets all sample/order pairs where everything arrived, within the past n days.
    received_predicate = lambda result: (
        result[_RECEIVED_TEST_INDEX]
        and result[_SENT_COUNT_INDEX] <= result[_RECEIVED_COUNT_INDEX]
        and in_past_n_days(result, now, report_cover_range)
    )

    # Gets samples or orders where something has gone missing within the past n days, and if an order
    # was placed, it was placed at least 36 hours ago.
    missing_predicate = lambda result: (
        (
            result[_SENT_COUNT_INDEX] != result[_RECEIVED_COUNT_INDEX]
            or (result[_SENT_FINALIZED_INDEX] and not result[_RECEIVED_TEST_INDEX])
        )
        and in_past_n_days(result, now, report_cover_range, ordered_before=now - _THIRTY_SIX_HOURS_AGO)
        and result[_EDITED_CANCELLED_RESTORED_STATUS_FLAG_INDEX] != 'cancelled'
    )

    # Gets samples or orders where something has modified within the past n days.
    modified_predicate = lambda result: (
        result[_EDITED_CANCELLED_RESTORED_STATUS_FLAG_INDEX] and in_past_n_days(result, now, report_cover_range)
    )

    code_dao = CodeDao()
    race_question_code = code_dao.get_code(PPI_SYSTEM, RACE_QUESTION_CODE)
    native_american_race_code = code_dao.get_code(PPI_SYSTEM, RACE_AIAN_CODE)

    # break into three steps to avoid OOM issue
    report_paths = [path_received, path_missing, path_modified]
    report_predicates = [received_predicate, missing_predicate, modified_predicate]

    for report_path, report_predicate in zip(report_paths, report_predicates):
        dv_filter = 1 if report_path == path_missing else 0
        #with exporter.open_writer(report_path, report_predicate) as report_writer:
        query_params = {
                    "race_question_code_id": race_question_code.codeId,
                    "native_american_race_code_id": native_american_race_code.codeId,
                    "biobank_id_prefix": get_biobank_id_prefix(),
                    "pmi_ops_system": _PMI_OPS_SYSTEM,
                    "ce_quest_system": _CE_QUEST_SYSTEM,
                    "kit_id_system": _KIT_ID_SYSTEM,
                    "tracking_number_system": _TRACKING_NUMBER_SYSTEM,
                    "n_days_ago": now - datetime.timedelta(days=(report_cover_range + 1)),
                    "dv_order_filter": dv_filter
                }

        logging.info(f"Writing {report_path} report.")
        exporter.run_export(report_path, replace_isodate(_RECONCILIATION_REPORT_SQL),
                            query_params, backup=True, predicate=report_predicate)
        logging.info(f"Completed {report_path} report.")

    # Now generate the withdrawal report, within the past n days.
    exporter.run_export(
        path_withdrawals,
        replace_isodate(_WITHDRAWAL_REPORT_SQL),
        {
            "race_question_code_id": race_question_code.codeId,
            "native_american_race_code_id": native_american_race_code.codeId,
            "n_days_ago": now - datetime.timedelta(days=report_cover_range),
            "biobank_id_prefix": get_biobank_id_prefix(),
        },
        backup=True,
    )
    logging.info(f"Completed {path_withdrawals} report.")

    # Generate the missing salivary report, within last n days (10 1/20)
    if report_type != "monthly" and path_salivary_missing is not None:
        exporter.run_export(
            path_salivary_missing,
            _SALIVARY_MISSING_REPORT_SQL,
            {
                "biobank_id_prefix": get_biobank_id_prefix(),
                "n_days_interval": 10,
            },
            backup=True,
        )
    logging.info("Completed monthly reconciliation report.")


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
_EDITED_CANCELLED_RESTORED_STATUS_FLAG_INDEX = 28

_ORDER_JOINS = """
      biobank_order
    INNER JOIN
      participant
    ON
      biobank_order.participant_id = participant.participant_id
    INNER JOIN
      biobank_order_identifier
    ON biobank_order.biobank_order_id = biobank_order_identifier.biobank_order_id
       AND biobank_order_identifier.system in (:pmi_ops_system, :ce_quest_system)
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
    LEFT OUTER JOIN
      site restored_site
    ON biobank_order.restored_site_id = restored_site.site_id
    LEFT OUTER JOIN
      site amended_site
    ON biobank_order.amended_site_id = amended_site.site_id
    LEFT OUTER JOIN
      site cancelled_site
    ON biobank_order.cancelled_site_id = cancelled_site.site_id
"""

_STORED_SAMPLE_JOIN_CRITERIA = """
      biobank_stored_sample.biobank_id = participant.biobank_id
      AND biobank_stored_sample.test = biobank_ordered_sample.test
      AND biobank_stored_sample.biobank_order_identifier = biobank_order_identifier.value
      AND biobank_ordered_sample.finalized IS NOT NULL
      AND biobank_stored_sample.confirmed IS NOT NULL
"""


def _get_hpo_type_sql(hpo_alias):
    result = "(CASE "
    for organization_type in OrganizationType:
        result += "WHEN %s.organization_type = %d THEN '%s' " % (
            hpo_alias,
            organization_type.number,
            organization_type.name,
        )
    result += "ELSE 'UNKNOWN' END)"
    return result


def _get_status_flag_sql():
    result = """
      CASE
        WHEN biobank_order.order_status = {amended} THEN 'edited'
        WHEN biobank_order.order_status = {cancelled} THEN 'cancelled'
        WHEN biobank_order.order_status = {unset} AND biobank_order.restored_time IS NOT NULL
          THEN 'restored'
        ELSE NULL
      END edited_cancelled_restored_status_flag,
      CASE
        WHEN biobank_order.order_status = {amended} THEN biobank_order.amended_username
        WHEN biobank_order.order_status = {cancelled} THEN biobank_order.cancelled_username
        WHEN biobank_order.order_status = {unset} AND biobank_order.restored_time IS NOT NULL
          THEN biobank_order.restored_username
        ELSE NULL
      END edited_cancelled_restored_name,
      CASE
        WHEN biobank_order.order_status = {amended} THEN amended_site.site_name
        WHEN biobank_order.order_status = {cancelled} THEN cancelled_site.site_name
        WHEN biobank_order.order_status = {unset} AND biobank_order.restored_time IS NOT NULL
          THEN restored_site.site_name
        ELSE NULL
      END edited_cancelled_restored_site_name,
      CASE
        WHEN biobank_order.order_status = {amended} THEN biobank_order.amended_time
        WHEN biobank_order.order_status = {cancelled} THEN biobank_order.cancelled_time
        WHEN biobank_order.order_status = {unset} AND biobank_order.restored_time IS NOT NULL
          THEN biobank_order.restored_time
        ELSE NULL
      END edited_cancelled_restored_site_time,
      CASE
        WHEN biobank_order.order_status = {amended} OR biobank_order.order_status = {cancelled} OR
             (biobank_order.order_status = {unset} AND biobank_order.restored_time IS NOT NULL)
          THEN biobank_order.amended_reason
        ELSE NULL
      END edited_cancelled_restored_site_reason
  """.format(
        amended=int(BiobankOrderStatus.AMENDED),
        cancelled=int(BiobankOrderStatus.CANCELLED),
        unset=int(BiobankOrderStatus.UNSET),
    )

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
_RECONCILIATION_REPORT_SQL = (
    """
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
    GROUP_CONCAT(notes_finalized) notes_finalized,
    GROUP_CONCAT(edited_cancelled_restored_status_flag) edited_cancelled_restored_status_flag,
    GROUP_CONCAT(edited_cancelled_restored_name) edited_cancelled_restored_name,
    GROUP_CONCAT(edited_cancelled_restored_site_name) edited_cancelled_restored_site_name,
    GROUP_CONCAT(edited_cancelled_restored_site_time) edited_cancelled_restored_site_time,
    GROUP_CONCAT(edited_cancelled_restored_site_reason) edited_cancelled_restored_site_reason,
    GROUP_CONCAT(DISTINCT order_origin) biobank_order_origin
  FROM
   (SELECT
      participant.biobank_id raw_biobank_id,
      biobank_order_identifier.value biobank_order_id,
      source_site.site_name source_site_name,
      source_site.mayolink_client_number source_site_mayolink_client_number,
      source_site_hpo.name source_site_hpo,
      """
    + _get_hpo_type_sql("source_site_hpo")
    + """ source_site_hpo_type,
      finalized_site.site_name finalized_site_name,
      finalized_site.mayolink_client_number finalized_site_mayolink_client_number,
      finalized_site_hpo.name finalized_site_hpo,
      """
    + _get_hpo_type_sql("finalized_site_hpo")
    + """ finalized_site_hpo_type,
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
      tracking_number_identifier.value fedex_tracking_number, """
    + _NATIVE_AMERICAN_SQL
    + """,
      biobank_order.collected_note notes_collected,
      biobank_order.processed_note notes_processed,
      biobank_order.finalized_note notes_finalized,
      biobank_order.order_origin,
      """
    + _get_status_flag_sql()
    + """
    FROM """
    + _ORDER_JOINS
    + """
    LEFT OUTER JOIN
        biobank_dv_order dv_order
    ON dv_order.biobank_order_id = biobank_order.biobank_order_id
        AND dv_order.is_test_sample IS NOT TRUE
    LEFT OUTER JOIN
      biobank_stored_sample
    ON """
    + _STORED_SAMPLE_JOIN_CRITERIA
    + """
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
      AND NOT EXISTS (
        SELECT 0 FROM participant
        WHERE participant.participant_id = dv_order.participant_id
      )
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
      NULL fedex_tracking_number, """
    + _NATIVE_AMERICAN_SQL
    + """,
      NULL notes_collected,
      NULL notes_processed,
      NULL notes_finalized,
      NULL edited_cancelled_restored_status_flag,
      NULL edited_cancelled_restored_name,
      NULL edited_cancelled_restored_site_name,
      NULL edited_cancelled_restored_site_time,
      NULL edited_cancelled_restored_site_reason,
      NULL order_origin
    FROM
      biobank_stored_sample
      LEFT OUTER JOIN
        participant ON biobank_stored_sample.biobank_id = participant.biobank_id
    WHERE biobank_stored_sample.confirmed IS NOT NULL AND NOT EXISTS (
      SELECT 0 FROM """
    + _ORDER_JOINS
    + " WHERE "
    + _STORED_SAMPLE_JOIN_CRITERIA
    + """
    ) AND NOT EXISTS (
      SELECT 0 FROM participant
       WHERE participant.biobank_id = biobank_stored_sample.biobank_id
         AND participant.withdrawal_time IS NOT NULL)
    AND
        (
            CASE
                WHEN 1 = :dv_order_filter THEN
                    biobank_stored_sample.biobank_id NOT IN (
                    SELECT p.biobank_id
                     FROM participant p
                        JOIN biobank_dv_order dv
                        ON p.participant_id = dv.participant_id
                        AND dv.is_test_sample IS NOT TRUE)
                ELSE TRUE
            END
        )
  ) reconciled
  WHERE (reconciled.collected IS NOT NULL
    AND reconciled.confirmed IS NOT NULL
    AND reconciled.collected >= reconciled.confirmed
    AND reconciled.collected >= :n_days_ago)
  OR (reconciled.collected IS NOT NULL
    AND reconciled.confirmed IS NOT NULL
    AND reconciled.confirmed >= reconciled.collected
    AND reconciled.confirmed >= :n_days_ago)
  OR (reconciled.collected IS NULL AND reconciled.confirmed  IS NOT NULL AND reconciled.confirmed >= :n_days_ago)
  OR (reconciled.collected IS NOT NULL AND reconciled.confirmed  IS NULL AND reconciled.collected >= :n_days_ago)
  GROUP BY
    biobank_id, sent_order_id, order_test, test
  ORDER BY
    ISODATE[MAX(collected)], ISODATE[MAX(confirmed)], GROUP_CONCAT(DISTINCT biobank_order_id),
    GROUP_CONCAT(DISTINCT biobank_stored_sample_id)
"""
)

# Generates a report on participants that have withdrawn in the past n days,
# including their biobank ID, withdrawal time, and whether they are Native American
# (as biobank samples for Native Americans are disposed of differently.)
# only send Biobank IDs of participants that had samples collected.
_WITHDRAWAL_REPORT_SQL = (
    """
  SELECT
    CONCAT(:biobank_id_prefix, participant.biobank_id) biobank_id,
    ISODATE[participant.withdrawal_time] withdrawal_time,"""
    + _NATIVE_AMERICAN_SQL
    + """
  FROM participant
  WHERE participant.withdrawal_time >= :n_days_ago
  AND
  (SELECT COUNT(*) FROM biobank_stored_sample WHERE biobank_id=participant.biobank_id)>0
"""
)

_SALIVARY_MISSING_REPORT_SQL = (
    """
    SELECT DISTINCT
      CONCAT(:biobank_id_prefix, p.biobank_id) AS biobank_id
    , dvo.tracking_id AS usps_tracking_id
    , dvo.biobank_order_id AS order_id
    , bo.created AS collection_date
FROM
    biobank_dv_order dvo
    JOIN participant p ON p.participant_id = dvo.participant_id
    JOIN biobank_order bo ON bo.biobank_order_id = dvo.biobank_order_id
    JOIN biobank_ordered_sample bos ON bos.order_id = bo.biobank_order_id
    JOIN biobank_order_identifier boi ON boi.biobank_order_id = bo.biobank_order_id
    LEFT JOIN biobank_stored_sample bss ON bss.biobank_id = p.biobank_id
WHERE TRUE
     AND (
         bo.created < DATE_SUB(now(), INTERVAL :n_days_interval DAY)
     )
     AND bss.biobank_stored_sample_id IS NULL
     AND dvo.is_test_sample IS NOT TRUE
    """
)

def in_past_n_days(result, now, n_days, ordered_before=None):
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
        return (now - max_time).days <= n_days
    return False
