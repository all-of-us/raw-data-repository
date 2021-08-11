import logging

from dateutil.parser import parse
from rdr_service.storage import GoogleCloudStorageCSVReader
from rdr_service.dao.retention_eligible_metrics_dao import RetentionEligibleMetricsDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.participant_enums import RetentionType, RetentionStatus

_BATCH_SIZE = 1000

# Note: depreciated to switch to using GoogleCloudStorageCSVReader version.
# def import_retention_eligible_metrics_file(task_data):
#     """
#     Import PTSC retention eligible metric file from bucket.
#     :param task_data: Cloud function event dict.
#     """
#     csv_file_cloud_path = task_data["file_path"]
#     upload_date = task_data["upload_date"]
#     dao = RetentionEligibleMetricsDao()
#
#     # Copy bucket file to local temp file.
#     logging.info(f"Opening gs://{csv_file_cloud_path}.")
#     tmp_file = tempfile.NamedTemporaryFile(prefix='ptsc_')
#     with GoogleCloudStorageProvider().open(csv_file_cloud_path, 'rt') as csv_file:
#         while True:
#             chunk = csv_file.read(_CHUNK_SIZE)
#             tmp_file.write(chunk)
#             if not chunk or len(chunk) < _CHUNK_SIZE:
#                 break
#     tmp_file.seek(0)
#
#     header = tmp_file.readline().decode('utf-8')
#     missing_cols = set(RetentionEligibleMetricCsvColumns.ALL) - set(header.split(','))
#     if missing_cols:
#         raise DataError(f"CSV is missing columns {missing_cols}, had columns {header}.")
#
#     strio = io.StringIO()
#     strio.write(header + '\n')
#     batch_count = upsert_count = 0
#     records = list()
#
#     with dao.session() as session:
#         while True:
#             # Create a mini-csv file with a _BATCH_SIZE number of records in the StringIO obj.
#             line = tmp_file.readline().decode('utf-8')
#             if line:
#                 strio.write(line + '\n')
#                 batch_count += 1
#
#             if batch_count == _BATCH_SIZE or not line:
#                 strio.seek(0)
#                 csv_reader = csv.DictReader(strio, delimiter=",")
#                 for row in csv_reader:
#                     if not row[RetentionEligibleMetricCsvColumns.PARTICIPANT_ID]:
#                         continue
#                     record = _create_retention_eligible_metrics_obj_from_row(row, upload_date)
#                     records.append(record)
#                 upsert_count += dao.upsert_all_with_session(session, records)
#                 if not line:
#                     break
#                 # reset for next batch, re-use objects.
#                 batch_count = 0
#                 records.clear()
#                 strio.seek(0)
#                 strio.truncate(0)
#                 strio.write(header + '\n')
#
#     tmp_file.close()
#
#     logging.info(f"Updating participant summary retention eligible flags for {upsert_count} participants...")
#     ParticipantSummaryDao().bulk_update_retention_eligible_flags(upload_date)
#     logging.info(f"Import and update completed for gs://{csv_file_cloud_path}")

def import_retention_eligible_metrics_file(task_data):
    """
    Import PTSC retention eligible metric file from bucket.
    :param task_data: Cloud function event dict.
    """
    csv_file_cloud_path = task_data["file_path"]
    upload_date = task_data["upload_date"]
    dao = RetentionEligibleMetricsDao()

    # Copy bucket file to local temp file.
    logging.info(f"Reading gs://{csv_file_cloud_path}.")
    csv_reader = GoogleCloudStorageCSVReader(csv_file_cloud_path)

    batch_count = upsert_count = 0
    records = list()
    with dao.session() as session:
        for row in csv_reader:
            if not row[RetentionEligibleMetricCsvColumns.PARTICIPANT_ID]:
                continue
            record = _create_retention_eligible_metrics_obj_from_row(row, upload_date)
            records.append(record)
            batch_count += 1

            if batch_count == _BATCH_SIZE:
                upsert_count += dao.upsert_all_with_session(session, records)
                records.clear()
                batch_count = 0

        if records:
            upsert_count += dao.upsert_all_with_session(session, records)

    logging.info(f"Updating participant summary retention eligible flags for {upsert_count} participants...")
    ParticipantSummaryDao().bulk_update_retention_eligible_flags(upload_date)
    logging.info(f"Import and update completed for gs://{csv_file_cloud_path}")


def _parse_field(parser_func, field_str):
    return parser_func(field_str) if field_str not in ('', 'NULL') else None


def _create_retention_eligible_metrics_obj_from_row(row, upload_date):
    retention_eligible = _parse_field(int, row[RetentionEligibleMetricCsvColumns.RETENTION_ELIGIBLE])
    eligible_time = _parse_field(parse, row[RetentionEligibleMetricCsvColumns.RETENTION_ELIGIBLE_TIME])
    actively_retained = _parse_field(int, row[RetentionEligibleMetricCsvColumns.ACTIVELY_RETAINED])
    passively_retained = _parse_field(int, row[RetentionEligibleMetricCsvColumns.PASSIVELY_RETAINED])

    retention_type = RetentionType.UNSET
    if actively_retained and passively_retained:
        retention_type = RetentionType.ACTIVE_AND_PASSIVE
    elif actively_retained:
        retention_type = RetentionType.ACTIVE
    elif passively_retained:
        retention_type = RetentionType.PASSIVE

    return RetentionEligibleMetrics(
        participantId=row[RetentionEligibleMetricCsvColumns.PARTICIPANT_ID],
        retentionEligible=retention_eligible,
        retentionEligibleTime=eligible_time,
        activelyRetained=actively_retained,
        passivelyRetained=passively_retained,
        fileUploadDate=upload_date,
        retentionEligibleStatus=RetentionStatus.ELIGIBLE if retention_eligible else RetentionStatus.NOT_ELIGIBLE,
        retentionType=retention_type
    )


class RetentionEligibleMetricCsvColumns(object):
    PARTICIPANT_ID = "participant_id"
    RETENTION_ELIGIBLE = "retention_eligible"
    RETENTION_ELIGIBLE_TIME = "retention_eligible_date"
    ACTIVELY_RETAINED = "actively_retained"
    PASSIVELY_RETAINED = "passively_retained"

    ALL = (PARTICIPANT_ID, RETENTION_ELIGIBLE, RETENTION_ELIGIBLE_TIME, ACTIVELY_RETAINED, PASSIVELY_RETAINED)


class DataError(RuntimeError):
    def __init__(self, msg, external=False):
        super(DataError, self).__init__(msg)
        self.external = external
