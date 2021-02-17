import logging
import csv

from dateutil.parser import parse
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.retention_eligible_metrics_dao import RetentionEligibleMetricsDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.participant_enums import RetentionType, RetentionStatus

_BATCH_SIZE = 1000


def import_retention_eligible_metrics_file(task_data):
    bucket_name = task_data["bucket"]
    csv_file_cloud_path = task_data["file_path"]
    upload_date = task_data["upload_date"]
    with open_cloud_file(csv_file_cloud_path) as csv_file:
        logging.info("Opening  CSV in {}: {}.".format(bucket_name, csv_file_cloud_path))
        csv_reader = csv.DictReader(csv_file, delimiter=",")
        missing_cols = set(RetentionEligibleMetricCsvColumns.ALL) - set(csv_reader.fieldnames)
        if missing_cols:
            raise DataError("CSV is missing columns %s, had columns %s." % (missing_cols, csv_reader.fieldnames))

        retention_eligible_metrics_dao = RetentionEligibleMetricsDao()
        records = []
        rows = list(csv_reader)
        with retention_eligible_metrics_dao.session() as session:
            count = 0
            for row in rows:
                if not row[RetentionEligibleMetricCsvColumns.PARTICIPANT_ID]:
                    continue
                record = _create_retention_eligible_metrics_obj_from_row(row, upload_date)
                records.append(record)
                if len(records) >= _BATCH_SIZE:
                    upsert_count = retention_eligible_metrics_dao.upsert_all_with_session(session, records)
                    session.commit()
                    records = []
                    count = count + upsert_count
            if records:
                upsert_count = retention_eligible_metrics_dao.upsert_all_with_session(session, records)
                session.commit()
                count = count + upsert_count
        logging.info("Updating participant summary retention eligible flags for {} participants...".format(count))
    participant_summary_dao = ParticipantSummaryDao()
    participant_summary_dao.bulk_update_retention_eligible_flags(upload_date)
    logging.info("import and update completed, file name: {}".format(csv_file_cloud_path))


def _create_retention_eligible_metrics_obj_from_row(row, upload_date):
    retention_eligible = int(row[RetentionEligibleMetricCsvColumns.RETENTION_ELIGIBLE]) \
        if row[RetentionEligibleMetricCsvColumns.RETENTION_ELIGIBLE] != '' else None
    eligible_time = parse(row[RetentionEligibleMetricCsvColumns.RETENTION_ELIGIBLE_TIME]) \
        if row[RetentionEligibleMetricCsvColumns.RETENTION_ELIGIBLE_TIME] not in ('', 'NULL') else None
    actively_retained = int(row[RetentionEligibleMetricCsvColumns.ACTIVELY_RETAINED]) \
        if row[RetentionEligibleMetricCsvColumns.ACTIVELY_RETAINED] != '' else None
    passively_retained = int(row[RetentionEligibleMetricCsvColumns.PASSIVELY_RETAINED]) \
        if row[RetentionEligibleMetricCsvColumns.PASSIVELY_RETAINED] != '' else None

    retention_type = RetentionType.UNSET
    if actively_retained and passively_retained:
        retention_type = RetentionType.ACTIVE_AND_PASSIVE
    elif actively_retained:
        retention_type = RetentionType.ACTIVE
    elif passively_retained:
        retention_type = RetentionType.PASSIVE

    kwargs = dict(
        participantId=row[RetentionEligibleMetricCsvColumns.PARTICIPANT_ID],
        retentionEligible=retention_eligible,
        retentionEligibleTime=eligible_time,
        activelyRetained=actively_retained,
        passivelyRetained=passively_retained,
        fileUploadDate=upload_date,
        retentionEligibleStatus=RetentionStatus.ELIGIBLE if retention_eligible else RetentionStatus.NOT_ELIGIBLE,
        retentionType=retention_type
    )

    obj = RetentionEligibleMetrics(**kwargs)

    return obj


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
