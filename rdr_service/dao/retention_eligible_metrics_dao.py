
from sqlalchemy.orm import Session

from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.participant_enums import RetentionType


class RetentionEligibleMetricsDao(UpdatableDao):

    def __init__(self):
        super(RetentionEligibleMetricsDao, self).__init__(RetentionEligibleMetrics, order_by_ending=["id"])

    @classmethod
    def find_metric_with_session(cls, session: Session, metrics_obj: RetentionEligibleMetrics) -> (int, bool):
        """
        Used to check the db for existing metrics objects for a participant. If a metrics object exists, then its
        id is returned as the first value. The second parameter is used to indicate whether the database has the
        same values, or if an operation is needed to bring the new data into the database.
        """

        db_result = session.query(RetentionEligibleMetrics).filter(
            RetentionEligibleMetrics.participantId == metrics_obj.participantId
        ).first()

        if not db_result:
            return None, True

        is_same_data = (  # Only comparing the data received from PTSC
            db_result.retentionEligibleStatus == metrics_obj.retentionEligibleStatus
            and (
                db_result.retentionType == metrics_obj.retentionType
                or (
                    db_result.retentionType is None
                    and metrics_obj.retentionType == RetentionType.UNSET
                )
            ) and db_result.retentionEligibleTime == metrics_obj.retentionEligibleTime
            and db_result.lastActiveRetentionActivityTime == metrics_obj.lastActiveRetentionActivityTime
        )

        return db_result.id, not is_same_data  # returning the id and whether an update is needed

    def _submit_rebuild_task(self, pids):
        """
        Rebuild Retention Eligible Metrics resource records
        :param pids: List of participant ids.
        """
        task = GCPCloudTask()
        params = {'batch': pids}
        task.execute('batch_rebuild_retention_eligible_task', queue='resource-tasks', payload=params,
                     in_seconds=30, project_id='all-of-us-rdr-prod')

    def upsert_all_with_session(self, session, retention_eligible_metrics_records):
        update_queue = list()
        for record in retention_eligible_metrics_records:
            session.merge(record)
            # Batch up records for resource rebuilding.
            update_queue.append(int(record.participantId))

        session.commit()
        if update_queue:
            self._submit_rebuild_task(update_queue)
        return len(update_queue)
