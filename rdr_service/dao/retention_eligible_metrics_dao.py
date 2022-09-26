from rdr_service.config import GAE_PROJECT
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.resource import generators
from rdr_service.participant_enums import RetentionType


class RetentionEligibleMetricsDao(UpdatableDao):

    def __init__(self):
        super(RetentionEligibleMetricsDao, self).__init__(RetentionEligibleMetrics, order_by_ending=["id"])

    def _find_dup_with_session(self, session, retention_eligible_metrics_obj):
        query = (session.query(RetentionEligibleMetrics)
                 .filter(RetentionEligibleMetrics.participantId == retention_eligible_metrics_obj.participantId))

        record = query.first()
        if record:
            if (
                record.retentionEligibleStatus == retention_eligible_metrics_obj.retentionEligibleStatus
                and (record.retentionType == retention_eligible_metrics_obj.retentionType
                     or (record.retentionType is None
                         and retention_eligible_metrics_obj.retentionType == RetentionType.UNSET))
                and record.retentionEligibleTime == retention_eligible_metrics_obj.retentionEligibleTime
                and record.lastActiveRetentionActivityTime ==
                retention_eligible_metrics_obj.lastActiveRetentionActivityTime
            ):
                return record.id, False
            else:
                return record.id, True
        else:
            return None, None

    def _submit_rebuild_task(self, pids):
        """
        Rebuild Retention Eligible Metrics resource records
        :param pids: List of participant ids.
        """
        # Rebuild participant for BigQuery
        if GAE_PROJECT == 'localhost':
            res_gen = generators.RetentionEligibleMetricGenerator()
            for pid in pids:
                res = res_gen.make_resource(pid)
                res.save()
        else:
            task = GCPCloudTask()
            params = {'batch': pids}
            task.execute('batch_rebuild_retention_eligible_task', queue='resource-tasks', payload=params,
                         in_seconds=30)

    def upsert_all_with_session(self, session, retention_eligible_metrics_records):
        update_queue = list()
        for record in retention_eligible_metrics_records:
            dup_id, need_update = self._find_dup_with_session(session, record)
            if dup_id and not need_update:
                continue
            elif dup_id and need_update:
                record.id = dup_id
            session.merge(record)
            # Batch up records for resource rebuilding.
            update_queue.append(int(record.participantId))

        session.commit()
        if update_queue:
            self._submit_rebuild_task(update_queue)
        return len(update_queue)
