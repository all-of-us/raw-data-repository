from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics


class RetentionEligibleMetricsDao(UpdatableDao):

    def __init__(self):
        super(RetentionEligibleMetricsDao, self).__init__(RetentionEligibleMetrics, order_by_ending=["id"])

    def _find_dup_with_session(self, session, retention_eligible_metrics_obj):
        query = (session.query(RetentionEligibleMetrics)
                 .filter(RetentionEligibleMetrics.participantId == retention_eligible_metrics_obj.participantId))

        record = query.first()
        if record:
            if record.retentionEligibleStatus == retention_eligible_metrics_obj.retentionEligibleStatus \
                and record.retentionType == retention_eligible_metrics_obj.retentionType \
                and record.retentionEligibleTime == retention_eligible_metrics_obj.retentionEligibleTime:
                return record.id, False
            else:
                return record.id, True
        else:
            return None, None

    def upsert_all_with_session(self, session, retention_eligible_metrics_records):
        upsert_count = 0
        for record in retention_eligible_metrics_records:
            dup_id, need_update = self._find_dup_with_session(session, record)
            if dup_id and not need_update:
                continue
            elif dup_id and need_update:
                record.id = dup_id
            session.merge(record)
            upsert_count = upsert_count + 1
        return upsert_count
