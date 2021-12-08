from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.ce_health_reconciliation import CeHealthReconciliation


class CeHealthReconciliationDao(UpdatableDao):

    def __init__(self):
        super(CeHealthReconciliationDao, self).__init__(CeHealthReconciliation, order_by_ending=["id"])

    def _find_dup_with_session(self, session, ce_health_reconciliation_obj):
        query = (session.query(CeHealthReconciliation)
                 .filter(CeHealthReconciliation.reportFilePath == ce_health_reconciliation_obj.reportFilePath))

        record = query.first()
        if record:
            return record.id

    def upsert_all_with_session(self, session, ce_health_reconciliation_list):
        records = list(ce_health_reconciliation_list)
        for record in records:
            dup_id = self._find_dup_with_session(session, record)
            if dup_id:
                record.id = dup_id
            session.merge(record)

    def get_missing_records_by_report_date(self, session, cutoff_date):
        query = (session.query(CeHealthReconciliation)
                 .filter(CeHealthReconciliation.reportDate >= cutoff_date,
                         CeHealthReconciliation.status.is_(False)))
        return query.all()
