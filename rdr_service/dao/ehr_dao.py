from sqlalchemy import func, select

from rdr_service.dao.base_dao import BaseDao
from rdr_service.dao.calendar_dao import CalendarDao
from rdr_service.model.ehr import EhrReceipt


class EhrReceiptDao(BaseDao):
    def __init__(self):
        super(EhrReceiptDao, self).__init__(EhrReceipt)

    def get_id(self, obj):
        return obj.ehrReceiptId

    def get_by_organization_id(self, organization_id):
        with self.session() as session:
            return self.get_by_organization_id_with_session(session, organization_id)

    def get_by_organization_id_with_session(self, session, organization_id):
        return session.query(EhrReceipt).filter(EhrReceipt.organizationId == organization_id).all()

    def get_active_organization_counts_in_interval(self, start_date, end_date, interval, organization_ids=None):
        """
    Get number of receipts per organization received in specific time intervals

    :param start_date: query min date
    :param end_date: query max date
    :param interval: time interval (one of the INTERVAL constants)
    :param organization_ids: (optional) filter results to matching organizations
    :return: dictionary of organizationId:list(dict(date=X, count=Y), ...)
    :rtype: dict
    """
        interval_query = CalendarDao.get_interval_query(
            start=start_date, end=end_date, interval_key=interval, include_end_date=True
        )
        active_org_count_conditions = (EhrReceipt.receiptTime >= interval_query.c.start_date) & (
            EhrReceipt.receiptTime < interval_query.c.end_date
        )
        if organization_ids:
            active_org_count_conditions &= EhrReceipt.organizationId.in_(organization_ids)
        active_org_count_query = select([func.count(EhrReceipt.organizationId.distinct())]).where(
            active_org_count_conditions
        )
        query = select(
            [
                interval_query.c.start_date,
                interval_query.c.end_date,
                active_org_count_query.label("active_organization_count"),
            ]
        )
        with self.session() as session:
            cursor = session.execute(query)
        return [dict(list(zip(list(cursor.keys()), row))) for row in cursor]
