from dao.base_dao import BaseDao
from dao.calendar_dao import CalendarDao
from model.ehr import EhrReceipt
from sqlalchemy import select, func


class EhrReceiptDao(BaseDao):

  def __init__(self):
    super(EhrReceiptDao, self).__init__(EhrReceipt)

  def get_id(self, obj):
    return obj.ehrReceiptId

  def get_active_site_counts_in_interval(self, start_date, end_date, interval, site_ids=None):
    """
    Get number of receipts per site received in specific time intervals

    :param start_date: query min date
    :param end_date: query max date
    :param interval: time interval (one of the INTERVAL constants)
    :param site_ids: (optional) filter results to matching sites
    :return: dictionary of siteId:list(dict(date=X, count=Y), ...)
    :rtype: dict
    """
    interval_query = CalendarDao.get_interval_query(
      start=start_date,
      end=end_date,
      interval_key=interval,
      include_end_date=True
    )
    active_site_count_conditions = (
      (EhrReceipt.recordedTime >= interval_query.c.start_date)
      & (EhrReceipt.recordedTime < interval_query.c.end_date)
    )
    if site_ids:
      active_site_count_conditions &= EhrReceipt.siteId.in_(site_ids)
    active_site_count_query = (
      select([func.count(EhrReceipt.siteId.distinct())])
        .where(active_site_count_conditions)
    )
    query = select([
      interval_query.c.start_date,
      interval_query.c.end_date,
      active_site_count_query.label('active_site_count'),
    ])
    #import sqlparse
    #print sqlparse.format(str(query), reindent=True)
    with self.session() as session:
      cursor = session.execute(query)
    return [
      dict(zip(cursor.keys(), row))
      for row
      in cursor
    ]
