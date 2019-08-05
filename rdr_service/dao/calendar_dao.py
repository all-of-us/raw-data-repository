from sqlalchemy import func, select, text

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.calendar import Calendar

INTERVAL_DAY = 'day'
INTERVAL_WEEK = 'week'
INTERVAL_MONTH = 'month'
INTERVAL_QUARTER = 'quarter'
INTERVALS = [
  INTERVAL_DAY,
  INTERVAL_WEEK,
  INTERVAL_MONTH,
  INTERVAL_QUARTER,
]


class CalendarDao(BaseDao):

  def __init__(self):
    super(CalendarDao, self).__init__(Calendar)

  def get_id(self, calendar):
    return calendar.day

  @staticmethod
  def get_interval_query(start, end, interval_key, include_end_date=False):
    """Get a query for the start & end dates of all of the periods of the given interval type.

    :param start: Begin with the interval which contains this date or datetime
    :param end: End with the interval which contains this date or datetime
    :param interval_key: one of the INTERVAL constants (from ehr_dao.py)
    :param include_end_date: should the query include `end_date` as a field?
                             defaults to False
    :return: a `sqlalchemy.select` query
    """
    if interval_key == INTERVAL_DAY:
      start_field = Calendar.day
      end_interval_offset = text('interval 1 day')
    elif interval_key == INTERVAL_WEEK:
      start_field = func.str_to_date(
        func.concat(func.yearweek(Calendar.day), 'Sunday'),
        '%X%V%W'
      )
      end_interval_offset = text('interval 1 week')
    elif interval_key == INTERVAL_MONTH:
      start_field = func.str_to_date(
        func.date_format(Calendar.day, "%Y%m01"),
        "%Y%m%d"
      )
      end_interval_offset = text('interval 1 month')
    elif interval_key == INTERVAL_QUARTER:
      start_field = func.date(func.concat(
        func.year(Calendar.day),
        '-', func.lpad((func.quarter(Calendar.day) - 1) * 3 + 1, 2, '0'),
        '-01'
      ))
      end_interval_offset = text('interval 1 quarter')
    else:
      raise NotImplemented("invalid interval: {interval}".format(interval=interval_key))
    start_date_query = (
      select([
        start_field.label('start_date')
      ])
        .where((Calendar.day >= start) & (Calendar.day <= end))
        .group_by(start_field)
        .alias('start_date_query')
    )
    end_date_field = (
      func.date_add(start_date_query.c.start_date, end_interval_offset).label('end_date')
    )
    fields = [start_date_query.c.start_date.label('start_date')]
    if include_end_date:
      fields.append(end_date_field)
    return select(fields).alias('interval_query')
