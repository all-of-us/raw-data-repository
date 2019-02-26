import sqlalchemy

from dao.base_dao import BaseDao
from model.calendar import Calendar
from model.participant import Participant
from model.participant_summary import ParticipantSummary
from participant_enums import TEST_EMAIL_PATTERN, WithdrawalStatus, TEST_HPO_ID


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


class MetricsEhrService(BaseDao):

  def __init__(self):
    super(MetricsEhrService, self).__init__(ParticipantSummary, backup=True)

  def get_metrics_over_time(
    self,
    start_date,
    end_date,
    site_ids=None,
    interval=INTERVAL_WEEK
  ):
    q = self._get_query(start_date, end_date, site_ids, self._get_interval_query(interval))
    with self.session() as session:
      cursor = session.execute(q)
    return [
      {
        'date': start_date.isoformat(),
        'until': end_date.isoformat(),
        'metrics': {
          'EHR_CONSENTED': consented_count,
          'EHR_RECEIVED': received_count,
        }
      }
      for start_date, end_date, consented_count, received_count
      in cursor
    ]

  @staticmethod
  def _get_interval_query(key):
    if key == INTERVAL_DAY:
      start_field = Calendar.day
      end_interval_offset = sqlalchemy.text('interval 1 day')
    elif key == INTERVAL_WEEK:
      start_field = sqlalchemy.func.str_to_date(
        sqlalchemy.func.concat(sqlalchemy.func.yearweek(Calendar.day), 'Sunday'),
        '%X%V%W'
      )
      end_interval_offset = sqlalchemy.text('interval 1 week')
    elif key == INTERVAL_MONTH:
      start_field = sqlalchemy.func.str_to_date(
        sqlalchemy.func.date_format(Calendar.day, "%Y%m01"),
        "%Y%m%d"
      )
      end_interval_offset = sqlalchemy.text('interval 1 month')
    elif key == INTERVAL_QUARTER:
      start_field = sqlalchemy.func.date(sqlalchemy.func.concat(
        sqlalchemy.func.year(Calendar.day),
        '-', sqlalchemy.func.lpad((sqlalchemy.func.quarter(Calendar.day) - 1) * 3 + 1, 2, '0'),
        '-01'
      ))
      end_interval_offset = sqlalchemy.text('interval 1 quarter')
    else:
      raise NotImplemented("invalid interval: {interval}".format(interval=key))
    start_date_query = (
      sqlalchemy.select([
        start_field.label('start_date')
      ])
        .group_by(start_field)
        .alias('start_date_query')
    )
    return (
      sqlalchemy.select([
        start_date_query.c.start_date
          .label('start_date'),
        sqlalchemy.func.date_add(start_date_query.c.start_date, end_interval_offset)
          .label('end_date'),
      ])
        .alias('interval_query')
    )

  @staticmethod
  def _get_query(
    start_date,
    end_date,
    site_ids,
    interval_query
  ):
    common_subquery_where_arg = (
      (ParticipantSummary.hpoId != TEST_HPO_ID)
      & (
        ParticipantSummary.email.isnot(None)
        | sqlalchemy.not_(ParticipantSummary.email.like(TEST_EMAIL_PATTERN))
      )
      & (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN)
      & Participant.isGhostId.isnot(True)
    )
    if site_ids:
      common_subquery_where_arg &= ParticipantSummary.siteId.in_(site_ids)

    base_subquery = (
      sqlalchemy.select([sqlalchemy.func.count()])
        .select_from(sqlalchemy.join(
          Participant, ParticipantSummary,
          Participant.participantId == ParticipantSummary.participantId
        ))
        .where(common_subquery_where_arg)
    )
    subquery_consented_count = base_subquery.where(
      ParticipantSummary.consentForElectronicHealthRecordsTime <= interval_query.c.start_date
    )
    subquery_received_count = (
      base_subquery
        .where(
          ParticipantSummary.ehrReceiptTime.isnot(None)
          & (ParticipantSummary.ehrReceiptTime <= interval_query.c.start_date)
        )
    )

    return (
      sqlalchemy.select([
        interval_query.c.start_date,
        interval_query.c.end_date,
        subquery_consented_count.label('consented_count'),
        subquery_received_count.label('received_count'),
      ])
      .where(
        (interval_query.c.start_date >= start_date)
        & (interval_query.c.start_date <= end_date)
      )
      .order_by(interval_query.c.start_date)
    )
