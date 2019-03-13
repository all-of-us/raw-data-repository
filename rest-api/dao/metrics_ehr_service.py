import config
import sqlalchemy

from dao.base_dao import BaseDao
from dao.calendar_dao import INTERVAL_WEEK, CalendarDao
from dao.ehr_dao import EhrReceiptDao
from model.participant import Participant
from model.participant_summary import ParticipantSummary
from model.site import Site
from participant_enums import WithdrawalStatus, QuestionnaireStatus, EhrStatus


class MetricsEhrService(BaseDao):

  def __init__(self):
    super(MetricsEhrService, self).__init__(ParticipantSummary, backup=True)
    self.ehr_receipt_dao = EhrReceiptDao()

  def get_metrics(
    self,
    start_date,
    end_date,
    site_ids=None,
    interval=INTERVAL_WEEK
  ):
    return {
      'metrics_over_time': self._get_metrics_over_time_data(
        start_date,
        end_date,
        interval,
        site_ids
      ),
      'site_metrics': self._get_site_metrics_data(end_date, site_ids),
    }

  def _get_metrics_over_time_data(self, start_date, end_date, interval, site_ids=None):
    active_site_counts = self.ehr_receipt_dao.get_active_site_counts_in_interval(
      start_date, end_date, interval, site_ids)
    active_site_counts_by_interval_start = {
      result['start_date']: result['active_site_count']
      for result in active_site_counts
    }
    interval_query = CalendarDao.get_interval_query(
      start=start_date,
      end=end_date,
      interval_key=interval
    )
    ehr_query = self._get_metrics_over_time_query(interval_query, site_ids)
    #import sqlparse
    #print sqlparse.format(str(q), reindent=True)
    with self.session() as session:
      ehr_cursor = session.execute(ehr_query)
    return [
      {
        'date': row_dict['start_date'],
        'metrics': {
          'EHR_CONSENTED': row_dict['consented_count'],
          'EHR_RECEIVED': row_dict['received_count'],
          'SITES_ACTIVE': active_site_counts_by_interval_start[row_dict['start_date']],
        }
      }
      for row_dict
      in [dict(zip(ehr_cursor.keys(), row)) for row in ehr_cursor]
    ]

  @staticmethod
  def _get_metrics_over_time_query(interval_query, site_ids=None):
    common_subquery_where_arg = (
      (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN)
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
        subquery_consented_count.label('consented_count'),
        subquery_received_count.label('received_count'),
      ])
      .order_by(interval_query.c.start_date)
    )

  def _get_site_metrics_data(self, end_date, site_ids=None):
    q = self._get_site_metrics_query(end_date, site_ids)
    with self.session() as session:
      cursor = session.execute(q)
    return {
      row_dict['site_id']: row_dict
      for row_dict
      in [
        dict(zip(cursor.keys(), row))
        for row
        in cursor
      ]
    }

  @staticmethod
  def _get_site_metrics_query(cutoff_date, site_ids=None):
    def make_sum_bool_field(condition_expression):
      return sqlalchemy.func.cast(
        sqlalchemy.func.sum(
          sqlalchemy.func.if_(condition_expression, 1, 0)
        ),
        sqlalchemy.Integer
      )

    ppi_baseline_module_count = len(config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS))

    # condition expression components
    had_consented_for_study = (
      (ParticipantSummary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED)
      & (ParticipantSummary.consentForStudyEnrollmentTime <= cutoff_date)
    )
    had_consented_for_ehr = (
      (ParticipantSummary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED)
      & (ParticipantSummary.consentForElectronicHealthRecordsTime <= cutoff_date)
    )
    had_completed_ppi = (
      ParticipantSummary.numCompletedBaselinePPIModules >= ppi_baseline_module_count
    )
    had_physical_measurements = ParticipantSummary.physicalMeasurementsFinalizedTime <= cutoff_date
    had_biosample = ParticipantSummary.biospecimenOrderTime <= cutoff_date
    had_ehr = (
      (ParticipantSummary.ehrStatus == EhrStatus.PRESENT)
      & (ParticipantSummary.ehrReceiptTime <= cutoff_date)
    )

    # condition expressions
    was_participant = Participant.signUpTime <= cutoff_date
    was_primary = was_participant & had_consented_for_study
    was_ehr_consented = was_primary & had_consented_for_ehr
    was_core = (
      was_ehr_consented
      & had_completed_ppi
      & had_physical_measurements
      & had_biosample
    )

    # build query
    fields = [
      Site.siteId.label('site_id'),
      Site.siteName.label('site_name'),
      make_sum_bool_field(was_participant).label('total_participants'),
      make_sum_bool_field(was_primary).label('total_primary_consented'),
      make_sum_bool_field(was_ehr_consented).label('total_ehr_consented'),
      make_sum_bool_field(was_core).label('total_core_participants'),

      # NOTE: The following fields should be restructured if/when we log individual ehr receipts.
      #       In their current form, a participant site change will alter the output.
      make_sum_bool_field(was_core & had_ehr)
        .label('total_ehr_data_received'),
      sqlalchemy.func.date(sqlalchemy.func.max(ParticipantSummary.ehrUpdateTime))
        .label('last_ehr_submission_date'),
    ]
    sites_with_participants_and_summaries = sqlalchemy.outerjoin(
      sqlalchemy.outerjoin(
        Site,
        ParticipantSummary,
        ParticipantSummary.siteId == Site.siteId
      ),
      Participant,
      Participant.participantId == ParticipantSummary.participantId
    )
    query = (
      sqlalchemy.select(fields)
        .select_from(sites_with_participants_and_summaries)
        .group_by(Site.siteId)
    )
    if site_ids:
      query = query.where(Site.siteId.in_(site_ids))
    #import sqlparse
    #print sqlparse.format(str(query), reindent=True)
    return query
