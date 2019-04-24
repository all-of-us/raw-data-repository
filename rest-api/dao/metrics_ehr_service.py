import config
import sqlalchemy

from dao.base_dao import BaseDao
from dao.calendar_dao import INTERVAL_WEEK, CalendarDao
from dao.ehr_dao import EhrReceiptDao
from model.organization import Organization
from model.participant import Participant
from model.participant_summary import ParticipantSummary
from participant_enums import WithdrawalStatus, QuestionnaireStatus, EhrStatus


class MetricsEhrService(BaseDao):

  def __init__(self):
    super(MetricsEhrService, self).__init__(ParticipantSummary, backup=True)
    self.ehr_receipt_dao = EhrReceiptDao()

  def _get_organization_ids_from_hpo_ids(self, hpo_ids):
    query = (
      sqlalchemy.select([Organization.organizationId])
      .where(Organization.hpoId.in_(hpo_ids))
    )
    with self.session() as session:
      result = session.execute(query)
    return list(row[0] for row in result)

  def get_metrics(
    self,
    start_date,
    end_date,
    organization_ids=None,
    hpo_ids=None,
    interval=INTERVAL_WEEK
  ):
    if organization_ids is None and hpo_ids is not None:
      organization_ids = self._get_organization_ids_from_hpo_ids(hpo_ids)
    return {
      'metrics_over_time': self._get_metrics_over_time_data(
        start_date,
        end_date,
        interval,
        organization_ids
      ),
      'organization_metrics': self.get_organization_metrics_data(end_date, organization_ids),
    }

  def _get_metrics_over_time_data(self, start_date, end_date, interval, organization_ids=None):
    """
    combines `Active Organization Counts Over Time` and `EHR Consented vs EHR Received Over Time`
    """
    active_organization_metrics = self.get_organizations_active_over_time_data(
      start_date, end_date, interval, organization_ids)
    active_organization_metrics_by_date = {
      result['date']: result['metrics']
      for result in active_organization_metrics
    }
    participant_ehr_metrics = self.get_participant_ehr_metrics_over_time_data(
      start_date, end_date, interval, organization_ids)
    participant_ehr_metrics_by_date = {
      result['date']: result['metrics']
      for result in participant_ehr_metrics
    }
    return [
      {
        'date': date_key,
        'metrics': dict(
          active_organization_metrics_by_date[date_key],
          **participant_ehr_metrics_by_date[date_key]
        )
      }
      for date_key in active_organization_metrics_by_date.keys()
    ]

  def get_organizations_active_over_time_data(
    self,
    start_date,
    end_date,
    interval,
    organization_ids=None
  ):
    """
    Count of organizations that have uploaded EHR over time
    """
    active_organization_counts = self.ehr_receipt_dao.get_active_organization_counts_in_interval(
      start_date, end_date, interval, organization_ids)
    return [
      {
        'date': result['start_date'],
        'metrics': {
          'ORGANIZATIONS_ACTIVE': result['active_organization_count'],
        }
      }
      for result in active_organization_counts
    ]

  def get_participant_ehr_metrics_over_time_data(
    self,
    start_date,
    end_date,
    interval,
    organization_ids=None
  ):
    """
    EHR Consented vs EHR Received over time
    """
    interval_query = CalendarDao.get_interval_query(
      start=start_date,
      end=end_date,
      interval_key=interval
    )
    ehr_query = self._get_participant_ehr_metrics_over_time_query(interval_query, organization_ids)
    with self.session() as session:
      ehr_cursor = session.execute(ehr_query)
    return [
      {
        'date': row_dict['start_date'],
        'metrics': {
          'EHR_CONSENTED': row_dict['consented_count'],
          'EHR_RECEIVED': row_dict['received_count'],
        }
      }
      for row_dict
      in [dict(zip(ehr_cursor.keys(), row)) for row in ehr_cursor]
    ]

  @staticmethod
  def _get_participant_ehr_metrics_over_time_query(interval_query, organization_ids=None):
    common_subquery_where_arg = (
      (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN)
      & Participant.isGhostId.isnot(True)
    )
    if organization_ids:
      common_subquery_where_arg &= ParticipantSummary.organizationId.in_(organization_ids)

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

  def get_organization_metrics_data(self, end_date, organization_ids=None):
    """
    Get organization participant status metrics as of end_date
    """
    q = self._get_organization_metrics_query(end_date, organization_ids)
    with self.session() as session:
      cursor = session.execute(q)
    return {
      row_dict['organization_id']: row_dict
      for row_dict
      in [
        dict(zip(cursor.keys(), row))
        for row
        in cursor
      ]
    }

  @staticmethod
  def _get_organization_metrics_query(cutoff_date, organization_ids=None):
    def make_sum_bool_field(condition_expression):
      return sqlalchemy.func.cast(
        sqlalchemy.func.sum(
          sqlalchemy.func.if_(condition_expression, 1, 0)
        ),
        sqlalchemy.Integer
      )

    ppi_baseline_module_count = len(config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS))

    can_be_included = (
      (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN)
      & Participant.isGhostId.isnot(True)
    )

    # condition expression components
    was_signed_up = Participant.signUpTime <= cutoff_date
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
    had_ehr_receipt = (
      (ParticipantSummary.ehrStatus == EhrStatus.PRESENT)
      & (ParticipantSummary.ehrReceiptTime <= cutoff_date)
    )

    # condition expressions
    was_participant = can_be_included & was_signed_up
    was_primary = was_participant & had_consented_for_study
    was_ehr_consented = was_primary & had_consented_for_ehr
    was_core = (
      was_ehr_consented
      & had_completed_ppi
      & had_physical_measurements
      & had_biosample
    )
    had_ehr_data = was_primary & had_ehr_receipt

    # build query
    fields = [
      Organization.externalId.label('organization_id'),
      Organization.displayName.label('organization_name'),
      make_sum_bool_field(was_participant).label('total_participants'),
      make_sum_bool_field(was_primary).label('total_primary_consented'),
      make_sum_bool_field(was_ehr_consented).label('total_ehr_consented'),
      make_sum_bool_field(was_core).label('total_core_participants'),
      make_sum_bool_field(had_ehr_data).label('total_ehr_data_received'),
      sqlalchemy.func.date(sqlalchemy.func.max(ParticipantSummary.ehrUpdateTime))
        .label('last_ehr_submission_date'),
    ]
    orgs_with_participants_and_summaries = sqlalchemy.join(
      sqlalchemy.join(
        Organization,
        ParticipantSummary,
        ParticipantSummary.organizationId == Organization.organizationId
      ),
      Participant,
      Participant.participantId == ParticipantSummary.participantId
    )

    query = (
      sqlalchemy.select(fields)
        .select_from(orgs_with_participants_and_summaries)
        .group_by(Organization.organizationId)
    )
    if organization_ids:
      query = query.where(Organization.organizationId.in_(organization_ids))
    return query
