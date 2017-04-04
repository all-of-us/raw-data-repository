import clock
import config

from query import OrderBy
from werkzeug.exceptions import BadRequest, NotFound
from code_constants import PPI_SYSTEM, UNSET, BIOBANK_TESTS
from dao.base_dao import UpdatableDao
from dao.database_utils import get_sql_and_params_for_array
from dao.code_dao import CodeDao
from dao.hpo_dao import HPODao
from model.participant_summary import ParticipantSummary, WITHDRAWN_PARTICIPANT_FIELDS
from model.participant_summary import WITHDRAWN_PARTICIPANT_VISIBILITY_TIME
from participant_enums import QuestionnaireStatus, PhysicalMeasurementsStatus, SampleStatus
from participant_enums import EnrollmentStatus, WithdrawalStatus
from sqlalchemy import or_

# By default / secondarily order by last name, first name, DOB, and participant ID
_ORDER_BY_ENDING = ['lastName', 'firstName', 'dateOfBirth', 'participantId']
# The default ordering of results for queries for withdrawn participants.
_WITHDRAWN_ORDER_BY_ENDING = ['withdrawalTime', 'participantId']
_CODE_FIELDS = ['genderIdentity']

# Query used to update the enrollment status for all participant summaries after
# a Biobank samples import.
_ENROLLMENT_STATUS_SQL = """
    UPDATE
      participant_summary
    SET
      enrollment_status =
        CASE WHEN (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :submitted
                   AND num_completed_baseline_ppi_modules = :num_baseline_ppi_modules
                   AND physical_measurements_status = :completed
                   AND samples_to_isolate_dna = :received)
             THEN :full_participant
             WHEN (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :submitted)
             THEN :member
             ELSE :interested
        END
   """

_SAMPLE_SQL = """,
      sample_status_%(test)s =
        CASE WHEN EXISTS(SELECT * FROM biobank_stored_sample
                         WHERE biobank_stored_sample.biobank_id = participant_summary.biobank_id
                         AND biobank_stored_sample.test = %(sample_param_ref)s)
             THEN :received ELSE :unset END,
      sample_status_%(test)s_time =
        (SELECT MIN(confirmed) FROM biobank_stored_sample
          WHERE biobank_stored_sample.biobank_id = participant_summary.biobank_id
          AND biobank_stored_sample.test = %(sample_param_ref)s)
   """

def _get_sample_sql_and_params():
  """Gets SQL and params needed to update status and time fields on the participant summary for
  each biobank sample.
  """
  sql = ''
  params = {}
  for i in range(0, len(BIOBANK_TESTS)):
    sample_param = 'sample%d' % i
    sample_param_ref = ':%s' % sample_param
    lower_test = BIOBANK_TESTS[i].lower()
    sql += _SAMPLE_SQL % {"test": lower_test, "sample_param_ref": sample_param_ref}
    params[sample_param] = BIOBANK_TESTS[i]
  return sql, params

class ParticipantSummaryDao(UpdatableDao):
  def __init__(self):
    super(ParticipantSummaryDao, self).__init__(ParticipantSummary,
                                                order_by_ending=_ORDER_BY_ENDING)

  def get_id(self, obj):
    return obj.participantId

  def _validate_update(self, session, obj, existing_obj):
    """Participant summaries don't have a version value; drop it from validation logic."""
    if not existing_obj:
      raise NotFound('%s with id %s does not exist' % (self.model_type.__name__, id))

  def _has_withdrawn_filter(self, query):
    for field_filter in query.field_filters:
      if (field_filter.field_name == 'withdrawalStatus' and
          field_filter.value == WithdrawalStatus.NO_USE):
        return True
      if field_filter.field_name == 'withdrawalTime' and field_filter.value is not None:
        return True
    return False

  def _create_query(self, session, query_def):
    if self._has_withdrawn_filter(query_def):
      # When querying for withdrawn particiapnts, ensure that the only fields being filtered on or
      # ordered by are in WITHDRAWN_PARTICIPANT_FIELDS.
      for field_filter in query_def.field_filters:
        if not field_filter.field_name in WITHDRAWN_PARTICIPANT_FIELDS:
          raise BadRequest("Can't filter on %s for withdrawn participants" %
                           field_filter.field_name)
      if query_def.order_by:
        if not query_def.order_by.field_name in WITHDRAWN_PARTICIPANT_FIELDS:
          raise BadRequest("Can't order by %s for withdrawn participants" %
                           query_def.order_by.field_name)
      return super(ParticipantSummaryDao, self)._create_query(session, query_def)
    else:
      # When *not* querying for withdrawn participants, ensure that we only return participants
      # who have not withdrawn or withdrew in the past 48 hours.
      query = super(ParticipantSummaryDao, self)._create_query(session, query_def)
      withdrawn_visible_start = clock.CLOCK.now() - WITHDRAWN_PARTICIPANT_VISIBILITY_TIME
      return query.filter(or_(ParticipantSummary.withdrawalStatus != WithdrawalStatus.NO_USE,
                              ParticipantSummary.withdrawalTime >= withdrawn_visible_start))

  def _get_order_by_ending(self, query):
    if self._has_withdrawn_filter(query):
      return _WITHDRAWN_ORDER_BY_ENDING
    return self.order_by_ending

  def _add_order_by(self, query, order_by, field_names, fields):
    if order_by.field_name in _CODE_FIELDS:
      return super(ParticipantSummaryDao, self)._add_order_by(query,
                                                              OrderBy(order_by.field_name + 'Id',
                                                                      order_by.ascending),
                                                              field_names,
                                                              fields)
    return super(ParticipantSummaryDao, self)._add_order_by(query, order_by, field_names, fields)

  def make_query_filter(self, field_name, value):
    # Handle HPO and code values when parsing filter values.
    if field_name == 'hpoId':
      hpo = HPODao().get_by_name(value)
      if not hpo:
        raise BadRequest('No HPO found with name %s' % value)
      return super(ParticipantSummaryDao, self).make_query_filter(field_name, hpo.hpoId)
    if field_name in _CODE_FIELDS:
      if value == UNSET:
        return super(ParticipantSummaryDao, self).make_query_filter(field_name + 'Id', None)
      # Note: we do not at present support querying for UNMAPPED code values.
      code = CodeDao().get_code(PPI_SYSTEM, value)
      if not code:
        raise BadRequest('No code found: %s' % value)
      return super(ParticipantSummaryDao, self).make_query_filter(field_name + 'Id', code.codeId)
    return super(ParticipantSummaryDao, self).make_query_filter(field_name, value)

  def update_from_biobank_stored_samples(self):
    """Rewrites sample-related summary data. Call this after reloading BiobankStoredSamples."""
    baseline_tests_sql, baseline_tests_params = get_sql_and_params_for_array(
        config.getSettingList(config.BASELINE_SAMPLE_TEST_CODES), 'baseline')
    dna_tests_sql, dna_tests_params = get_sql_and_params_for_array(
        config.getSettingList(config.DNA_SAMPLE_TEST_CODES), 'dna')
    sample_sql, sample_params = _get_sample_sql_and_params()
    sql = """
    UPDATE
      participant_summary
    SET
      num_baseline_samples_arrived = (
        SELECT
          COUNT(*)
        FROM
          biobank_stored_sample
        WHERE
          biobank_stored_sample.biobank_id = participant_summary.biobank_id
          AND biobank_stored_sample.test IN %s
      ),
      samples_to_isolate_dna = (
          CASE WHEN EXISTS(SELECT * FROM biobank_stored_sample
                           WHERE biobank_stored_sample.biobank_id = participant_summary.biobank_id
                           AND biobank_stored_sample.test IN %s)
          THEN :received ELSE :unset END
      ) %s""" % (baseline_tests_sql, dna_tests_sql, sample_sql)
    params = {'received': int(SampleStatus.RECEIVED), 'unset': int(SampleStatus.UNSET)}
    params.update(baseline_tests_params)
    params.update(dna_tests_params)
    params.update(sample_params)
    enrollment_status_params = {'submitted': int(QuestionnaireStatus.SUBMITTED),
                                'num_baseline_ppi_modules': self._get_num_baseline_ppi_modules(),
                                'completed': int(PhysicalMeasurementsStatus.COMPLETED),
                                'received': int(SampleStatus.RECEIVED),
                                'full_participant': int(EnrollmentStatus.FULL_PARTICIPANT),
                                'member': int(EnrollmentStatus.MEMBER),
                                'interested': int(EnrollmentStatus.INTERESTED)}
    with self.session() as session:
      session.execute(sql, params)
      session.execute(_ENROLLMENT_STATUS_SQL, enrollment_status_params)

  def _get_num_baseline_ppi_modules(self):
    return len(config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS))

  def update_enrollment_status(self, summary):
    """Updates the enrollment status field on the provided participant summary to
    the correct value based on the other fields on it. Called after
    a questionnaire response or physical measurements are submitted."""
    consent = (summary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED and
               summary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED)
    enrollment_status = self.calculate_enrollment_status(consent,
                                                         summary.numCompletedBaselinePPIModules,
                                                         summary.physicalMeasurementsStatus,
                                                         summary.samplesToIsolateDNA)
    summary.enrollment_status = enrollment_status

  def calculate_enrollment_status(self, consent_for_study_enrollment_and_ehr,
                                  num_completed_baseline_ppi_modules,
                                  physical_measurements_status,
                                  samples_to_isolate_dna):
    if consent_for_study_enrollment_and_ehr:
      if (num_completed_baseline_ppi_modules == self._get_num_baseline_ppi_modules() and
          physical_measurements_status == PhysicalMeasurementsStatus.COMPLETED and
          samples_to_isolate_dna == SampleStatus.RECEIVED):
        return EnrollmentStatus.FULL_PARTICIPANT
      return EnrollmentStatus.MEMBER
    return EnrollmentStatus.INTERESTED
