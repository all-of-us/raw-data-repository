from query import OrderBy
from werkzeug.exceptions import BadRequest, NotFound

import config
from code_constants import PPI_SYSTEM, UNSET
from dao.base_dao import UpdatableDao
from dao.database_utils import get_sql_and_params_for_array
from dao.code_dao import CodeDao
from dao.hpo_dao import HPODao
from model.participant_summary import ParticipantSummary

# By default / secondarily order by last name, first name, DOB, and participant ID
_ORDER_BY_ENDING = ['lastName', 'firstName', 'dateOfBirth', 'participantId']
_CODE_FIELDS = ['genderIdentity']


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
        raise BadRequest("No HPO found with name %s" % value)
      return super(ParticipantSummaryDao, self).make_query_filter(field_name, hpo.hpoId)
    if field_name in _CODE_FIELDS:
      if value == UNSET:
        return super(ParticipantSummaryDao, self).make_query_filter(field_name + 'Id', None)
      code = CodeDao().get_code(PPI_SYSTEM, value)
      if not code:
        raise BadRequest("No code found: %s" % value)
      return super(ParticipantSummaryDao, self).make_query_filter(field_name + 'Id', code.codeId)
    return super(ParticipantSummaryDao, self).make_query_filter(field_name, value)

  def update_from_biobank_stored_samples(self):
    """Rewrites sample-related summary data. Call this after reloading BiobankStoredSamples."""
    baseline_tests_sql, baseline_tests_params = get_sql_and_params_for_array(
        config.getSettingList(config.BASELINE_SAMPLE_TEST_CODES), 'baseline')
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
      )""" % baseline_tests_sql

    with self.session() as session:
      session.execute(sql, baseline_tests_params)
