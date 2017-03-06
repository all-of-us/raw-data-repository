from code_constants import PPI_SYSTEM
from dao.base_dao import UpdatableDao
from dao.code_dao import CodeDao
from dao.hpo_dao import HPODao
from model.participant_summary import ParticipantSummary
from query import OrderBy
from werkzeug.exceptions import BadRequest


# By default / secondarily order by last name, first name, DOB, and participant ID
_ORDER_BY_ENDING = ['lastName','firstName', 'dateOfBirth', 'participantId']
_CODE_FIELDS = ['genderIdentityId', 'ethnicityId', 'raceId']

class ParticipantSummaryDao(UpdatableDao):
  def __init__(self):
    super(ParticipantSummaryDao, self).__init__(ParticipantSummary,
                                                order_by_ending=_ORDER_BY_ENDING)

  def get_id(self, obj):
    return obj.participantId

  def make_query_filter(self, field_name, value):
    # Handle HPO and code values when parsing filter values.
    if field_name == 'hpoId':
      hpo = HPODao().get_by_name(value)
      if not hpo:
        raise BadRequest("No HPO found with name %s" % value)
      return super(ParticipantSummaryDao, self).make_query_filter(field_name, hpo.hpoId)
    if field_name in _CODE_FIELDS:
      code = CodeDAO().get_code(PPI_SYSTEM, value)
      if not code:
        raise BadRequest("No code found: %s" % value)
      return super(ParticipantSummaryDao, self).make_query_filter(field_name, code.codeId)
    return super(ParticipantSummaryDao, self).make_query_filter(field_name, value)
