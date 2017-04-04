import api_util

from api.base_api import UpdatableApi
from api_util import PTC
from code_constants import PPI_SYSTEM
from dao.code_dao import CodeDao
from dao.questionnaire_dao import QuestionnaireDao
from flask import request
from werkzeug.exceptions import BadRequest, NotFound

class QuestionnaireApi(UpdatableApi):
  def __init__(self):
    super(QuestionnaireApi, self).__init__(QuestionnaireDao())

  @api_util.auth_required(PTC)
  def get(self, id_=None):
    if id_:
      return super(QuestionnaireApi, self).get(id_)
    else:
      concept = request.args.get('concept')
      if not concept:
        raise BadRequest('Either questionnaire ID or concept must be specified in request.')
      concept_code = CodeDao().get_code(PPI_SYSTEM, concept)
      if not concept_code:
        raise BadRequest('Code not found: %s' % concept)
      questionnaire = self.dao.get_latest_questionnaire_with_concept(concept_code.codeId)
      if not questionnaire:
        raise NotFound('Could not find questionnaire with concept: %s' % concept)
      return self._make_response(questionnaire)

  @api_util.auth_required(PTC)
  def post(self):
    return super(QuestionnaireApi, self).post()

  @api_util.auth_required(PTC)
  def put(self, id_):
    return super(QuestionnaireApi, self).put(id_)

