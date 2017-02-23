import api_util

from api.base_api import BaseApi
from api_util import PTC
from dao.questionnaire_dao import QuestionnaireDao

class QuestionnaireApi(BaseApi):
  def __init__(self):
    super(QuestionnaireApi, self).__init__(QuestionnaireDao())

  @api_util.auth_required(PTC)
  def get(self, id_=None):
    return super(QuestionnaireApi, self).get(id_)

  @api_util.auth_required(PTC)
  def post(self):
    return super(QuestionnaireApi, self).post()

  @api_util.auth_required(PTC)
  def put(self, id_):
    return super(QuestionnaireApi, self).patch(id_)

