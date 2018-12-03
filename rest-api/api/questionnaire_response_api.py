import app_util

from api.base_api import BaseApi
from api_util import PTC, PTC_AND_HEALTHPRO
from dao.questionnaire_response_dao import QuestionnaireResponseDao

class QuestionnaireResponseApi(BaseApi):
  def __init__(self):
    super(QuestionnaireResponseApi, self).__init__(QuestionnaireResponseDao())

  @app_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id, id_):
    #pylint: disable=unused-argument
    return super(QuestionnaireResponseApi, self).get(id_)

  @app_util.auth_required(PTC)
  def post(self, p_id):
    return super(QuestionnaireResponseApi, self).post(participant_id=p_id)
