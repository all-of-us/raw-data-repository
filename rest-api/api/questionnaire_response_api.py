import api_util

from api.base_api import BaseApi
from api_util import PTC
from dao.questionnaire_response_dao import QuestionnaireResponseDao
from model.utils import from_client_participant_id

class QuestionnaireResponseApi(BaseApi):
  def __init__(self):
    super(QuestionnaireResponseApi, self).__init__(QuestionnaireResponseDao())

  @api_util.auth_required(PTC)
  def get(self, id_=None):
    return super(QuestionnaireResponseApi, self).get(id_)

  @api_util.auth_required(PTC)
  def post(self, a_id):
    participant_id = from_client_participant_id(a_id)
    return super(QuestionnaireResponseApi, self).post(participant_id=participant_id)