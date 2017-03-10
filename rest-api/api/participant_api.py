import api_util

from api.base_api import UpdatableApi
from api_util import PTC, PTC_AND_HEALTHPRO
from dao.participant_dao import ParticipantDao

class ParticipantApi(UpdatableApi):
  def __init__(self):
    super(ParticipantApi, self).__init__(ParticipantDao())

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id):
    return super(ParticipantApi, self).get(p_id)

  @api_util.auth_required(PTC)
  def post(self):
    result = super(ParticipantApi, self).post()
    return result

  @api_util.auth_required(PTC)
  def put(self, p_id):
    return super(ParticipantApi, self).put(p_id)

  # TODO(DA-216): remove once PTC migrates to PUT
  @api_util.auth_required(PTC)
  def patch(self, p_id):
    return super(ParticipantApi, self).put(p_id)
