import api_util

from api.base_api import UpdatableApi
from api_util import PTC, PTC_AND_HEALTHPRO
from dao.participant_dao import ParticipantDao
from model.utils import from_client_participant_id

class ParticipantApi(UpdatableApi):
  def __init__(self):
    super(ParticipantApi, self).__init__(ParticipantDao())

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, id_=None):
    return super(ParticipantApi, self).get(from_client_participant_id(id_))

  @api_util.auth_required(PTC)
  def post(self):
    return super(ParticipantApi, self).post()

  @api_util.auth_required(PTC)
  def put(self, id_):
    return super(ParticipantApi, self).put(from_client_participant_id(id_))

  # TODO(DA-216): remove once PTC migrates to PUT
  @api_util.auth_required(PTC)
  def patch(self, id_):
    return super(ParticipantApi, self).patch(from_client_participant_id(id_))
