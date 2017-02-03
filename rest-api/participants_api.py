import api_util
import base_api
import participant_dao

from api_util import PTC, PTC_AND_HEALTHPRO


class ParticipantAPI(base_api.BaseApi):
  def __init__(self):
    super(ParticipantAPI, self).__init__(participant_dao.DAO())

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, id_=None, a_id=None):
    return super(ParticipantAPI, self).get(id_, a_id)

  @api_util.auth_required(PTC)
  def post(self, a_id=None):
    return super(ParticipantAPI, self).post(a_id)

  @api_util.auth_required(PTC)
  def put(self, id_, a_id=None):
    return super(ParticipantAPI, self).put(id_, a_id)

  @api_util.auth_required(PTC)
  def patch(self, id_, a_id=None):
    return super(ParticipantAPI, self).patch(id_, a_id)
