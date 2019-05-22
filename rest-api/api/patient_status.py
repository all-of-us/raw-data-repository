from api.base_api import BaseApi, UpdatableApi
from app_util import auth_required
from api_util import HEALTHPRO
from dao.patient_status_dao import PatientStatusDao


class PatientStatusApi(UpdatableApi):
  def __init__(self):
    super(PatientStatusApi, self).__init__(PatientStatusDao(), get_returns_children=True)

  @auth_required(HEALTHPRO)
  def post(self, p_id, org_id=None): # pylint: disable=unused-argument
    return super(PatientStatusApi, self).post(participant_id=p_id), 201

  @auth_required(HEALTHPRO)
  def get(self, p_id, org_id):  # pylint: disable=unused-argument
    return self.dao.get(p_id, org_id)

  @auth_required(HEALTHPRO)
  def put(self, p_id, org_id):  # pylint: disable=unused-argument
    return super(PatientStatusApi, self).put(org_id, participant_id=p_id, skip_etag=True)

  def _make_response(self, obj):
    result = super(UpdatableApi, self)._make_response(obj)
    return result, 200


class PatientStatusHistoryApi(BaseApi):
  def __init__(self):
    super(PatientStatusHistoryApi, self).__init__(PatientStatusDao(), get_returns_children=True)

  @auth_required(HEALTHPRO)
  def get(self, p_id, org_id):
    return self.dao.get_history(p_id, org_id)
