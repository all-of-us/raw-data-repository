from api.base_api import BaseApi
from api_util import auth_required, HEALTHPRO, PTC_AND_HEALTHPRO
from dao.biobank_order_dao import BiobankOrderDao
from dao.participant_dao import ParticipantDao


class BiobankOrderApi(BaseApi):
  def __init__(self):
    super(BiobankOrderApi, self).__init__(BiobankOrderDao(), get_returns_children=True)

  @auth_required(HEALTHPRO)
  def post(self, p_id):
    order = super(BiobankOrderApi, self).post(participant_id=p_id)
    ParticipantDao().add_missing_hpo_from_site(p_id, order.finalizedSiteId)
    return order

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id, bo_id=None):  # pylint: disable=unused-argument
    return super(BiobankOrderApi, self).get(bo_id)
