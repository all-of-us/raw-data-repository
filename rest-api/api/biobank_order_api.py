from api.base_api import BaseApi
from api_util import auth_required, HEALTHPRO, PTC_AND_HEALTHPRO
from dao.biobank_order_dao import BiobankOrderDao


class BiobankOrderApi(BaseApi):
  def __init__(self):
    super(BiobankOrderApi, self).__init__(BiobankOrderDao(), get_with_children=True)

  @auth_required(HEALTHPRO)
  def post(self, p_id):
    return super(BiobankOrderApi, self).post(participant_id=p_id)

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id, bo_id=None):  # pylint: disable=unused-argument
    return super(BiobankOrderApi, self).get(bo_id)
