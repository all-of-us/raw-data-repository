from api.base_api import BaseApi
from app_util import auth_required
from api_util import PTC_AND_HEALTHPRO
from dao.hpo_dao import HPODao

class AwardeeApi(BaseApi):
  def __init__(self):
    super(AwardeeApi, self).__init__(HPODao(), get_returns_children=True)

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id=None):
    if p_id:
      return super(AwardeeApi, self).get(p_id)
    else:
      return super(AwardeeApi, self)._query('id', None)