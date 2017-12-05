from api.base_api import BaseApi
from app_util import auth_required
from api_util import PTC_AND_HEALTHPRO
from dao.hpo_dao import HPODao

class AwardeeApi(BaseApi):
  def __init__(self):
    super(AwardeeApi, self).__init__(HPODao(), get_returns_children=True)

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, awardee_id=None):
    if awardee_id:
      return super(AwardeeApi, self).get(awardee_id)
    else:
      return super(AwardeeApi, self)._query('name', None)