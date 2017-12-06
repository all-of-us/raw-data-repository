from api.base_api import BaseApi
from app_util import auth_required
from api_util import PTC_AND_HEALTHPRO
from dao.hpo_dao import HPODao
from werkzeug.exceptions import NotFound

class AwardeeApi(BaseApi):
  def __init__(self):
    super(AwardeeApi, self).__init__(HPODao(), get_returns_children=True)

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, p_id=None):
    if p_id:
       hpo = self.dao.get_by_name(p_id)
       if not hpo:
         raise NotFound("Awardee with ID %s not found" % p_id)
       return self._make_response(self.dao.get_with_children(hpo.hpoId))
    else:
      return super(AwardeeApi, self)._query('id', None)