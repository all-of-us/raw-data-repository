from api.base_api import BaseApi
from app_util import auth_required
from api_util import PTC_AND_HEALTHPRO
from dao.hpo_dao import HPODao
from werkzeug.exceptions import NotFound
from flask import request

class AwardeeApi(BaseApi):
  def __init__(self):
    super(AwardeeApi, self).__init__(HPODao(), get_returns_children=True)

  @auth_required(PTC_AND_HEALTHPRO)
  def get(self, a_id=None):
    if a_id:
      hpo = self.dao.get_by_name(a_id)
      if not hpo:
        raise NotFound("Awardee with ID %s not found" % a_id)
      return self._make_response(self.dao.get_with_children(hpo.hpoId))
    return super(AwardeeApi, self)._query(id_field='id')

  def _make_resource_url(self, json, id_field, participant_id):
    import main
    return main.api.url_for(self.__class__, a_id=json[id_field],
                            _external=True)

  def _make_response(self, obj):
    inactive = request.args.get('_inactive')
    return self.dao.to_client_json(obj, inactive)
