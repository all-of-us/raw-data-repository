from flask import request
from werkzeug.exceptions import NotFound

from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import PTC_AND_HEALTHPRO
from rdr_service.app_util import auth_required
from rdr_service.dao.hpo_dao import HPODao


class AwardeeApi(BaseApi):
    def __init__(self):
        super(AwardeeApi, self).__init__(HPODao(), get_returns_children=True)

    @auth_required(PTC_AND_HEALTHPRO)
    def get(self, a_id=None):
        if a_id:
            hpo = self.dao.get_by_name(a_id)
            if not hpo:
                raise NotFound(f"Awardee with ID {a_id} not found")
            return self._make_response(self.dao.get_with_children(hpo.hpoId))
        return super(AwardeeApi, self)._query(id_field="id")

    def _make_resource_url(self, json, id_field, participant_id):  # pylint: disable=unused-argument
        from rdr_service import main

        return main.api.url_for(self.__class__, a_id=json[id_field], _external=True)

    def _make_response(self, obj):
        inactive = request.args.get("_inactive")
        obsolete = request.args.get("_obsolete")
        return self.dao.to_client_json(obj,
                                       inactive_sites=inactive,
                                       include_obsolete=obsolete)
