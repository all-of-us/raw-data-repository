from flask import request
from werkzeug.exceptions import NotFound

from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import PTC_AND_HEALTHPRO
from rdr_service.app_util import auth_required
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.model.site_enums import ObsoleteStatus


class AwardeeApi(BaseApi):
    def __init__(self):
        super(AwardeeApi, self).__init__(HPODao(), get_returns_children=True)

    @auth_required(PTC_AND_HEALTHPRO)
    def get(self, a_id=None):
        self.dao.obsolete_filters = self._get_obsolete_filters()
        if a_id:
            hpo = self.dao.get_by_name(a_id)
            if not hpo:
                raise NotFound(f"Awardee with ID {a_id} not found")
            return self._make_response(self.dao.get_with_children(hpo.hpoId))
        return super(AwardeeApi, self)._query(id_field="id")

    @classmethod
    def _make_resource_url(cls, json, id_field, participant_id):  # pylint: disable=unused-argument
        from rdr_service import main

        return main.api.url_for(cls, a_id=json[id_field], _external=True)

    def _make_response(self, obj):
        inactive = request.args.get("_inactive")
        return self.dao.to_client_json(obj,
                                       inactive_sites=inactive,
                                       obsolete_filters=self._get_obsolete_filters())

    def _get_obsolete_filters(self):
        obsolete_param = request.args.get("_obsolete")
        obsolete_filters = [None,
                            ObsoleteStatus.ACTIVE,
                            ObsoleteStatus.OBSOLETE]
        if obsolete_param is not None and obsolete_param.lower() == 'false':
            obsolete_filters = obsolete_filters[:-1]
        return obsolete_filters
