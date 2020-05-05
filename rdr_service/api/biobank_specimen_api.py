from flask import request

from rdr_service.api.base_api import UpdatableApi
from rdr_service.api_util import HEALTHPRO
from rdr_service.app_util import auth_required
from rdr_service.dao.biobank_specimen_dao import BiobankSpecimenDao
from werkzeug.exceptions import BadRequest


class BiobankSpecimenApi(UpdatableApi):
    def __init__(self):
        super().__init__(BiobankSpecimenDao(), get_returns_children=True)

    @auth_required(HEALTHPRO)
    def put(self, *args, **kwargs):  # pylint: disable=unused-argument
        resource = request.get_json(force=True)

        for required_field in ['rlimsID', 'orderID', 'testcode', 'participantID']:
            if required_field not in resource:
                raise BadRequest("Missing field: %s" % required_field)

        if self.dao.exists(resource):
            return super(BiobankSpecimenApi, self).put(kwargs['rlims_id'], skip_etag=True)
        else:
            return super(BiobankSpecimenApi, self).post()
