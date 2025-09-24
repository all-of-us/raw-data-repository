from flask import request

from werkzeug.exceptions import NotFound
from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RTI, RDR_AND_HEALTHPRO
from rdr_service.app_util import auth_required
from rdr_service.dao.study_nph_dao import NphBiospecimenDao


class NphBiospecimenAPI(BaseApi):
    def __init__(self):
        super().__init__(NphBiospecimenDao())

    @auth_required(RDR_AND_HEALTHPRO + [RTI])
    def get(self, nph_participant_id=None):
        log_api_request(log=request.log_record)

        # Lookup single participant from BigQuery Snapshot
        if nph_participant_id:
            rows = self.dao.get_by_participant(int(nph_participant_id))
            if not rows:
                raise NotFound(f'NPH participant {nph_participant_id} was not found')
            return self.dao.to_client_json(rows)

        # list all (paginated)
        count = int(request.args.get("count", 100))
        token = request.args.get("token")
        result = self.dao.get_all(count=count, token=token)
        return {
            "items": result.items,
            "next_token": result.token,
            "more_available": result.more_available,
            "total": result.total,
        }

    @classmethod
    def _make_resource_url(cls, response_json, id_field, participant_id):
        from rdr_service import main
        return main.api.url_for(cls, nph_participant_id=response_json[0][id_field], _external=True)

