from werkzeug.exceptions import NotFound, BadRequest

from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import GEM
from rdr_service.app_util import auth_required
from rdr_service.dao.genomics_dao import GenomicPiiDao


class GenomicPiiApi(BaseApi):
    def __init__(self):
        super(GenomicPiiApi, self).__init__(GenomicPiiDao())

    @auth_required(GEM)
    def get(self, mode=None, p_id=None):
        if mode not in ('GEM', 'RHP'):
            raise BadRequest("GenomicPII Mode required to be \"GEM\" or \"RHP\".")

        if p_id is not None:
            pii = self.dao.get_by_pid(p_id)

            if not pii:
                raise NotFound(f"Participant with ID {p_id} not found")

            proto_payload = {
                'mode': mode,
                'data': pii
            }

            return self._make_response(proto_payload)

        raise BadRequest
