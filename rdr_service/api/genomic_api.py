from werkzeug.exceptions import NotFound, BadRequest

from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import GEM
from rdr_service.app_util import auth_required
from rdr_service.dao.genomics_dao import GemPiiDao


class GenomicGemPiiApi(BaseApi):
    def __init__(self):
        super(GenomicGemPiiApi, self).__init__(GemPiiDao())

    @auth_required(GEM)
    def get(self, p_id=None):
        if p_id is not None:
            pii = self.dao.get_by_pid(p_id)
            if not pii:
                raise NotFound(f"Participant with ID {p_id} not found")
            return self._make_response(pii)
        raise BadRequest
