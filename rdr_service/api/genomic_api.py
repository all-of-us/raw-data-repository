from flask import request
from werkzeug.exceptions import NotFound

from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import PTC_AND_HEALTHPRO
from rdr_service.app_util import auth_required
from rdr_service.dao.genomics_dao import GemPiiDao


class GenomicGemPiiApi(BaseApi):
    def __init__(self):
        super(GenomicGemPiiApi, self).__init__(GemPiiDao())

    @auth_required(PTC_AND_HEALTHPRO) # TODO: add GEM Color to Config
    def get(self, p_id=None):
        if p_id:
            pii = self.dao.get_by_pid(p_id)
            # if not participant:
            #     raise NotFound(f"Awardee with ID {a_id} not found")
            return self._make_response(pii)
        return False # super(GenomicGemPiiApi, self)._query(id_field="id")
