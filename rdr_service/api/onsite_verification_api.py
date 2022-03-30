from werkzeug.exceptions import BadRequest

from rdr_service import app_util
from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import RDR_AND_HEALTHPRO
from rdr_service.dao.onsite_verification_dao import OnsiteVerificationDao


class OnsiteVerificationApi(BaseApi):
    def __init__(self):
        super().__init__(OnsiteVerificationDao())

    @app_util.auth_required(RDR_AND_HEALTHPRO)
    def post(self):
        return super(OnsiteVerificationApi, self).post()

    @app_util.auth_required(RDR_AND_HEALTHPRO)
    def get(self, p_id=None):
        if p_id is None:
            raise BadRequest("Request must include participant id")
        return super(OnsiteVerificationApi, self).get(participant_id=p_id)

    def list(self, participant_id=None):
        return OnsiteVerificationDao().get_verification_history(participant_id)
