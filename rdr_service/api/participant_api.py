from rdr_service import app_util
from werkzeug.exceptions import NotFound

from rdr_service.api.base_api import UpdatableApi
from rdr_service.api_util import PTC, PTC_AND_HEALTHPRO
from rdr_service.dao.base_dao import _MIN_ID, _MAX_ID
from rdr_service.dao.participant_dao import ParticipantDao


class ParticipantApi(UpdatableApi):
    def __init__(self):
        super(ParticipantApi, self).__init__(ParticipantDao())

    @app_util.auth_required(PTC_AND_HEALTHPRO)
    def get(self, p_id):
        # Make sure participant id is in the correct range of possible values.
        if not _MIN_ID <= p_id <= _MAX_ID:
            raise NotFound(f"Participant with ID {p_id} is not found.")
        return super().get(p_id)

    @app_util.auth_required(PTC)
    def post(self):
        return super(ParticipantApi, self).post()

    @app_util.auth_required(PTC)
    def put(self, p_id):
        return super(ParticipantApi, self).put(p_id)
