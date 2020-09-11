from rdr_service import app_util
from werkzeug.exceptions import NotFound
from flask import request

from rdr_service.api.base_api import UpdatableApi, BaseApi
from rdr_service.api_util import PTC, PTC_AND_HEALTHPRO, HEALTHPRO
from rdr_service.dao.base_dao import _MIN_ID, _MAX_ID
from rdr_service.dao.participant_dao import ParticipantDao


class ParticipantApi(UpdatableApi):
    def __init__(self):
        super(ParticipantApi, self).__init__(ParticipantDao())

    @app_util.auth_required(PTC_AND_HEALTHPRO)
    def get(self, p_id):
        # Make sure participant id is in the correct range of possible values.
        if isinstance(p_id, int) and not _MIN_ID <= p_id <= _MAX_ID:
            raise NotFound(f"Participant with ID {p_id} is not found.")
        return super().get(p_id)

    @app_util.auth_required(PTC)
    def post(self):
        return super(ParticipantApi, self).post()

    @app_util.auth_required(PTC)
    def put(self, p_id):
        return super(ParticipantApi, self).put(p_id)


class ParticipantResearchIdApi(BaseApi):
    def __init__(self):
        super(ParticipantResearchIdApi, self).__init__(ParticipantDao())

    @app_util.auth_required(HEALTHPRO)
    def get(self):
        kwargs = {
            'sign_up_after': request.args.get('signUpAfter'),
            'sort': request.args.get('sort'),
        }
        return self.dao.get_pid_rid_mapping(**kwargs)
