from rdr_service import app_util
from werkzeug.exceptions import NotFound
from flask import request

from rdr_service.api.base_api import UpdatableApi, BaseApi
from rdr_service.api_util import dispatch_task, PTC, PTC_AND_HEALTHPRO, HEALTHPRO
from rdr_service.dao.base_dao import _MIN_ID, _MAX_ID
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.pediatric_data_log_dao import PediatricDataLogDao
from rdr_service.model.utils import from_client_participant_id


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
        response, *_ = super(ParticipantApi, self).post()

        participant_id = from_client_participant_id(response['participantId'])
        self._check_for_pediatric_update(participant_id)
        dispatch_task(endpoint='update_retention_status', payload={'participant_id': participant_id})

        return response, *_

    @app_util.auth_required(PTC)
    def put(self, p_id):
        response = super(ParticipantApi, self).put(p_id)
        self._check_for_pediatric_update(p_id)
        dispatch_task(endpoint='update_retention_status', payload={'participant_id': p_id})

        return response

    def _check_for_pediatric_update(self, participant_id):
        pediatric_age_range_field = 'childAccountType'
        request_json = self.get_request_json()
        if pediatric_age_range_field in request_json:
            PediatricDataLogDao.record_age_range(
                participant_id=participant_id,
                age_range_str=request_json[pediatric_age_range_field]
            )


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
