from werkzeug.exceptions import NotFound, BadRequest
from flask import request

from rdr_service.app_util import auth_required
from rdr_service.api.base_api import UpdatableApi
from rdr_service.api_util import HEALTHPRO
from rdr_service.dao.participant_incentives_dao import ParticipantIncentivesDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao


class ParticipantIncentivesApi(UpdatableApi):
    def __init__(self):
        super(ParticipantIncentivesApi, self).__init__(ParticipantIncentivesDao())
        self.ps_dao = ParticipantSummaryDao()

    @auth_required(HEALTHPRO)
    def post(self, p_id):
        participant = self.ps_dao.get_by_participant_id(p_id)

        if not participant:
            raise NotFound(f"Participant with ID {p_id} not found")

        self.validate_required_data()

        # return super(ParticipantIncentivesApi, self).post()

    @auth_required(HEALTHPRO)
    def put(self, p_id):
        participant = self.ps_dao.get_by_participant_id(p_id)

        if not participant:
            raise NotFound(f"Participant with ID {p_id} not found")

        self.validate_required_data()

        # return super(ParticipantIncentivesApi, self).put()

    @staticmethod
    def _get_required_keys(required_type=None):
        key_map = {
            'POST': {
                'default': ['createdBy', 'site', 'dateGiven', 'occurrence', 'incentiveType', 'amount']
            },
            'PUT': {
                'default': ['createdBy', 'site', 'dateGiven', 'occurrence', 'incentiveType', 'amount'],
                'cancel': ['incentiveId', 'cancelledBy', 'cancelledDate']
            }
        }
        try:
            if not required_type:
                return key_map[request.method]['default']

            return key_map[request.method][required_type]

        except KeyError:
            raise BadRequest(f"Error in getting required keys for method: {request.method}")

    def validate_required_data(self):
        payload = request.get_json()
        required_type = None

        if 'cancel' in payload:
            required_type = 'cancel'

        req_keys = self._get_required_keys(required_type)

        if not all(k in list(payload.keys()) for k in req_keys):
            raise BadRequest(f"Missing required key/values in request, required: {','.join(req_keys)}")

