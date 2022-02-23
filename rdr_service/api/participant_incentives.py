from werkzeug.exceptions import NotFound, BadRequest
from flask import request

from rdr_service.app_util import auth_required
from rdr_service.api.base_api import UpdatableApi, log_api_request
from rdr_service.api_util import RDR_AND_HEALTHPRO
from rdr_service.dao.participant_incentives_dao import ParticipantIncentivesDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.site_dao import SiteDao


class ParticipantIncentivesApi(UpdatableApi):
    def __init__(self):
        super(ParticipantIncentivesApi, self).__init__(ParticipantIncentivesDao())
        self.ps_dao = ParticipantSummaryDao()
        self.site_dao = SiteDao()
        self.site_id = None

    @auth_required(RDR_AND_HEALTHPRO)
    def post(self, p_id):
        participant = self.ps_dao.get_by_participant_id(p_id)

        if not participant:
            raise NotFound(f"Participant with ID {p_id} not found")

        self.validate_payload_data()

        model = self.dao.from_client_json(request.get_json(force=True))
        model.participantId = p_id
        model.site = self.site_id

        obj = self._do_insert(model)

        log_api_request(log=request.log_record)
        return self._make_response(obj)

    @auth_required(RDR_AND_HEALTHPRO)
    def put(self, p_id):
        participant = self.ps_dao.get_by_participant_id(p_id)

        if not participant:
            raise NotFound(f"Participant with ID {p_id} not found")

        self.validate_payload_data()

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

    def validate_payload_data(self):
        resource = request.get_json(force=True)
        required_type = None

        if 'cancel' in resource:
            required_type = 'cancel'

        req_keys = self._get_required_keys(required_type)

        if not all(k in list(resource.keys()) for k in req_keys):
            raise BadRequest(f"Missing required key/values in request, required: {','.join(req_keys)}")

        valid_site = self.site_dao.get_by_google_group(resource['site'])

        if not valid_site:
            raise BadRequest(f"Site for group {resource['site']} is invalid")

        self.site_id = valid_site.siteId

