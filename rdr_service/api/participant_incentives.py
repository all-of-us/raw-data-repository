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

        model = self.dao.from_client_json(
            request.get_json(force=True)
        )
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

        resource = request.get_json(force=True)

        self.validate_payload_data()

        model = self.dao.from_client_json(
            request.get_json(force=True),
            incentive_id=resource['incentiveId']
        )

        model.id = resource['incentiveId']
        self._do_update(model)

        log_api_request(log=request.log_record)
        return self._make_response(model)

    @staticmethod
    def _get_required_keys(**kwargs):
        if request.method == 'PUT':
            base_put = ['incentiveId']
            if kwargs.get('is_cancel') is not None:
                return base_put + ['cancelledBy', 'cancelledDate']

            if kwargs.get('is_declined') is not None:
                return base_put

        return ['site', 'dateGiven']

    def validate_payload_data(self):
        resource = request.get_json(force=True)

        if resource.get('site'):
            valid_site = self.site_dao.get_by_google_group(resource['site'])

            if not valid_site:
                raise BadRequest(f"Site for group {resource['site']} is invalid")

            self.site_id = valid_site.siteId

        req_keys = self._get_required_keys(
            is_cancel=resource.get('cancel'),
            is_declined=resource.get('declined')
        )

        if not all(k in list(resource.keys()) for k in req_keys):
            raise BadRequest(f"Missing required key/values in request, required: {' | '.join(req_keys)}")


