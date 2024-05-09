from flask import request
from werkzeug.exceptions import BadRequest

from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RDR, PPSC
from rdr_service.app_util import auth_required
from rdr_service.dao.ppsc_dao import ParticipantDao


class PPSCParticipantAPI(BaseApi):
    def __init__(self):
        super().__init__(ParticipantDao())

    @auth_required([PPSC, RDR])
    def post(self):
        # Adding request log here so if exception is raised
        # per validation fail the payload is stored
        log_api_request(log=request.log_record)

        req_data = self.get_request_json()
        required_keys = ['participantId', 'biobankId', 'registeredDate']

        # check req keys in payload
        if all([key in req_data for key in required_keys]) \
                and all([val for val in req_data.values() if val is not None]):

            if self.dao.get_participant_by_participant_id(
                participant_id=req_data.get('participantId')
            ):
                raise BadRequest(f'Participant {req_data.get("participantId")} already exists')

            # converted_dict: dict = {self.dao.camel_to_snake(k): v for k, v in req_data.items()}
            # inserted_participant = self.dao.insert(**self.dao.model_type)
            # return self._make_response()

        response_string: str = ', '.join(required_keys)
        raise BadRequest(f'Missing/Extra data in createParticipant payload: Required keys - {response_string}')

    def handle_participant_insert(self):
        ...
