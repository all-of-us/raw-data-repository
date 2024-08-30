import logging

from flask import request
from werkzeug.exceptions import BadRequest, Forbidden

from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RDR, PPSC
from rdr_service.app_util import auth_required, nonprod
from rdr_service.dao.ppsc_dao import ParticipantDao, PPSCDefaultBaseDao
from rdr_service.model.ppsc import EnrollmentEvent, EnrollmentEventType, ParticipantEventActivity, Activity
from rdr_service.services.ppsc.ppsc_data_sync import CreateParticipantSync


# pylint: disable=broad-except
class PPSCParticipantAPI(BaseApi):
    def __init__(self):
        super().__init__(ParticipantDao())
        self.ppsc_participant_dao = ParticipantDao()
        self.current_activities = PPSCDefaultBaseDao(model_type=Activity).get_all()
        self.ppsc_participant_activity_dao = PPSCDefaultBaseDao(model_type=ParticipantEventActivity)
        self.ppsc_enrollment_type_dao = PPSCDefaultBaseDao(model_type=EnrollmentEventType)
        self.ppsc_enrollment_event_dao = PPSCDefaultBaseDao(model_type=EnrollmentEvent)

    @auth_required([PPSC, RDR])
    def post(self):
        # Adding request log here so if exception is raised
        # per validation fail the payload is stored
        log_api_request(log=request.log_record)

        req_data = self.get_request_json()
        required_keys = ['participantId', 'biobankId', 'registeredDate']

        # check req keys in payload
        if all(key in req_data for key in required_keys) \
                and all(val for val in req_data.values() if val is not None):

            converted_dict: dict = {
                self.dao.camel_to_snake(k): self.dao.extract_prefix_from_val(v) for (k, v)
                in req_data.items() if k in required_keys
            }

            if self.dao.get_participant_by_participant_id(
                participant_id=int(converted_dict.get('participant_id'))
            ):
                raise Forbidden(f'Participant {req_data.get("participantId")} already exists')

            if self.dao.get_participant_by_biobank_id(
                biobank_id=int(converted_dict.get('biobank_id'))
            ):
                raise Forbidden(f'Participant with Biobank ID {req_data.get("biobankId")} already exists')

            inserted_participant = self.handle_participant_insert(
                participant_data=converted_dict,
                req_data=req_data
            )
            return self._make_response(obj=inserted_participant)

        response_string: str = ', '.join(required_keys)
        raise BadRequest(f'Payload for createParticipant is invalid: Required keys - {response_string}')

    def handle_participant_insert(self, *, participant_data: dict, req_data: dict) -> dict:
        enrollment_activity = list(filter(lambda x: x.name.lower() == 'enrollment', self.current_activities))[0]
        enrollment_type = list(filter(lambda x: x.source_name.lower() == 'participant_created',
                                      self.ppsc_enrollment_type_dao.get_all()))[0]

        participant_data['id'] = participant_data.get('participant_id')
        del participant_data['participant_id']

        inserted_participant = self.ppsc_participant_dao.insert(
            self.ppsc_participant_dao.model_type(**participant_data)
        )

        participant_event_activity_dict = {
            'activity_id': enrollment_activity.id,
            'participant_id': inserted_participant.id,
            'resource': req_data
        }
        participant_event_activity = self.ppsc_participant_activity_dao.insert(
            self.ppsc_participant_activity_dao.model_type(**participant_event_activity_dict)
        )

        enrollment_event_dict = {
            'event_id': participant_event_activity.id,
            'participant_id': inserted_participant.id,
            'event_type_id': enrollment_type.id
        }
        self.ppsc_enrollment_event_dao.insert(
            self.ppsc_enrollment_event_dao.model_type(**enrollment_event_dict)
        )

        self.sync_to_rdr_schema(participant_data=inserted_participant.asdict())
        return inserted_participant

    @classmethod
    @nonprod
    def sync_to_rdr_schema(cls, *, participant_data):
        try:
            CreateParticipantSync(participant_data=participant_data).run_sync()
        except Exception as e:
            logging.warning(f'Error when syncing data to RDR schema: {e}')
