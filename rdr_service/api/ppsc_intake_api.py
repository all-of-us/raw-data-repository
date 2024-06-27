from dateutil import parser
from flask import request
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import BadRequest, NotFound

from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RDR, PPSC
from rdr_service.app_util import auth_required
from rdr_service import config, clock
from rdr_service.dao.ppsc_dao import PPSCDefaultBaseDao
from rdr_service.model.ppsc import ParticipantEventActivity, Activity, \
    ConsentEvent, ProfileUpdatesEvent, SurveyCompletionEvent


class PPSCIntakeAPI(BaseApi):
    def __init__(self):
        self.participant_event_activity_dao = PPSCDefaultBaseDao(model_type=ParticipantEventActivity)
        self.intake_activities = config.getSettingJson("ppsc_intake_activities")
        self.activity_records = PPSCDefaultBaseDao(model_type=Activity).get_all()
        self.consent_event_dao = PPSCDefaultBaseDao(model_type=ConsentEvent)
        self.profile_updates_event_dao = PPSCDefaultBaseDao(model_type=ProfileUpdatesEvent)
        self.survey_completion_event_dao = PPSCDefaultBaseDao(model_type=SurveyCompletionEvent)
        self.activity_date_time_value = None
        super().__init__(self.participant_event_activity_dao)

    @auth_required([PPSC, RDR])
    def post(self):
        log_api_request(log=request.log_record)

        # Validate
        self.validate_payload(req_data=self.get_request_json())

        # Route to correct activity and insert events
        inserted_event = self.handle_event_insert(
            req_data=self.get_request_json(),
        )
        return self._make_response(obj=inserted_event)

    def validate_payload(self, *, req_data: dict):
        required_keys = ['activity', 'eventType', 'participantId', 'dataElements']

        # Check required keys in payload
        if all([key in req_data for key in required_keys]) \
                and all([val for val in req_data.values() if val is not None]):
            pass
        else:
            raise BadRequest(f'Invalid Intake API Payload: Required keys: {required_keys}')

        # Check Activity is valid
        if req_data['activity'] not in self.intake_activities:
            raise BadRequest(f'Invalid Intake API Payload: Invalid Activity: {req_data["activity"]}')

        event_type_lookup_str = 'ppsc_intake_' + req_data['activity'].lower().replace(' ', '_') + '_event_types'

        # Check Event Type is valid
        if req_data['eventType'] not in config.getSettingJson(event_type_lookup_str):
            raise BadRequest(f'Invalid Intake API Payload: Invalid EventType: {req_data["eventType"]}')

        # Check for Event Authored Date
        self.activity_date_time_value = next((item['dataElementValue'] for item in req_data['dataElements'] if
                                              item['dataElementName'] == 'activity_date_time'), None)

        # Check if the activity_date_time_value is not None
        if self.activity_date_time_value is not None:
            try:
                # Parse the datetime string to a datetime object using dateutil.parser
                parsed_datetime = parser.isoparse(self.activity_date_time_value)

                # Check if the datetime is in UTC
                if parsed_datetime.tzinfo is not None and parsed_datetime.tzinfo.utcoffset(parsed_datetime) is not None:
                    self.activity_date_time_value = parsed_datetime
                else:
                    raise BadRequest("The activity_date_time_value is missing timezone info or not UTC.")

            except ValueError:
                raise BadRequest("The activity_date_time_value is not valid.")
        else:
            raise BadRequest("No activity_date_time_value provided.")

    def handle_event_insert(self, *, req_data: dict) -> dict:
        activity_record = list(filter(lambda x: x.name.lower() == req_data['activity'].lower(),
                                      self.activity_records))[0]

        # Insert participant_event_activity record
        participant_event_activity_dict = {
            'activity_id': activity_record.id,
            'participant_id': self.dao.extract_prefix_from_val(req_data['participantId']),
            'resource': req_data
        }

        # Validate participant ID and insert
        try:
            participant_event_activity = self.participant_event_activity_dao.insert(
                self.participant_event_activity_dao.model_type(**participant_event_activity_dict)
            )
        except IntegrityError:
            raise NotFound(f"Participant with ID {req_data['participantId']} not found")

        # get correct [Activity]Event DAO
        dao_str = f"{req_data['activity'].lower().replace(' ', '_')}_event_dao"
        activity_event_dao = self.__dict__.get(dao_str)

        records_to_insert = []

        # Iterate through data elements, add to bulk insert
        for data_element in req_data['dataElements']:
            now = clock.CLOCK.now()  # event_listener doesn't work with bulk inserts
            event_dict = {
                'event_id': participant_event_activity.id,
                'created': now,
                'modified': now,
                'participant_id': self.dao.extract_prefix_from_val(req_data['participantId']),
                'event_type_name': req_data['eventType'],
                'event_authored_time': self.activity_date_time_value,
                'data_element_name': data_element['dataElementName'],
                'data_element_value': data_element['dataElementValue']
            }

            records_to_insert.append(event_dict)

        activity_event_dao.insert_bulk(records_to_insert)

        return participant_event_activity.resource
