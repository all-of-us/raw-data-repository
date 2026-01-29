from dateutil import parser
from flask import request
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import BadRequest, NotFound

from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RDR, PPSC
from rdr_service.app_util import auth_required
from rdr_service.ppsc_transform_org_map import TRANSFORM_ORG_MAP
from rdr_service import config, clock
from rdr_service.dao.ppsc_dao import PPSCDefaultBaseDao, PPSCNphOptEventInDao
from rdr_service.model.ppsc import (
    ParticipantEventActivity, Activity,
    ConsentEvent, ProfileUpdatesEvent, SurveyCompletionEvent, WithdrawalEvent, DeactivationEvent,
    AccountLinkageEvent, ParticipantStatusEvent, AttributionEvent
)


class PPSCIntakeAPI(BaseApi):
    def __init__(self):
        self.participant_event_activity_dao = PPSCDefaultBaseDao(model_type=ParticipantEventActivity)
        self.intake_activities = config.getSettingJson("ppsc_intake_activities")
        self.primary_consent_types = config.getSettingJson("ppsc_primary_consent_types")
        self.activity_records = PPSCDefaultBaseDao(model_type=Activity).get_all()
        self.consent_event_dao = PPSCDefaultBaseDao(model_type=ConsentEvent)
        self.profile_updates_event_dao = PPSCDefaultBaseDao(model_type=ProfileUpdatesEvent)
        self.survey_completion_event_dao = PPSCDefaultBaseDao(model_type=SurveyCompletionEvent)
        self.withdrawal_event_dao = PPSCDefaultBaseDao(model_type=WithdrawalEvent)
        self.deactivation_event_dao = PPSCDefaultBaseDao(model_type=DeactivationEvent)
        self.participant_status_event_dao = PPSCDefaultBaseDao(model_type=ParticipantStatusEvent)
        self.attribution_event_dao = PPSCDefaultBaseDao(model_type=AttributionEvent)
        self.nph_opt_in_event_dao = PPSCNphOptEventInDao()
        self.account_linkage_event_dao = PPSCDefaultBaseDao(model_type=AccountLinkageEvent)
        self.activity_date_time_value = None
        super().__init__(self.participant_event_activity_dao)

    @auth_required([PPSC, RDR])
    def post(self):
        req_data = self.get_request_json()

        # Validate
        self.validate_payload(req_data=req_data)

        # Route to correct activity and insert events
        inserted_event = self.handle_event_insert(
            req_data=self.get_request_json(),
        )
        log_api_request(log=request.log_record, model_obj=inserted_event)
        return self._make_response(obj=inserted_event.resource)

    def validate_payload(self, *, req_data: dict):
        required_keys = ['activity', 'eventType', 'participantId', 'dataElements']

        # Check required keys in payload
        if all(key in req_data for key in required_keys) \
                and all(val for val in req_data.values() if val is not None):
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
            if req_data['eventType'] in ['Enrollment Status', 'UBR Status'] :
                pass
            else:
                raise BadRequest("No activity_date_time_value provided.")

        # Check for Primary Consent
        if req_data['eventType'] not in self.primary_consent_types:
            if not self.check_consent(req_data['participantId'].split('P')[1],
                                              self.primary_consent_types,
                                              'activity_status',
                                              '%yes%'):
                raise BadRequest("No Primary Consent record found.")

        # Check Enrollment Status for timestamps
        if req_data['eventType'] == "Enrollment Status":
            data_element_names = [item['dataElementName'].lower() for item in req_data['dataElements']]
            for name in data_element_names:
                if '_date_time' not in name:
                    if not name+'_date_time' in data_element_names:
                        raise BadRequest(f"Enrollment Status {name} is missing {name+'_date_time'}.")

        # Check profile data for date of birth
        if req_data['eventType'] == 'Profile Data':
            dob_present = False
            for item in req_data['dataElements']:
                if item['dataElementName'].lower() == 'piibirthinformation_birthdate':
                    dob_present = True
                    if item.get('dataElementValue', None) is None:
                        raise BadRequest("Invalid Date of Birth")
            if not dob_present:
                raise BadRequest("Profile Data payload missing Date of Birth")

    def handle_event_insert(self, *, req_data: dict):
        activity_record = list(filter(lambda x: x.name.lower() == req_data['activity'].lower(),
                                      self.activity_records))

        if not activity_record:
            raise BadRequest(f"Activity {req_data['activity']} is Invalid.")

        activity_record = activity_record[0]

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

            # DA-4970: Transforming SEEC_MOREHOUSE to DREF_MOREHOUSE
            if (
                req_data['activity'].lower() == 'attribution' and
                data_element.get('dataElementName').lower() == 'activity_status' and
                data_element.get('dataElementValue').upper() in TRANSFORM_ORG_MAP
            ):
                data_element['dataElementValue'] = TRANSFORM_ORG_MAP[data_element.get('dataElementValue').upper()]

            now = clock.CLOCK.now()  # event_listener doesn't work with bulk inserts
            event_dict = {
                'event_id': participant_event_activity.id,
                'created': now,
                'modified': now,
                'participant_id': self.dao.extract_prefix_from_val(req_data['participantId']),
                'event_type_name': req_data['eventType'],
                'event_authored_time': self.activity_date_time_value,
                'data_element_name': data_element['dataElementName'].strip('​')
            }

            # PPSC sends strings or arrays (multi-select)
            if isinstance(data_element['dataElementValue'], list):
                # The below iteration is to handle multi-select answers
                for value in data_element['dataElementValue']:
                    event_copy = event_dict.copy()
                    event_copy["data_element_value"] = value
                    records_to_insert.append(event_copy)
            else:
                event_dict["data_element_value"] = data_element['dataElementValue']
                records_to_insert.append(event_dict)

        activity_event_dao.insert_bulk(records_to_insert)

        return participant_event_activity

    def check_consent(self, participant_id, event_types, data_element_name, data_element_value):
        with self.dao.session() as session:
            return session.query(ConsentEvent).filter(
                ConsentEvent.participant_id == participant_id,
                ConsentEvent.event_type_name.in_(event_types),
                ConsentEvent.data_element_name == data_element_name,
                ConsentEvent.data_element_value.ilike(data_element_value)
            ).first()
