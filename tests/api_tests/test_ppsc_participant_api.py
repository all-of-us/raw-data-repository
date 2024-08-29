import http.client
import random
from copy import deepcopy

from rdr_service import config
from rdr_service.api_util import PPSC, RDR, HEALTHPRO
from rdr_service.dao.ppsc_dao import ParticipantDao, PPSCDefaultBaseDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.model.ppsc import ParticipantEventActivity, EnrollmentEvent
from tests.helpers.unittest_base import BaseTestCase


class PPSCParticipantAPITest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        self.ppsc_partcipant_dao = ParticipantDao()
        self.ppsc_participant_activity_dao = PPSCDefaultBaseDao(model_type=ParticipantEventActivity)
        self.ppsc_enrollment_event_dao = PPSCDefaultBaseDao(model_type=EnrollmentEvent)

        activities = ['ENROLLMENT']
        for activity in activities:
            self.ppsc_data_gen.create_database_activity(
                name=activity
            )

        self.ppsc_data_gen.create_database_enrollment_event_type(
            name="Participant Created",
            source_name='participant_created'
        )

    def overwrite_test_user_roles(self, roles):
        new_user_info = deepcopy(config.getSettingJson(config.USER_INFO))
        new_user_info['example@example.com']['roles'] = roles
        self.temporarily_override_config_setting(config.USER_INFO, new_user_info)

    def test_ppsc_role_validation(self):

        accepted_roles = [PPSC, RDR]

        self.overwrite_test_user_roles(
            [random.choice(accepted_roles)]
        )

        payload = {
            'participantId': 'P22',
            'biobankId': 'T22',
            'registeredDate': '2024-03-26T13:24:03.935Z'
        }

        response = self.send_post('createParticipant', request_data=payload)
        self.assertTrue(response is not None)

        self.overwrite_test_user_roles([HEALTHPRO])

        response = self.send_post('createParticipant', request_data=payload, expected_status=http.client.FORBIDDEN)
        self.assertTrue(response.status_code == 403)

    def test_req_keys_missing_validation(self):
        # missing keys
        payload = {
            'participantId': 22
        }
        response = self.send_post('createParticipant', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Payload for createParticipant is invalid: Required keys - '
                                                   'participantId, biobankId, registeredDate')

        # extra keys - will return a response instead of an error since json will be cleaned
        payload = {
            'badKey': 22,
            'participantId': 'P22',
            'biobankId': 'T22',
            'registeredDate': '2024-03-26T13:24:03.935Z'
        }
        response = self.send_post('createParticipant', request_data=payload)
        self.assertTrue(response is not None)

        # null values
        payload = {
            'participantId': '',
            'biobankId': 'T22',
            'registeredDate': '2024-03-26T13:24:03.935Z'
        }
        response = self.send_post('createParticipant', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['message'], 'Payload for createParticipant is invalid: Required keys - '
                                                   'participantId, biobankId, registeredDate')

    def test_participant_exists_validation(self):
        current_participant = self.ppsc_data_gen.create_database_participant(
            **{
                'id': 22,
                'biobank_id': 22,
            }
        )
        payload = {
            'participantId': 'P22',
            'biobankId': 'T22',
            'registeredDate': '2024-03-26T13:24:03.935Z'
        }

        response = self.send_post('createParticipant', request_data=payload, expected_status=http.client.FORBIDDEN)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json['message'], f'Participant {payload.get("participantId")} already exists')
        self.assertEqual(current_participant.id, int(payload.get("participantId")[1:]))

    def test_biobank_id_exists_validation(self):
        current_participant = self.ppsc_data_gen.create_database_participant(
            **{
                'id': 21,
                'biobank_id': 24,
            }
        )
        payload = {
            'participantId': 'P22',
            'biobankId': 'T24',
            'registeredDate': '2024-03-26T13:24:03.935Z'
        }

        response = self.send_post('createParticipant', request_data=payload, expected_status=http.client.FORBIDDEN)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json['message'], f'Participant with Biobank ID {payload.get("biobankId")}'
                                                   f' already exists')
        self.assertEqual(current_participant.biobank_id, int(payload.get("biobankId")[1:]))

    def test_payload_inserts_participant_records(self):

        payload = {
            'participantId': 'P22',
            'biobankId': 'T22',
            'registeredDate': '2024-03-26T13:24:03.935Z'
        }
        response = self.send_post('createParticipant', request_data=payload)
        self.assertTrue(response is not None)
        self.assertEqual(response, f'Participant {payload.get("participantId")} was created successfully')

        # check insert model deps
        current_participant_event_activity = self.ppsc_participant_activity_dao.get_all()
        self.assertEqual(len(current_participant_event_activity), 1)
        current_participant_event_activity = current_participant_event_activity[0]
        self.assertEqual(current_participant_event_activity.activity_id, 1)
        self.assertEqual(current_participant_event_activity.participant_id, int(payload.get("participantId")[1:]))
        self.assertEqual(current_participant_event_activity.resource, payload)

        current_enrollment_event = self.ppsc_enrollment_event_dao.get_all()
        self.assertEqual(len(current_enrollment_event), 1)
        current_enrollment_event = current_enrollment_event[0]
        self.assertEqual(current_enrollment_event.participant_id, int(payload.get("participantId")[1:]))
        self.assertTrue(current_enrollment_event.event_id is not None)
        self.assertTrue(current_enrollment_event.event_type_id is not None)

        current_participant = self.ppsc_partcipant_dao.get_all()
        self.assertEqual(len(current_participant), 1)
        current_participant = current_participant[0]
        self.assertEqual(current_participant.biobank_id, int(payload.get("biobankId")[1:]))
        self.assertEqual(current_participant.id, int(payload.get("participantId")[1:]))
        self.assertTrue(current_participant.registered_date is not None)

    def test_participant_create_sync_rdr_schema(self):

        payload = {
            'participantId': 'P22',
            'biobankId': 'T22',
            'registeredDate': '2024-03-26T13:24:03.935Z'
        }
        response = self.send_post('createParticipant', request_data=payload)
        self.assertTrue(response is not None)
        self.assertEqual(response, f'Participant {payload.get("participantId")} was created successfully')

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("ppsc.activity")
        self.clear_table_after_test("ppsc.participant")
        self.clear_table_after_test("ppsc.enrollment_event_type")
        self.clear_table_after_test("ppsc.enrollment_event")
