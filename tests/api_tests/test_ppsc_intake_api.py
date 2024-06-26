import http
import random
from copy import deepcopy
from datetime import datetime

from rdr_service import clock, config
from rdr_service.api_util import HEALTHPRO, PPSC, RDR
from rdr_service.dao.ppsc_dao import PPSCDefaultBaseDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.model.ppsc import ParticipantEventActivity, ConsentEvent
from tests.helpers.unittest_base import BaseTestCase


class PPSCIntakeAPITest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        self.ppsc_participant_activity_dao = PPSCDefaultBaseDao(model_type=ParticipantEventActivity)
        self.consent_event_dao = PPSCDefaultBaseDao(model_type=ConsentEvent)

        activities = ["ENROLLMENT", "Consent", "Survey Completion", "Profile Updates"]
        for activity in activities:
            self.ppsc_data_gen.create_database_activity(
                name=activity
            )

    def overwrite_test_user_roles(self, roles):
        new_user_info = deepcopy(config.getSettingJson(config.USER_INFO))
        new_user_info['example@example.com']['roles'] = roles
        self.temporarily_override_config_setting(config.USER_INFO, new_user_info)

    def test_intake_roles(self):
        accepted_roles = [PPSC, RDR]

        self.overwrite_test_user_roles(
            [random.choice(accepted_roles)]
        )

        payload = {
            "activity": "Consent",
            "eventType": "Primary Consent",
            "participantId": f"P1222",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.NOT_FOUND)
        self.assertTrue(response is not None)

        self.overwrite_test_user_roles([HEALTHPRO])

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.FORBIDDEN)
        self.assertTrue(response.status_code == 403)

    def test_intake_required_fields(self):
        participant = self.ppsc_data_gen.create_database_participant()
        payload = {
            "eventType": "Primary Consent",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

        payload = {
            "activity": "Consent",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

        payload = {
            "activity": "Consent",
            "eventType": "Primary Consent",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_activity_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Hamburger",
            "eventType": "Primary Consent",
            "participantId": f"P{participant.id}",
            "dataElements": [
               {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
               },
               {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
               },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_consent_event_type_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Consent",
            "eventType": "Pepperoni",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_consent_activity_date_time_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Consent",
            "eventType": "Primary Consent",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
                }
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

        payload = {
            "activity": "Consent",
            "eventType": "Primary Consent",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "PineappleZ"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

        payload = {
            "activity": "Consent",
            "eventType": "Primary Consent",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_participant_validation(self):
        payload = {
            "activity": "Consent",
            "eventType": "Primary Consent",
            "participantId": f"P10000",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.NOT_FOUND)
        self.assertEqual(response.status_code, 404)

    def test_intake_consent_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Consent",
            "eventType": "Primary Consent",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        self.assertEqual(1, len(participant_event_activities))

        self.assertEqual(test_time, participant_event_activities[0].created)
        self.assertEqual(test_time, participant_event_activities[0].modified)
        self.assertEqual(participant.id, participant_event_activities[0].participant_id)
        self.assertEqual(payload, participant_event_activities[0].resource)
        self.assertEqual(2, participant_event_activities[0].activity_id)

        consent_events = self.consent_event_dao.get_all()
        self.assertEqual(2, len(consent_events))
        self.assertEqual(test_time, consent_events[0].created)
        self.assertEqual(test_time, consent_events[0].modified)
        self.assertEqual(1, consent_events[0].event_id)
        self.assertEqual(participant.id, consent_events[0].participant_id)
        self.assertEqual('Primary Consent', consent_events[0].event_type_name)
        self.assertEqual('activity_status', consent_events[0].data_element_name)
        self.assertEqual('yes', consent_events[0].data_element_value)

        self.assertEqual(test_time, consent_events[1].created)
        self.assertEqual(test_time, consent_events[1].modified)
        self.assertEqual(1, consent_events[1].event_id)
        self.assertEqual(participant.id, consent_events[1].participant_id)
        self.assertEqual('Primary Consent', consent_events[1].event_type_name)
        self.assertEqual('activity_date_time', consent_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00Z", consent_events[1].data_element_value)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("ppsc.activity")
        self.clear_table_after_test("ppsc.participant")
        self.clear_table_after_test("ppsc.participant_event_activity")
        self.clear_table_after_test("ppsc.consent_event")
