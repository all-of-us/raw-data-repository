import http
import random
from copy import deepcopy
from datetime import datetime

from rdr_service import clock, config
from rdr_service.api_util import HEALTHPRO, PPSC, RDR
from rdr_service.dao.ppsc_dao import PPSCDefaultBaseDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.model.ppsc import ParticipantEventActivity, ConsentEvent, SurveyCompletionEvent, ProfileUpdatesEvent, \
    WithdrawalEvent, DeactivationEvent, ParticipantStatusEvent, SiteAttributionEvent
from tests.helpers.unittest_base import BaseTestCase


class PPSCIntakeAPITest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        self.ppsc_participant_activity_dao = PPSCDefaultBaseDao(model_type=ParticipantEventActivity)
        self.consent_event_dao = PPSCDefaultBaseDao(model_type=ConsentEvent)
        self.survey_completion_event_dao = PPSCDefaultBaseDao(model_type=SurveyCompletionEvent)
        self.profile_updates_event_dao = PPSCDefaultBaseDao(model_type=ProfileUpdatesEvent)
        self.withdrawal_event_dao = PPSCDefaultBaseDao(model_type=WithdrawalEvent)
        self.deactivation_event_dao = PPSCDefaultBaseDao(model_type=DeactivationEvent)
        self.participant_status_event_dao = PPSCDefaultBaseDao(model_type=ParticipantStatusEvent)
        self.site_attribution_event_dao = PPSCDefaultBaseDao(model_type=SiteAttributionEvent)

        activities = [
            "ENROLLMENT",
            "Consent",
            "Survey Completion",
            "Profile Updates",
            "Withdrawal",
            "Deactivation",
            "Participant Status",
            "Site Attribution"
        ]
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

    def test_intake_survey_completion_event_type_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Survey Completion",
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

    def test_intake_survey_completion_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Survey Completion",
            "eventType": "The Basics",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_complete"
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
        self.assertEqual(3, participant_event_activities[0].activity_id)

        survey_events = self.survey_completion_event_dao.get_all()
        self.assertEqual(2, len(survey_events))
        self.assertEqual(test_time, survey_events[0].created)
        self.assertEqual(test_time, survey_events[0].modified)
        self.assertEqual(1, survey_events[0].event_id)
        self.assertEqual(participant.id, survey_events[0].participant_id)
        self.assertEqual('The Basics', survey_events[0].event_type_name)
        self.assertEqual('activity_status', survey_events[0].data_element_name)
        self.assertEqual('submitted_complete', survey_events[0].data_element_value)

        self.assertEqual(test_time, survey_events[1].created)
        self.assertEqual(test_time, survey_events[1].modified)
        self.assertEqual(1, survey_events[1].event_id)
        self.assertEqual(participant.id, survey_events[1].participant_id)
        self.assertEqual('The Basics', survey_events[1].event_type_name)
        self.assertEqual('activity_date_time', survey_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00Z", survey_events[1].data_element_value)

    def test_intake_profile_updates_event_type_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Profile Updates",
            "eventType": "Pepperoni",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "first_name",
                    "dataElementValue": "Jane"
                },
                {
                    "dataElementName": "last_name",
                    "dataElementValue": "Eyre"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_profile_updates_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Profile Updates",
            "eventType": "Profile Data",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "first_name",
                    "dataElementValue": "Jane"
                },
                {
                    "dataElementName": "last_name",
                    "dataElementValue": "Eyre"
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
        self.assertEqual(4, participant_event_activities[0].activity_id)

        profile_updates_events = self.profile_updates_event_dao.get_all()
        self.assertEqual(3, len(profile_updates_events))

        self.assertEqual(test_time, profile_updates_events[0].created)
        self.assertEqual(test_time, profile_updates_events[0].modified)
        self.assertEqual(1, profile_updates_events[0].event_id)
        self.assertEqual(participant.id, profile_updates_events[0].participant_id)
        self.assertEqual('Profile Data', profile_updates_events[0].event_type_name)
        self.assertEqual('first_name', profile_updates_events[0].data_element_name)
        self.assertEqual('Jane', profile_updates_events[0].data_element_value)

        self.assertEqual(test_time, profile_updates_events[1].created)
        self.assertEqual(test_time, profile_updates_events[1].modified)
        self.assertEqual(1, profile_updates_events[1].event_id)
        self.assertEqual(participant.id, profile_updates_events[1].participant_id)
        self.assertEqual('Profile Data', profile_updates_events[1].event_type_name)
        self.assertEqual('last_name', profile_updates_events[1].data_element_name)
        self.assertEqual("Eyre", profile_updates_events[1].data_element_value)

        self.assertEqual(test_time, profile_updates_events[2].created)
        self.assertEqual(test_time, profile_updates_events[2].modified)
        self.assertEqual(1, profile_updates_events[2].event_id)
        self.assertEqual(participant.id, profile_updates_events[2].participant_id)
        self.assertEqual('Profile Data', profile_updates_events[2].event_type_name)
        self.assertEqual('activity_date_time', profile_updates_events[2].data_element_name)
        self.assertEqual("2024-05-20T14:30:00Z", profile_updates_events[2].data_element_value)

    def test_intake_withdrawal_event_type_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Withdrawal",
            "eventType": "Pepperoni",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "withdrawn"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_withdrawal_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Withdrawal",
            "eventType": "Withdrawal",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "withdrawn"
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
        self.assertEqual(5, participant_event_activities[0].activity_id)

        withdrawal_events = self.withdrawal_event_dao.get_all()
        self.assertEqual(2, len(withdrawal_events))
        self.assertEqual(test_time, withdrawal_events[0].created)
        self.assertEqual(test_time, withdrawal_events[0].modified)
        self.assertEqual(1, withdrawal_events[0].event_id)
        self.assertEqual(participant.id, withdrawal_events[0].participant_id)
        self.assertEqual('Withdrawal', withdrawal_events[0].event_type_name)
        self.assertEqual('activity_status', withdrawal_events[0].data_element_name)
        self.assertEqual('withdrawn', withdrawal_events[0].data_element_value)

        self.assertEqual(test_time, withdrawal_events[1].created)
        self.assertEqual(test_time, withdrawal_events[1].modified)
        self.assertEqual(1, withdrawal_events[1].event_id)
        self.assertEqual(participant.id, withdrawal_events[1].participant_id)
        self.assertEqual('Withdrawal', withdrawal_events[1].event_type_name)
        self.assertEqual('activity_date_time', withdrawal_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00Z", withdrawal_events[1].data_element_value)

    def test_intake_deactivation_event_type_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Deactivation",
            "eventType": "Pepperoni",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "deactivated"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_deactivation_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Deactivation",
            "eventType": "Deactivation",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "deactivated"
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
        self.assertEqual(6, participant_event_activities[0].activity_id)

        deactivation_events = self.deactivation_event_dao.get_all()
        self.assertEqual(2, len(deactivation_events))
        self.assertEqual(test_time, deactivation_events[0].created)
        self.assertEqual(test_time, deactivation_events[0].modified)
        self.assertEqual(1, deactivation_events[0].event_id)
        self.assertEqual(participant.id, deactivation_events[0].participant_id)
        self.assertEqual('Deactivation', deactivation_events[0].event_type_name)
        self.assertEqual('activity_status', deactivation_events[0].data_element_name)
        self.assertEqual('deactivated', deactivation_events[0].data_element_value)

        self.assertEqual(test_time, deactivation_events[1].created)
        self.assertEqual(test_time, deactivation_events[1].modified)
        self.assertEqual(1, deactivation_events[1].event_id)
        self.assertEqual(participant.id, deactivation_events[1].participant_id)
        self.assertEqual('Deactivation', deactivation_events[1].event_type_name)
        self.assertEqual('activity_date_time', deactivation_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00Z", deactivation_events[1].data_element_value)

    def test_intake_participant_status_event_type_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Participant Status",
            "eventType": "Pepperoni",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "not_test"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_participant_status_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Participant Status",
            "eventType": "Participant Status",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "not_test"
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
        self.assertEqual(7, participant_event_activities[0].activity_id)

        participant_status_events = self.participant_status_event_dao.get_all()
        self.assertEqual(2, len(participant_status_events))
        self.assertEqual(test_time, participant_status_events[0].created)
        self.assertEqual(test_time, participant_status_events[0].modified)
        self.assertEqual(1, participant_status_events[0].event_id)
        self.assertEqual(participant.id, participant_status_events[0].participant_id)
        self.assertEqual('Participant Status', participant_status_events[0].event_type_name)
        self.assertEqual('activity_status', participant_status_events[0].data_element_name)
        self.assertEqual('not_test', participant_status_events[0].data_element_value)

        self.assertEqual(test_time, participant_status_events[1].created)
        self.assertEqual(test_time, participant_status_events[1].modified)
        self.assertEqual(1, participant_status_events[1].event_id)
        self.assertEqual(participant.id, participant_status_events[1].participant_id)
        self.assertEqual('Participant Status', participant_status_events[1].event_type_name)
        self.assertEqual('activity_date_time', participant_status_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00Z", participant_status_events[1].data_element_value)

    def test_intake_site_attribution_event_type_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Site Attribution",
            "eventType": "Pepperoni",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "site_name",
                    "dataElementValue": "test-site-1"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_site_attribution_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Site Attribution",
            "eventType": "Site Attribution",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "site_name",
                    "dataElementValue": "test-site-1"
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
        self.assertEqual(8, participant_event_activities[0].activity_id)

        site_attribution_events = self.site_attribution_event_dao.get_all()
        self.assertEqual(2, len(site_attribution_events))
        self.assertEqual(test_time, site_attribution_events[0].created)
        self.assertEqual(test_time, site_attribution_events[0].modified)
        self.assertEqual(1, site_attribution_events[0].event_id)
        self.assertEqual(participant.id, site_attribution_events[0].participant_id)
        self.assertEqual('Site Attribution', site_attribution_events[0].event_type_name)
        self.assertEqual('site_name', site_attribution_events[0].data_element_name)
        self.assertEqual('test-site-1', site_attribution_events[0].data_element_value)

        self.assertEqual(test_time, site_attribution_events[1].created)
        self.assertEqual(test_time, site_attribution_events[1].modified)
        self.assertEqual(1, site_attribution_events[1].event_id)
        self.assertEqual(participant.id, site_attribution_events[1].participant_id)
        self.assertEqual('Site Attribution', site_attribution_events[1].event_type_name)
        self.assertEqual('activity_date_time', site_attribution_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00Z", site_attribution_events[1].data_element_value)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("ppsc.activity")
        self.clear_table_after_test("ppsc.participant")
        self.clear_table_after_test("ppsc.participant_event_activity")
        self.clear_table_after_test("ppsc.consent_event")
        self.clear_table_after_test("ppsc.survey_completion_event")
        self.clear_table_after_test("ppsc.profile_updates_event")
        self.clear_table_after_test("ppsc.withdrawal_event")
        self.clear_table_after_test("ppsc.deactivation_event")
        self.clear_table_after_test("ppsc.participant_status_event")
        self.clear_table_after_test("ppsc.site_attribution_event")

