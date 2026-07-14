import http
import random
from copy import deepcopy
from datetime import datetime

from rdr_service import clock, config
from rdr_service.api_util import HEALTHPRO, PPSC, RDR
from rdr_service.dao.ppsc_dao import PPSCDefaultBaseDao, PPSCNphOptEventInDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.model.ppsc import (
    Activity, ParticipantEventActivity, ConsentEvent, SurveyCompletionEvent, ProfileUpdatesEvent,
    WithdrawalEvent, DeactivationEvent, ParticipantStatusEvent, AttributionEvent, AccountLinkageEvent)
from rdr_service.model.requests_log import RequestsLog
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
        self.attribution_event_dao = PPSCDefaultBaseDao(model_type=AttributionEvent)
        self.nph_opt_in_event_dao = PPSCNphOptEventInDao()
        self.account_linkage_event_dao = PPSCDefaultBaseDao(model_type=AccountLinkageEvent)

        activities = [
            "ENROLLMENT",
            "Consent",
            "Survey Completion",
            "Profile Updates",
            "Withdrawal",
            "Deactivation",
            "Participant Status",
            "Attribution",
            "NPH Opt In",
            "Account Linkage"
        ]
        for activity in activities:
            self.ppsc_data_gen.create_database_activity(
                name=activity
            )

    def send_valid_primary_consent(self, participant, consent_type="Primary Consent", status="yes", age_group=None):
        payload = {
            "activity": "Consent",
            "eventType": consent_type,
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": status
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }
        if age_group:
            payload['dataElements'].append({
                'dataElementName': 'age_group',
                'dataElementValue': age_group
            })

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

    def send_pediatric_assent(self, participant, status, age_group='7-12', authored_timestamp=datetime(2024, 6, 25, 12, 1)):
        payload = {
            "activity": "Survey Completion",
            "eventType": 'Pediatrics Assent',
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": status
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": authored_timestamp.isoformat() + 'Z'
                },
                {
                    'dataElementName': 'age_group',
                    'dataElementValue': age_group
                }
            ]
        }

        self.send_post('Intake', request_data=payload)

    def filter_events_by_type(self, events, activity_id):
        for event in events:
            if event.activity_id == activity_id:
                return event

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
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
        self.assertEqual("2024-05-20T14:30:00.000Z", consent_events[1].data_element_value)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_survey_completion_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                }
            ]
        }

        payload2 = {
            "activity": "Survey Completion",
            "eventType": "Social Factors Update",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_complete"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)
            self.send_post('Intake', request_data=payload2, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities, 3)

        self.assertEqual(test_time, participant_event_activities.created)
        self.assertEqual(test_time, participant_event_activities.modified)
        self.assertEqual(participant.id, participant_event_activities.participant_id)
        self.assertEqual(payload, participant_event_activities.resource)
        self.assertEqual(3, participant_event_activities.activity_id)

        survey_events = self.survey_completion_event_dao.get_all()
        self.assertEqual(4, len(survey_events))
        self.assertEqual(test_time, survey_events[0].created)
        self.assertEqual(test_time, survey_events[0].modified)
        self.assertEqual(participant_event_activities.id, survey_events[0].event_id)
        self.assertEqual(participant.id, survey_events[0].participant_id)
        self.assertEqual('The Basics', survey_events[0].event_type_name)
        self.assertEqual('activity_status', survey_events[0].data_element_name)
        self.assertEqual('submitted_complete', survey_events[0].data_element_value)

        self.assertEqual(test_time, survey_events[1].created)
        self.assertEqual(test_time, survey_events[1].modified)
        self.assertEqual(participant_event_activities.id, survey_events[1].event_id)
        self.assertEqual(participant.id, survey_events[1].participant_id)
        self.assertEqual('The Basics', survey_events[1].event_type_name)
        self.assertEqual('activity_date_time', survey_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00.000Z", survey_events[1].data_element_value)

        self.assertEqual(test_time, survey_events[2].created)
        self.assertEqual(test_time, survey_events[2].modified)
        self.assertEqual(participant.id, survey_events[2].participant_id)
        self.assertEqual('Social Factors Update', survey_events[2].event_type_name)
        self.assertEqual('activity_status', survey_events[2].data_element_name)
        self.assertEqual('submitted_complete', survey_events[2].data_element_value)

        self.assertEqual(test_time, survey_events[3].created)
        self.assertEqual(test_time, survey_events[3].modified)
        self.assertEqual(participant.id, survey_events[3].participant_id)
        self.assertEqual('Social Factors Update', survey_events[3].event_type_name)
        self.assertEqual('activity_date_time', survey_events[3].data_element_name)
        self.assertEqual("2024-05-20T14:30:00.000Z", survey_events[3].data_element_value)


    def test_intake_survey_data_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

        payload = {
            "activity": "Survey Completion",
            "eventType": "Basics Data",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "thebasics_birthplace",
                    "dataElementValue": [
                        "USA"
                    ]
                },
                {
                    "dataElementName": "race_whatraceethnicity",
                    "dataElementValue": [
                        "WhatRaceEthnicity_AIAN",
                        "WhatRaceEthnicity_Hispanic"
                    ]
                },
                {
                    "dataElementName": "biologicalsexatbirth_sexatbirth",
                    "dataElementValue": [
                        "SexAtBirth_Male"
                    ]
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        survey_events = self.survey_completion_event_dao.get_all()
        self.assertEqual(5, len(survey_events))
        self.assertEqual(participant.id, survey_events[0].participant_id)
        self.assertEqual('Basics Data', survey_events[0].event_type_name)

        self.assertEqual('thebasics_birthplace', survey_events[0].data_element_name)
        self.assertEqual('USA', survey_events[0].data_element_value)

        self.assertEqual('race_whatraceethnicity', survey_events[1].data_element_name)
        self.assertEqual('WhatRaceEthnicity_AIAN', survey_events[1].data_element_value)

        self.assertEqual('race_whatraceethnicity', survey_events[2].data_element_name)
        self.assertEqual('WhatRaceEthnicity_Hispanic', survey_events[2].data_element_value)

        self.assertEqual('biologicalsexatbirth_sexatbirth', survey_events[3].data_element_name)
        self.assertEqual('SexAtBirth_Male', survey_events[3].data_element_value)

        self.assertEqual('activity_date_time', survey_events[4].data_element_name)
        self.assertEqual("2024-05-20T14:30:00.000Z", survey_events[4].data_element_value)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_profile_updates_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
                {
                    "dataElementName": "piibirthinformation_birthdate",
                    "dataElementValue": "2000-01-01"
                }
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities, 4)

        self.assertEqual(test_time, participant_event_activities.created)
        self.assertEqual(test_time, participant_event_activities.modified)
        self.assertEqual(participant.id, participant_event_activities.participant_id)
        self.assertEqual(payload, participant_event_activities.resource)
        self.assertEqual(4, participant_event_activities.activity_id)

        profile_updates_events = self.profile_updates_event_dao.get_all()
        self.assertEqual(4, len(profile_updates_events))

        self.assertEqual(test_time, profile_updates_events[0].created)
        self.assertEqual(test_time, profile_updates_events[0].modified)
        self.assertEqual(participant_event_activities.id, profile_updates_events[0].event_id)
        self.assertEqual(participant.id, profile_updates_events[0].participant_id)
        self.assertEqual('Profile Data', profile_updates_events[0].event_type_name)
        self.assertEqual('first_name', profile_updates_events[0].data_element_name)
        self.assertEqual('Jane', profile_updates_events[0].data_element_value)

        self.assertEqual(test_time, profile_updates_events[1].created)
        self.assertEqual(test_time, profile_updates_events[1].modified)
        self.assertEqual(participant_event_activities.id, profile_updates_events[1].event_id)
        self.assertEqual(participant.id, profile_updates_events[1].participant_id)
        self.assertEqual('Profile Data', profile_updates_events[1].event_type_name)
        self.assertEqual('last_name', profile_updates_events[1].data_element_name)
        self.assertEqual("Eyre", profile_updates_events[1].data_element_value)

        self.assertEqual(test_time, profile_updates_events[2].created)
        self.assertEqual(test_time, profile_updates_events[2].modified)
        self.assertEqual(participant_event_activities.id, profile_updates_events[2].event_id)
        self.assertEqual(participant.id, profile_updates_events[2].participant_id)
        self.assertEqual('Profile Data', profile_updates_events[2].event_type_name)
        self.assertEqual('activity_date_time', profile_updates_events[2].data_element_name)
        self.assertEqual("2024-05-20T14:30:00.000Z", profile_updates_events[2].data_element_value)

        self.assertEqual(test_time, profile_updates_events[3].created)
        self.assertEqual(test_time, profile_updates_events[3].modified)
        self.assertEqual(participant_event_activities.id, profile_updates_events[3].event_id)
        self.assertEqual(participant.id, profile_updates_events[3].participant_id)
        self.assertEqual('Profile Data', profile_updates_events[3].event_type_name)
        self.assertEqual('piibirthinformation_birthdate', profile_updates_events[3].data_element_name)
        self.assertEqual("2000-01-01", profile_updates_events[3].data_element_value)

    def test_intake_withdrawal_event_type_validation(self):
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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_withdrawal_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities, 5)

        self.assertEqual(test_time, participant_event_activities.created)
        self.assertEqual(test_time, participant_event_activities.modified)
        self.assertEqual(participant.id, participant_event_activities.participant_id)
        self.assertEqual(payload, participant_event_activities.resource)
        self.assertEqual(5, participant_event_activities.activity_id)

        withdrawal_events = self.withdrawal_event_dao.get_all()
        self.assertEqual(2, len(withdrawal_events))
        self.assertEqual(test_time, withdrawal_events[0].created)
        self.assertEqual(test_time, withdrawal_events[0].modified)
        self.assertEqual(participant_event_activities.id, withdrawal_events[0].event_id)
        self.assertEqual(participant.id, withdrawal_events[0].participant_id)
        self.assertEqual('Withdrawal', withdrawal_events[0].event_type_name)
        self.assertEqual('activity_status', withdrawal_events[0].data_element_name)
        self.assertEqual('withdrawn', withdrawal_events[0].data_element_value)

        self.assertEqual(test_time, withdrawal_events[1].created)
        self.assertEqual(test_time, withdrawal_events[1].modified)
        self.assertEqual(participant_event_activities.id, withdrawal_events[1].event_id)
        self.assertEqual(participant.id, withdrawal_events[1].participant_id)
        self.assertEqual('Withdrawal', withdrawal_events[1].event_type_name)
        self.assertEqual('activity_date_time', withdrawal_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00.000Z", withdrawal_events[1].data_element_value)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_deactivation_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities,6)
        self.assertEqual(test_time, participant_event_activities.created)
        self.assertEqual(test_time, participant_event_activities.modified)
        self.assertEqual(participant.id, participant_event_activities.participant_id)
        self.assertEqual(payload, participant_event_activities.resource)
        self.assertEqual(6, participant_event_activities.activity_id)

        deactivation_events = self.deactivation_event_dao.get_all()
        self.assertEqual(2, len(deactivation_events))
        self.assertEqual(test_time, deactivation_events[0].created)
        self.assertEqual(test_time, deactivation_events[0].modified)
        self.assertEqual(participant_event_activities.id, deactivation_events[0].event_id)
        self.assertEqual(participant.id, deactivation_events[0].participant_id)
        self.assertEqual('Deactivation', deactivation_events[0].event_type_name)
        self.assertEqual('activity_status', deactivation_events[0].data_element_name)
        self.assertEqual('deactivated', deactivation_events[0].data_element_value)

        self.assertEqual(test_time, deactivation_events[1].created)
        self.assertEqual(test_time, deactivation_events[1].modified)
        self.assertEqual(participant_event_activities.id, deactivation_events[1].event_id)
        self.assertEqual(participant.id, deactivation_events[1].participant_id)
        self.assertEqual('Deactivation', deactivation_events[1].event_type_name)
        self.assertEqual('activity_date_time', deactivation_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00.000Z", deactivation_events[1].data_element_value)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_participant_status_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities, 7)

        self.assertEqual(test_time, participant_event_activities.created)
        self.assertEqual(test_time, participant_event_activities.modified)
        self.assertEqual(participant.id, participant_event_activities.participant_id)
        self.assertEqual(payload, participant_event_activities.resource)
        self.assertEqual(7, participant_event_activities.activity_id)

        participant_status_events = self.participant_status_event_dao.get_all()
        self.assertEqual(2, len(participant_status_events))
        self.assertEqual(test_time, participant_status_events[0].created)
        self.assertEqual(test_time, participant_status_events[0].modified)
        self.assertEqual(participant_event_activities.id, participant_status_events[0].event_id)
        self.assertEqual(participant.id, participant_status_events[0].participant_id)
        self.assertEqual('Participant Status', participant_status_events[0].event_type_name)
        self.assertEqual('activity_status', participant_status_events[0].data_element_name)
        self.assertEqual('not_test', participant_status_events[0].data_element_value)

        self.assertEqual(test_time, participant_status_events[1].created)
        self.assertEqual(test_time, participant_status_events[1].modified)
        self.assertEqual(participant_event_activities.id, participant_status_events[1].event_id)
        self.assertEqual(participant.id, participant_status_events[1].participant_id)
        self.assertEqual('Participant Status', participant_status_events[1].event_type_name)
        self.assertEqual('activity_date_time', participant_status_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00.000Z", participant_status_events[1].data_element_value)

    def test_intake_site_attribution_event_type_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Attribution",
            "eventType": "Pepperoni",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "site_name",
                    "dataElementValue": "test-site-1"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_morehouse_attribution_payload_is_updated(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

        payload = {
            "activity": "Attribution",
            "eventType": "Org Attribution",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "SEEC_MOREHOUSE"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2025-05-20T12:38:00.000Z"
                },
            ]
        }

        test_time = datetime(2025, 5, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        original_org = payload.get("dataElements")[0].get("dataElementValue")
        # Verify that sent data stored in the participant_even_activities as is
        self.assertEqual(original_org, participant_event_activities[1].resource["dataElements"][0]["dataElementValue"])

        attribution_events = self.attribution_event_dao.get_all()
        self.assertEqual('Org Attribution', attribution_events[0].event_type_name)
        # Verify value is transformed
        self.assertEqual('DREF_MOREHOUSE', attribution_events[0].data_element_value)



    def test_intake_attribution_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

        payload = {
            "activity": "Attribution",
            "eventType": "Org Attribution",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "site_name",
                    "dataElementValue": "test-site-1"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities, 8)

        self.assertEqual(test_time, participant_event_activities.created)
        self.assertEqual(test_time, participant_event_activities.modified)
        self.assertEqual(participant.id, participant_event_activities.participant_id)
        self.assertEqual(payload, participant_event_activities.resource)
        self.assertEqual(8, participant_event_activities.activity_id)

        attribution_events = self.attribution_event_dao.get_all()
        self.assertEqual(2, len(attribution_events))
        self.assertEqual(test_time, attribution_events[0].created)
        self.assertEqual(test_time, attribution_events[0].modified)
        self.assertEqual(participant_event_activities.id, attribution_events[0].event_id)
        self.assertEqual(participant.id, attribution_events[0].participant_id)
        self.assertEqual('Org Attribution', attribution_events[0].event_type_name)
        self.assertEqual('site_name', attribution_events[0].data_element_name)
        self.assertEqual('test-site-1', attribution_events[0].data_element_value)

        self.assertEqual(test_time, attribution_events[1].created)
        self.assertEqual(test_time, attribution_events[1].modified)
        self.assertEqual(participant_event_activities.id, attribution_events[1].event_id)
        self.assertEqual(participant.id, attribution_events[1].participant_id)
        self.assertEqual('Org Attribution', attribution_events[1].event_type_name)
        self.assertEqual('activity_date_time', attribution_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00.000Z", attribution_events[1].data_element_value)

    def test_intake_nph_opt_in_event_type_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "NPH Opt In",
            "eventType": "Pepperoni",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_yes,"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_nph_opt_in_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

        payload = {
            "activity": "NPH Opt In",
            "eventType": "NPH Opt In",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_yes"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities, 9)

        self.assertEqual(test_time, participant_event_activities.created)
        self.assertEqual(test_time, participant_event_activities.modified)
        self.assertEqual(participant.id, participant_event_activities.participant_id)
        self.assertEqual(payload, participant_event_activities.resource)
        self.assertEqual(9, participant_event_activities.activity_id)

        nph_opt_in_events = self.nph_opt_in_event_dao.get_all()
        self.assertEqual(2, len(nph_opt_in_events))
        self.assertEqual(test_time, nph_opt_in_events[0].created)
        self.assertEqual(test_time, nph_opt_in_events[0].modified)
        self.assertEqual(participant_event_activities.id, nph_opt_in_events[0].event_id)
        self.assertEqual(participant.id, nph_opt_in_events[0].participant_id)
        self.assertEqual('NPH Opt In', nph_opt_in_events[0].event_type_name)
        self.assertEqual('activity_status', nph_opt_in_events[0].data_element_name)
        self.assertEqual('submitted_yes', nph_opt_in_events[0].data_element_value)

        self.assertEqual(test_time, nph_opt_in_events[1].created)
        self.assertEqual(test_time, nph_opt_in_events[1].modified)
        self.assertEqual(participant_event_activities.id, nph_opt_in_events[1].event_id)
        self.assertEqual(participant.id, nph_opt_in_events[1].participant_id)
        self.assertEqual('NPH Opt In', nph_opt_in_events[1].event_type_name)
        self.assertEqual('activity_date_time', nph_opt_in_events[1].data_element_name)
        self.assertEqual("2024-05-20T14:30:00.000Z", nph_opt_in_events[1].data_element_value)

    def test_intake_account_linkage_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

        payload = {
            "activity": "Account Linkage",
            "eventType": "Relationship Type",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "relationship_type",
                    "dataElementValue": "Child"
                },
                {
                    "dataElementName": "relationship_participant_id",
                    "dataElementValue": "P129821040"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2025-02-03T20:55:28.079Z"
                },
            ]
        }
        self.send_post('Intake', request_data=payload)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities, 10)

        account_linkage_activity = self.session.query(Activity).filter(Activity.name == 'Account Linkage').one()
        self.assertEqual(account_linkage_activity.id, participant_event_activities.activity_id)

        account_linkage_events = self.account_linkage_event_dao.get_all()
        self.assertEqual(3, len(account_linkage_events))

        for index, (name, value) in enumerate([
            ['relationship_type', 'Child'],
            ['relationship_participant_id', 'P129821040'],
            ['activity_date_time', '2025-02-03T20:55:28.079Z']
        ]):
            event_record = account_linkage_events[index]
            self.assertEqual(participant.id, event_record.participant_id)
            self.assertEqual('Relationship Type', event_record.event_type_name)
            self.assertEqual(name, event_record.data_element_name)
            self.assertEqual(value, event_record.data_element_value)

    def test_intake_consent_validation(self):
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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }
        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertTrue(response is not None)

    def test_intake_enrollment_status_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()

        payload = {
            "activity": "Participant Status",
            "eventType": "Enrollment Status",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "registered",
                    "dataElementValue": "yes"
                },
            ]
        }

        response = self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)
        self.assertEqual(response.status_code, 400)

    def test_intake_enrollment_status_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

        payload = {
            "activity": "Participant Status",
            "eventType": "Enrollment Status",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "registered",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "registered_date_time",
                    "dataElementValue": "2024-04-21T18:06:04.356Z"
                },
                {
                    "dataElementName": "participant",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "participant_date_time",
                    "dataElementValue": "2024-10-28T19:20:42.000Z"
                },
                {
                    "dataElementName": "participant_ehr_consent",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "participant_ehr_consent_date_time",
                    "dataElementValue": "2024-10-28T19:20:42.000Z"
                },
                {
                    "dataElementName": "outdoors",
                    "dataElementValue": "yes"
                },
                {
                    "dataElementName": "outdoors_date_time",
                    "dataElementValue": "2024-10-31T19:20:42.000Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities, 7)

        self.assertEqual(test_time, participant_event_activities.created)
        self.assertEqual(test_time, participant_event_activities.modified)
        self.assertEqual(participant.id, participant_event_activities.participant_id)
        self.assertEqual(payload, participant_event_activities.resource)
        self.assertEqual(7, participant_event_activities.activity_id)

        participant_status_events = self.participant_status_event_dao.get_all()
        self.assertEqual(8, len(participant_status_events))
        self.assertEqual(test_time, participant_status_events[0].created)
        self.assertEqual(test_time, participant_status_events[0].modified)
        self.assertEqual(participant_event_activities.id, participant_status_events[0].event_id)
        self.assertEqual(participant.id, participant_status_events[0].participant_id)
        self.assertEqual('Enrollment Status', participant_status_events[0].event_type_name)
        self.assertEqual('registered', participant_status_events[0].data_element_name)
        self.assertEqual('yes', participant_status_events[0].data_element_value)

        self.assertEqual('registered_date_time', participant_status_events[1].data_element_name)
        self.assertEqual('2024-04-21T18:06:04.356Z', participant_status_events[1].data_element_value)

        self.assertEqual('participant', participant_status_events[2].data_element_name)
        self.assertEqual('yes', participant_status_events[2].data_element_value)

        self.assertEqual('participant_date_time', participant_status_events[3].data_element_name)
        self.assertEqual('2024-10-28T19:20:42.000Z', participant_status_events[3].data_element_value)

        self.assertEqual('participant_ehr_consent', participant_status_events[4].data_element_name)
        self.assertEqual('yes', participant_status_events[4].data_element_value)

        self.assertEqual('participant_ehr_consent_date_time', participant_status_events[5].data_element_name)
        self.assertEqual('2024-10-28T19:20:42.000Z', participant_status_events[5].data_element_value)

        self.assertEqual('outdoors', participant_status_events[6].data_element_name)
        self.assertEqual('yes', participant_status_events[6].data_element_value)

        self.assertEqual('outdoors_date_time', participant_status_events[7].data_element_name)
        self.assertEqual('2024-10-31T19:20:42.000Z', participant_status_events[7].data_element_value)

    def test_intake_ubr_status_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

        payload = {
             "activity": "Participant Status",
             "eventType": "UBR Status",
             "participantId": f"P{participant.id}",
             "dataElements": [
               {
                 "dataElementName": "ubr_overall",
                 "dataElementValue": "UBR"
               },
               {
                 "dataElementName": "ubr_geography",
                 "dataElementValue": "RBR"
               },
               {
                 "dataElementName": "ubr_healthcare_access_and_utilization",
                 "dataElementValue": "Unknown"
               },
               {
                 "dataElementName": "ubr_racial_identity",
                 "dataElementValue": "UBR"
               },
               {
                 "dataElementName": "ubr_gender_identity",
                 "dataElementValue": "RBR"
               },
               {
                 "dataElementName": "ubr_sex_at_birth",
                 "dataElementValue": "RBR"
               },
             ]
            }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities, 7)

        self.assertEqual(test_time, participant_event_activities.created)
        self.assertEqual(test_time, participant_event_activities.modified)
        self.assertEqual(participant.id, participant_event_activities.participant_id)
        self.assertEqual(payload, participant_event_activities.resource)
        self.assertEqual(7, participant_event_activities.activity_id)

        participant_status_events = self.participant_status_event_dao.get_all()
        self.assertEqual(6, len(participant_status_events))
        self.assertEqual(test_time, participant_status_events[0].created)
        self.assertEqual(test_time, participant_status_events[0].modified)
        self.assertEqual(participant_event_activities.id, participant_status_events[0].event_id)
        self.assertEqual(participant.id, participant_status_events[0].participant_id)
        self.assertEqual('UBR Status', participant_status_events[0].event_type_name)
        self.assertEqual('ubr_overall', participant_status_events[0].data_element_name)
        self.assertEqual('UBR', participant_status_events[0].data_element_value)

        self.assertEqual('ubr_geography', participant_status_events[1].data_element_name)
        self.assertEqual('RBR', participant_status_events[1].data_element_value)

        self.assertEqual('ubr_healthcare_access_and_utilization', participant_status_events[2].data_element_name)
        self.assertEqual('Unknown', participant_status_events[2].data_element_value)

        self.assertEqual('ubr_racial_identity', participant_status_events[3].data_element_name)
        self.assertEqual('UBR', participant_status_events[3].data_element_value)

        self.assertEqual('ubr_gender_identity', participant_status_events[4].data_element_name)
        self.assertEqual('RBR', participant_status_events[4].data_element_value)

        self.assertEqual('ubr_sex_at_birth', participant_status_events[5].data_element_name)
        self.assertEqual('RBR', participant_status_events[5].data_element_value)

    def test_intake_retention_status_insert(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

        payload = {
         "activity": "Participant Status",
         "eventType": "Retention Status",
         "participantId": f"P{participant.id}",
         "dataElements": [
          {
           "dataElementName": "activity_date_time",
           "dataElementValue": "2020-04-17T19:00:00.000Z"
          },
          {
           "dataElementName": "activity_status",
           "dataElementValue": "eligible"
          },
          {
           "dataElementName": "retention_type",
           "dataElementValue": "Active"
          },
          {
           "dataElementName": "last_retention_activity_date_time",
           "dataElementValue": "2020-04-17T19:00:00.000Z"
          },
         ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

        participant_event_activities = self.ppsc_participant_activity_dao.get_all()
        participant_event_activities = self.filter_events_by_type(participant_event_activities, 7)

        self.assertEqual(test_time, participant_event_activities.created)
        self.assertEqual(test_time, participant_event_activities.modified)
        self.assertEqual(participant.id, participant_event_activities.participant_id)
        self.assertEqual(payload, participant_event_activities.resource)
        self.assertEqual(7, participant_event_activities.activity_id)

        participant_status_events = self.participant_status_event_dao.get_all()
        self.assertEqual(4, len(participant_status_events))
        self.assertEqual(test_time, participant_status_events[0].created)
        self.assertEqual(test_time, participant_status_events[0].modified)
        self.assertEqual(participant_event_activities.id, participant_status_events[0].event_id)
        self.assertEqual(participant.id, participant_status_events[0].participant_id)
        self.assertEqual('Retention Status', participant_status_events[0].event_type_name)
        self.assertEqual('activity_date_time', participant_status_events[0].data_element_name)
        self.assertEqual('2020-04-17T19:00:00.000Z', participant_status_events[0].data_element_value)

        self.assertEqual('activity_status', participant_status_events[1].data_element_name)
        self.assertEqual('eligible', participant_status_events[1].data_element_value)

        self.assertEqual('retention_type', participant_status_events[2].data_element_name)
        self.assertEqual('Active', participant_status_events[2].data_element_value)

        self.assertEqual('last_retention_activity_date_time', participant_status_events[3].data_element_name)
        self.assertEqual('2020-04-17T19:00:00.000Z', participant_status_events[3].data_element_value)

    def test_intake_pediatric_permission_allowed(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant, consent_type="Pediatric Permission", status="submitted_yes")

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
                {
                    "dataElementName": "piibirthinformation_birthdate",
                    "dataElementValue": "2020-01-01"
                }
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

    def test_pediatric_survey_with_missing_age_group_permission(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(
            participant,
            consent_type="Pediatric Permission",
            status="submitted_yes",
            age_group='0-6'
        )

        payload = {
            "activity": "Survey Completion",
            "eventType": "Overall Health",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_complete"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
                {
                    "dataElementName": "age_group",
                    "dataElementValue": "7-12"
                }
            ]
        }

        self.send_post('Intake', request_data=payload, expected_status=422)

    def test_pediatric_survey_with_correct_age_group_permission(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(
            participant,
            consent_type="Pediatric Permission",
            status="Yes",
            age_group='7-12'
        )

        payload = {
            "activity": "Survey Completion",
            "eventType": "Overall Health",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_complete"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
                {
                    "dataElementName": "age_group",
                    "dataElementValue": "7-12"
                }
            ]
        }

        self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

    def test_pediatric_survey_with_na_assent(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(
            participant,
            consent_type="Pediatric Permission",
            status="Yes",
            age_group='7-12'
        )
        self.send_pediatric_assent(participant, 'N/A')

        payload = {
            "activity": "Survey Completion",
            "eventType": "Overall Health",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_complete"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
                {
                    "dataElementName": "age_group",
                    "dataElementValue": "7-12"
                }
            ]
        }

        self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

    def test_pediatric_survey_with_no_assent(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(
            participant,
            consent_type="Pediatric Permission",
            status="Yes",
            age_group='7-12'
        )
        self.send_pediatric_assent(participant, 'No')

        payload = {
            "activity": "Survey Completion",
            "eventType": "Overall Health",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_complete"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
                {
                    "dataElementName": "age_group",
                    "dataElementValue": "7-12"
                }
            ]
        }

        self.send_post('Intake', request_data=payload, expected_status=422)

    def test_pediatric_survey_with_yes_assent_followed_by_no(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(
            participant,
            consent_type="Pediatric Permission",
            status="Yes",
            age_group='7-12'
        )
        self.send_pediatric_assent(participant, 'No', authored_timestamp=datetime(2020, 1, 7))
        self.send_pediatric_assent(participant, 'Yes', authored_timestamp=datetime(2020, 1, 5))

        payload = {
            "activity": "Survey Completion",
            "eventType": "Overall Health",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_complete"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
                {
                    "dataElementName": "age_group",
                    "dataElementValue": "7-12"
                }
            ]
        }

        self.send_post('Intake', request_data=payload, expected_status=422)

    def test_pediatric_survey_with_no_assent_followed_by_yes(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(
            participant,
            consent_type="Pediatric Permission",
            status="Yes",
            age_group='7-12'
        )
        self.send_pediatric_assent(participant, 'Yes', authored_timestamp=datetime(2020, 1, 7))
        self.send_pediatric_assent(participant, 'No', authored_timestamp=datetime(2020, 1, 5))

        payload = {
            "activity": "Survey Completion",
            "eventType": "Overall Health",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "submitted_complete"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
                {
                    "dataElementName": "age_group",
                    "dataElementValue": "7-12"
                }
            ]
        }

        self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

    def test_intake_test_account_allowed(self):
        participant = self.ppsc_data_gen.create_database_participant()
        payload = {
            "activity": "Participant Status",
            "eventType": "Test Flag",
            "participantId": f"P{participant.id}",
            "dataElements": [
                {
                    "dataElementName": "activity_status",
                    "dataElementValue": "test"
                },
                {
                    "dataElementName": "activity_date_time",
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                },
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.OK)

    def test_requests_log_generation(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

        # checking for the log in the Requests Log and verifying the fpk info
        log_entry = self.session.query(RequestsLog).order_by(
            RequestsLog.id.desc()
        ).first()
        self.assertIsNotNone(log_entry, "No log entry found in the requests log table")
        self.assertEqual(log_entry.participantId, 100000000)
        self.assertEqual(log_entry.fpk_table, "participant_event_activity")
        self.assertEqual(log_entry.fpk_column, "id")
        self.assertIsNotNone(log_entry.fpk_id)

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
        self.clear_table_after_test("ppsc.attribution_event")

    def test_intake_date_of_birth_validation(self):
        participant = self.ppsc_data_gen.create_database_participant()
        self.send_valid_primary_consent(participant)

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
                    "dataElementValue": "2024-05-20T14:30:00.000Z"
                }
            ]
        }

        test_time = datetime(2024, 6, 25, 12, 1)
        with clock.FakeClock(test_time):
            self.send_post('Intake', request_data=payload, expected_status=http.client.BAD_REQUEST)

