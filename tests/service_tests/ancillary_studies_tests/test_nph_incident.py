from unittest.mock import Mock, patch
from typing import Dict, Any, Optional
from datetime import datetime
from uuid import uuid4
from faker import Faker

from tests.helpers.unittest_base import BaseTestCase
from rdr_service.clock import FakeClock
from rdr_service.dao.study_nph_dao import (
    NphParticipantDao, NphActivityDao,
    NphParticipantEventActivityDao,
    NphIncidentDao,
)
from rdr_service.model.study_nph import (
    Participant, Activity, ParticipantEventActivity, Incident
)
from rdr_service.services.ancillary_studies.nph_incident import create_nph_incident


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)


class TestNphIncident(BaseTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.nph_participant_dao = NphParticipantDao()
        self.nph_activity_dao = NphActivityDao()
        self.nph_participant_event_activity_dao = NphParticipantEventActivityDao()
        self.nph_incident_dao = NphIncidentDao()
        self.faker = Faker()

    def _create_nph_participant(self, participant_obj: Dict[str, Any]) -> Participant:
        nph_participant = Participant(**participant_obj)
        with FakeClock(TIME):
            return self.nph_participant_dao.insert(nph_participant)

    def _create_nph_activity(self, activity_obj: Dict[str, Any]) -> Activity:
        nph_activity = Activity(**activity_obj)
        with FakeClock(TIME):
            return self.nph_activity_dao.insert(nph_activity)

    def _create_nph_participant_event_activity(self, obj: Dict[str, Any]) -> ParticipantEventActivity:
        nph_participant_event_activity = ParticipantEventActivity(**obj)
        with FakeClock(TIME):
            return self.nph_participant_event_activity_dao.insert(nph_participant_event_activity)

    def test_create_nph_incident(self):
        participant_obj_params = {
            "ignore_flag": 0,
            "disable_flag": 0,
            "disable_reason": "N/A",
            "biobank_id": 1E7,
            "research_id": 1E7
        }
        nph_participant = self._create_nph_participant(participant_obj_params)
        nph_activity_obj_params = {
            "ignore_flag": 0,
            "name": "sample activity",
            "rdr_note": "sample rdr note",
            "rule_codes": None,
        }
        nph_activity = self._create_nph_activity(nph_activity_obj_params)

        nph_participant_event_activity_obj_params = {
            "ignore_flag": 0,
            "participant_id": nph_participant.id,
            "activity_id": nph_activity.id,
            "resource": None,
        }
        nph_participant_event_activity = (
            self._create_nph_participant_event_activity(nph_participant_event_activity_obj_params)
        )
        notification_ts = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)
        mock_trace_id = str(uuid4())
        mock_dev_note = ''.join(self.faker.random_letters(length=1024))
        mock_message = ''.join(self.faker.random_letters(length=1024))
        nph_incident_kwargs = {
            "dev_note": mock_dev_note,
            "message": mock_message,
            "notification_date": notification_ts,
            "participant_id": nph_participant.id,
            "event_id": nph_participant_event_activity.id,
            "trace_id": mock_trace_id,
            "save_incident": True,
        }
        create_nph_incident(**nph_incident_kwargs)
        with self.nph_participant_dao.session() as session:
            self.assertIsNotNone(session.query(Incident).first())

    @patch("rdr_service.services.ancillary_studies.nph_incident.SlackMessageHandler.send_message_to_webhook")
    def test_create_nph_incident_can_send_slack_msg_using_webhook(self, mock_send_message_to_webhook: Mock):
        mock_send_message_to_webhook.return_value = True
        participant_obj_params = {
            "ignore_flag": 0,
            "disable_flag": 0,
            "disable_reason": "N/A",
            "biobank_id": 1E7,
            "research_id": 1E7
        }
        nph_participant = self._create_nph_participant(participant_obj_params)
        nph_activity_obj_params = {
            "ignore_flag": 0,
            "name": "sample activity",
            "rdr_note": "sample rdr note",
            "rule_codes": None,
        }
        nph_activity = self._create_nph_activity(nph_activity_obj_params)

        nph_participant_event_activity_obj_params = {
            "ignore_flag": 0,
            "participant_id": nph_participant.id,
            "activity_id": nph_activity.id,
            "resource": None,
        }
        nph_participant_event_activity = (
            self._create_nph_participant_event_activity(nph_participant_event_activity_obj_params)
        )
        notification_ts = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)
        mock_trace_id = str(uuid4())
        mock_dev_note = ''.join(self.faker.random_letters(length=1024))
        mock_message = ''.join(self.faker.random_letters(length=1024))
        nph_incident_kwargs = {
            "dev_note": mock_dev_note,
            "message": mock_message,
            "notification_date": notification_ts,
            "participant_id": nph_participant.id,
            "event_id": nph_participant_event_activity.id,
            "trace_id": mock_trace_id,
            "save_incident": True,
            "slack": True
        }
        with FakeClock(TIME):
            create_nph_incident(**nph_incident_kwargs)

        with self.nph_participant_dao.session() as session:
            incident: Optional[Incident] = session.query(Incident).first()

        self.assertIsNotNone(incident)
        self.assertEqual(incident.notification_sent_flag, 1)
        self.assertEqual(incident.notification_date, TIME)

    def tearDown(self):
        self.clear_table_after_test("nph.incident")
        self.clear_table_after_test("nph.participant_event_activity")
        self.clear_table_after_test("nph.activity")
        self.clear_table_after_test("nph.participant")
