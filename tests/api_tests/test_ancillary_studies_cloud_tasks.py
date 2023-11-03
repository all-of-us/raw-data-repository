import mock
from typing import Dict, Any
from datetime import datetime
from uuid import uuid4
from dateutil import parser
from http.client import INTERNAL_SERVER_ERROR, OK

from rdr_service.dao.study_nph_dao import NphEnrollmentEventDao
from rdr_service.dao.rex_dao import ParticipantMapping
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.data_gen.generators.nph import NphDataGenerator
from tests.helpers.unittest_base import BaseTestCase

from rdr_service.dao.study_nph_dao import NphIncidentDao


class AncillaryStudiesEnrollmentCloudTaskTest(BaseTestCase):
    def setUp(self):
        super(AncillaryStudiesEnrollmentCloudTaskTest, self).setUp()
        self.nph_datagen = NphDataGenerator()

    def test_insert_study_event_task(self):
        self.nph_datagen.create_database_participant(id=123123123)
        self.nph_datagen.create_database_activity(id=1, name="ENROLLMENT")
        self.nph_datagen.create_database_enrollment_event_type(name="REFERRED")

        data = {
            "study": "nph",
            "activity_id": 1,
            "event_type_id": 1,
            "participant_id": 123123123,
            "event_authored_time": "2023-01-18 20:03:54"
        }

        from rdr_service.resource import main as resource_main

        self.send_post(
            local_path='InsertStudyEventTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        enrollment_event_dao = NphEnrollmentEventDao()
        event = enrollment_event_dao.get(1)
        self.assertEqual(event.event_type_id, 1)
        self.assertEqual(event.participant_id, 123123123)

    @mock.patch('rdr_service.dao.rex_dao.RexParticipantMappingDao.get_from_ancillary_id')
    def test_update_participant_summary_for_nph_consent(self, mock_participant_mapping):
        aou_participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(participant=aou_participant)
        mock_participant_mapping.return_value = ParticipantMapping(primary_participant_id=aou_participant.participantId)

        authored_time = '2023-03-01T14:13:12'
        data = {
            'event_type': 'consent',
            'participant_id': 123321654,
            'event_authored_time': authored_time
        }

        from rdr_service.resource import main as resource_main

        self.send_post(
            local_path='UpdateParticipantSummaryForNphTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        ps_dao = ParticipantSummaryDao()

        ps = ps_dao.get_by_participant_id(aou_participant.participantId)

        self.assertTrue(ps.consentForNphModule1)
        self.assertEqual(parser.parse(authored_time),ps.consentForNphModule1Authored)

    @mock.patch('rdr_service.dao.rex_dao.RexParticipantMappingDao.get_from_ancillary_id')
    def test_update_participant_summary_for_nph_withdrawal(self, mock_participant_mapping):
        aou_participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(participant=aou_participant)
        mock_participant_mapping.return_value = ParticipantMapping(primary_participant_id=aou_participant.participantId)

        authored_time = '2023-03-01T14:13:12'
        data = {
            'event_type': 'withdrawal',
            'participant_id': 123321654,
            'event_authored_time': authored_time
        }

        from rdr_service.resource import main as resource_main

        self.send_post(
            local_path='UpdateParticipantSummaryForNphTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        ps_dao = ParticipantSummaryDao()
        ps = ps_dao.get_by_participant_id(aou_participant.participantId)
        self.assertTrue(ps.nphWithdrawal)
        self.assertEqual(parser.parse(authored_time), ps.nphWithdrawalAuthored)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("rex.participant_mapping")
        self.clear_table_after_test("rex.study")
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.activity")
        self.clear_table_after_test("nph.enrollment_event_type")
        self.clear_table_after_test("nph.participant_event_activity")
        self.clear_table_after_test("nph.enrollment_event")


class NphIncidentTaskApiCloudTaskTest(BaseTestCase):

    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    TIME = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)

    def setUp(self) -> None:
        super().setUp()
        self.nph_incident_dao = NphIncidentDao()
        self.nph_data_generator = NphDataGenerator()

    def _get_nph_incident_task_payload(self) -> Dict[str, Any]:
        participant_obj_params = {
            "ignore_flag": 0,
            "disable_flag": 0,
            "disable_reason": "N/A",
            "biobank_id": 1E7,
            "research_id": 1E7
        }
        nph_participant = self.nph_data_generator.create_database_participant(**participant_obj_params)
        nph_activity_obj_params = {
            "ignore_flag": 0,
            "name": "sample activity",
            "rdr_note": "sample rdr note",
            "rule_codes": None,
        }
        nph_activity = self.nph_data_generator.create_database_activity(**nph_activity_obj_params)

        nph_participant_event_activity_obj_params = {
            "ignore_flag": 0,
            "participant_id": nph_participant.id,
            "activity_id": nph_activity.id,
            "resource": None,
        }
        nph_participant_event_activity = (
            self.nph_data_generator.create_database_participant_event_activity(
                **nph_participant_event_activity_obj_params
            )
        )
        notification_ts = datetime.now().strftime(self.DATETIME_FORMAT)
        mock_trace_id = str(uuid4())
        mock_dev_note = ''.join(self.fake.random_letters(length=1024))
        mock_message = ''.join(self.fake.random_letters(length=1024))
        nph_incident_kwargs = {
            "dev_note": mock_dev_note,
            "message": mock_message,
            "notification_date": notification_ts,
            "participant_id": nph_participant.id,
            "event_id": nph_participant_event_activity.id,
            "trace_id": mock_trace_id,
        }
        return nph_incident_kwargs

    def test_nph_incident_task_returns_500(self):
        nph_incident_kwargs = {
            "dev_note": "dev_note",
            "message": "mock_message",
            "participant_id": 1,
            "event_id": 1,
            "trace_id": 1234,
        }
        from rdr_service.resource import main as resource_main
        response = self.send_post(
            local_path='NphIncidentTaskApi',
            request_data=nph_incident_kwargs,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
            expected_status=INTERNAL_SERVER_ERROR
        )
        self.assertEqual(response.status_code, INTERNAL_SERVER_ERROR)

    @mock.patch("rdr_service.services.ancillary_studies.nph_incident.SlackMessageHandler.send_message_to_webhook")
    def test_nph_withdrawn_pid_notifier_task_returns_200(self, mock_send_message_to_webhook: mock.Mock):
        mock_send_message_to_webhook.return_value = True
        from rdr_service.resource import main as resource_main
        response = self.send_post(
            local_path='WithdrawnParticipantNotifierTaskApi',
            request_data={"withdrawn_pids": ["1", "2"]},
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
            expected_status=OK
        )
        self.assertEqual(response, {'success': True})

    def tearDown(self):
        self.clear_table_after_test("nph.incident")
        self.clear_table_after_test("nph.participant_event_activity")
        self.clear_table_after_test("nph.activity")
        self.clear_table_after_test("nph.participant")
