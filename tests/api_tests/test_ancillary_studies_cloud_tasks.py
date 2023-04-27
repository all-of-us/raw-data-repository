import mock
from dateutil import parser

from rdr_service.dao.study_nph_dao import NphEnrollmentEventDao
from rdr_service.dao.rex_dao import ParticipantMapping
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.data_gen.generators.nph import NphDataGenerator
from tests.helpers.unittest_base import BaseTestCase


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
