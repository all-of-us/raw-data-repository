from collections import namedtuple
from datetime import  date, datetime
import mock

from rdr_service.dao.deceased_report_dao import DeceasedReportDao
from rdr_service.participant_enums import DeceasedStatus, DeceasedReportDenialReason, DeceasedReportStatus
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.tools.tool_libs.resurrect import ResurrectClass, PMI_OPS_URL
from tests.helpers.unittest_base import BaseTestCase

FakeFile = namedtuple('FakeFile', ['name', 'updated'])


class ResurrectTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao = DeceasedReportDao()
        self.deceased_participant_id = self.initialize_deceased_participant()

    @staticmethod
    def run_tool(participant_id, reason=DeceasedReportDenialReason.MARKED_IN_ERROR, reason_description=None):
        environment = mock.MagicMock()
        environment.project = 'unit_test'
        environment.account = 'resurrecting-username'

        args = mock.MagicMock(spec=['pid', 'reason', 'reason-desc'])
        args.pid = participant_id
        args.reason = reason
        args.reason_desc = reason_description

        resurrect_tool = ResurrectClass(args, environment)
        resurrect_tool.run()

    def initialize_deceased_participant(self):
        participant = self.data_generator.create_database_participant()
        self.data_generator.create_database_participant_summary(
            participantId=participant.participantId,
            deceasedStatus=DeceasedStatus.APPROVED,
            deceasedAuthored=datetime(2020, 1, 1),
            dateOfDeath=date(2020, 1, 1)
        )
        external_user = self.data_generator.create_database_api_user(
            system='external-url',
            username='external-user'
        )
        self.data_generator.create_database_deceased_report(
            participantId=participant.participantId,
            status=DeceasedReportStatus.APPROVED,
            reviewerId=external_user.id
        )

        return participant.participantId

    def retrieve_object_from_database(self, model_class, participant_id):
        # Resetting the session since it would have cached the summary or deceased report
        self.session.commit()
        self.session.close()

        return self.session.query(model_class).filter(
            model_class.participantId == participant_id
        ).one()

    def test_resurrecting_participant(self):
        self.run_tool(self.deceased_participant_id)

        participant_summary = self.retrieve_object_from_database(ParticipantSummary, self.deceased_participant_id)
        self.assertEqual(DeceasedStatus.UNSET, participant_summary.deceasedStatus)
        self.assertIsNone(participant_summary.deceasedAuthored)
        self.assertIsNone(participant_summary.dateOfDeath)

        deceased_report = self.retrieve_object_from_database(DeceasedReport, self.deceased_participant_id)
        self.assertEqual(DeceasedReportStatus.DENIED, deceased_report.status)
        self.assertEqual(PMI_OPS_URL, deceased_report.reviewer.system)
        self.assertEqual('resurrecting-username', deceased_report.reviewer.username)

    def test_other_denial_reason_description(self):
        self.run_tool(self.deceased_participant_id, reason=DeceasedReportDenialReason.OTHER, reason_description='test')

        deceased_report = self.retrieve_object_from_database(DeceasedReport, self.deceased_participant_id)
        self.assertEqual('test', deceased_report.denialReasonOther)

    def test_error_on_missing_other_description(self):
        with self.assertRaises(Exception):
            self.run_tool(self.deceased_participant_id, reason=DeceasedReportDenialReason.OTHER)

    def test_error_on_modifying_report_not_approved(self):
        report = self.retrieve_object_from_database(DeceasedReport, self.deceased_participant_id)
        report.status = DeceasedReportStatus.PENDING
        self.session.commit()

        with self.assertRaises(Exception):
            self.run_tool(self.deceased_participant_id)
