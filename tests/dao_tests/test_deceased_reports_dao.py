
from rdr_service.config import DECEASED_REPORT_FILTER_EXCEPTIONS
from rdr_service.dao.deceased_report_dao import DeceasedReportDao
from rdr_service.participant_enums import DeceasedReportStatus, WithdrawalStatus
from tests.helpers.unittest_base import BaseTestCase


class DeceasedReportDaoTest(BaseTestCase):
    def test_report_exceptions(self):
        """
        Some deceased participants have been inadvertently withdrawn. They have pending deceased reports, but since
        they're withdrawn the reports don't display for approval. In order to get the pending reports approved,
        and let us remove the withdrawal status without making the participant appear contactable, we needed a way
        of making the pending reports visible even though the participants are withdrawn.
        """

        # Create a withdrawn participant with a report that is pending
        participant = self.data_generator.create_database_participant(
            withdrawalStatus=WithdrawalStatus.NO_USE
        )
        expected_report = self.data_generator.create_database_deceased_report(
            participantId=participant.participantId,
            status=DeceasedReportStatus.PENDING
        )

        # Set up participant as an exception to the report filters
        self.temporarily_override_config_setting(DECEASED_REPORT_FILTER_EXCEPTIONS, [participant.participantId])

        # Load the deceased reports and verify that the pending report is visible
        dao = DeceasedReportDao()
        actual_reports = dao.load_reports(participant_id=participant.participantId)
        self.assertNotEmpty(actual_reports)
        self.assertEqual(expected_report.id, actual_reports[0].id)
