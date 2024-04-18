from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.tool_test_mixin import ToolTestMixin
from rdr_service.tools.tool_libs.retention_metrics import RetentionRecalcClass
from datetime import datetime


class RetentionMetricsTest(BaseTestCase, ToolTestMixin):
    """
    A test for the retention metrics tool
    """

    def setUp(self):
        super().setUp(with_data=True, with_consent_codes=True)
        self.participant = self.data_generator.create_database_participant(
            participantId=123,
        )
        self.participant_id = self.participant.participantId
        self.setup_mismatched_retention_data()

    def setup_mismatched_retention_data(self):
        # Setting up the mismatched data to see if running the tool will fix the mismatches
        self.summary = self.participant_summary(self.participant)
        self.retention_eligible_metrics = RetentionEligibleMetrics(
            participantId=self.participant_id,
            retentionEligibleStatus=True,
            retentionEligibleTime=datetime(2024, 4, 17),
        )
        self.session.add(self.summary)
        self.session.add(self.retention_eligible_metrics)
        self.session.commit()

    def test_fix_mismatch_flag(self):
        """
        a test to see if the data from participant summary
         will match the data from retention metrics
        if the Retention Recalc Tool is called with the 'fix-mismatches' flag
        """
        tool_args = {
            "id": False,
            "from_file": False,
            "fix_mismatches": True}
        self.run_tool(RetentionRecalcClass, tool_args=tool_args, mock_session=False, session=self.session)
        self.session.refresh(self.summary)

        self.assertEqual(
            self.summary.retentionEligibleStatus,
            self.retention_eligible_metrics.retentionEligibleStatus,
        )
        self.assertEqual(
            self.summary.retentionEligibleTime,
            self.retention_eligible_metrics.retentionEligibleTime,
        )
