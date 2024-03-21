import mock
from rdr_service.tools.tool_libs.participant_summary_data_dump import ParticipantSummaryDataDump
from tests.helpers.unittest_base import BaseTestCase


class ParticipantSummaryDataDumpTest(BaseTestCase):

    def setUp(self):
        super().setUp()

        participant_summary_dao_patcher = mock.patch(
            'rdr_service.tools.tool_libs.participant_summary_data_dump.ParticipantSummaryDao'
        )
        self.participant_summary_dao_mock = participant_summary_dao_patcher.start().return_value
        self.addCleanup(participant_summary_dao_patcher.stop)

    def test_running_tool(self):
        environment = mock.MagicMock()
        environment.project = 'unit_test'
        args = mock.MagicMock()
        mock.patch('rdr_service.tools.tool_libs.participant_summary_data_dump.upload_to_gcs')
        psdd_tool = ParticipantSummaryDataDump(args, environment)
        psdd_tool.run()
        self.assertLogs('last_id:')
        self.assertLogs('to cloud bucket')

