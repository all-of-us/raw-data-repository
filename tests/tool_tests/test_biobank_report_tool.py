from datetime import datetime, timedelta
import mock

from rdr_service import config
from rdr_service.model.participant import Participant
from rdr_service.participant_enums import WithdrawalAIANCeremonyStatus, WithdrawalStatus
from rdr_service.tools.tool_libs.biobank_report import BiobankReportTool
from tests.helpers.tool_test_mixin import ToolTestMixin
from tests.helpers.unittest_base import BaseTestCase


class BiobankReportToolTest(ToolTestMixin, BaseTestCase):
    def test_ceremony_decision(self):
        # Set up data for withdrawal report
        # Clearing microseconds to avoid rounding time up in database and causing test to fail
        two_days_ago = self._datetime_n_days_ago(2)
        withdrawal_reason_justification = 'testing withdrawal'
        no_ceremony_native_american_participant = self.data_generator.create_withdrawn_participant(
            withdrawal_reason_justification=withdrawal_reason_justification,
            is_native_american=True,
            requests_ceremony=WithdrawalAIANCeremonyStatus.DECLINED,
            withdrawal_time=two_days_ago
        )
        ceremony_native_american_participant = self.data_generator.create_withdrawn_participant(
            withdrawal_reason_justification=withdrawal_reason_justification,
            is_native_american=True,
            requests_ceremony=WithdrawalAIANCeremonyStatus.REQUESTED,
            withdrawal_time=two_days_ago
        )
        native_american_participant_without_answer = self.data_generator.create_withdrawn_participant(
            withdrawal_reason_justification=withdrawal_reason_justification,
            is_native_american=True,
            requests_ceremony=None,
            withdrawal_time=two_days_ago
        )
        # Non-AIAN should not have been presented with a ceremony choice
        non_native_american_participant = self.data_generator.create_withdrawn_participant(
            withdrawal_reason_justification=withdrawal_reason_justification,
            is_native_american=False,
            requests_ceremony=None,
            withdrawal_time=two_days_ago
        )

        # Check that the participants are written to the export with the expected values
        rows_written = self.run_withdrawal_report()
        withdrawal_iso_str = two_days_ago.strftime('%Y-%m-%dT%H:%M:%SZ')
        self._assert_participant_in_report_rows(
            non_native_american_participant,
            rows_written,
            withdrawal_iso_str,
            as_native_american=False,
            needs_ceremony_indicator='NA',
            withdrawal_reason_justification=withdrawal_reason_justification
        )
        self._assert_participant_in_report_rows(
            ceremony_native_american_participant,
            rows_written,
            withdrawal_iso_str,
            as_native_american=True,
            needs_ceremony_indicator='Y',
            withdrawal_reason_justification=withdrawal_reason_justification
        )
        self._assert_participant_in_report_rows(
            native_american_participant_without_answer,
            rows_written,
            withdrawal_iso_str,
            as_native_american=True,
            needs_ceremony_indicator='U',
            withdrawal_reason_justification=withdrawal_reason_justification
        )
        self._assert_participant_in_report_rows(
            no_ceremony_native_american_participant,
            rows_written,
            withdrawal_iso_str,
            as_native_american=True,
            needs_ceremony_indicator='N',
            withdrawal_reason_justification=withdrawal_reason_justification
        )

    def test_withdrawal_report_includes_participants_with_recent_samples(self):
        """
        Occasionally a participant will send a saliva kit and then immediately withdraw. In this scenario
        they would never be on a withdrawal manifest because they didn't have any samples until after the
        10-day window.
        """
        twenty_days_ago = self._datetime_n_days_ago(20)
        five_days_ago = self._datetime_n_days_ago(5)

        # Create a participant that has a withdrawal time outside of the report range, but recently had a sample created
        withdrawn_participant = self.data_generator.create_database_participant(
            withdrawalTime=twenty_days_ago,
            withdrawalStatus=WithdrawalStatus.NO_USE,
            withdrawalReasonJustification='withdraw before delivery'
        )
        self.data_generator.create_database_biobank_stored_sample(
            biobankId=withdrawn_participant.biobankId,
            created=five_days_ago
        )

        rows_written = self.run_withdrawal_report()
        self._assert_participant_in_report_rows(
            withdrawn_participant,
            rows=rows_written,
            withdrawal_date_str=twenty_days_ago.strftime('%Y-%m-%dT%H:%M:%SZ'),
            withdrawal_reason_justification='withdraw before delivery'
        )

    def _assert_participant_in_report_rows(self, participant: Participant, rows, withdrawal_date_str,
                                           withdrawal_reason_justification, as_native_american: bool = False,
                                           needs_ceremony_indicator: str = 'NA', paired_hpo='UNSET', paired_org=None,
                                           paired_site=None, deceased_status='UNSET'):
        expected_data = {
            'participant_id': f'P{participant.participantId}',
            'biobank_id': f'Z{participant.biobankId}',
            'withdrawal_time': withdrawal_date_str,
            'is_native_american': 'Y' if as_native_american else 'N',
            'needs_disposal_ceremony': needs_ceremony_indicator,
            'participant_origin': participant.participantOrigin,
            'paired_hpo': paired_hpo,
            'paired_org': paired_org,
            'paired_site': paired_site,
            'withdrawal_reason_justification': withdrawal_reason_justification,
            'deceased_status': deceased_status
        }
        self.assertIn(expected_data, rows)

    def run_withdrawal_report(self):
        with mock.patch('rdr_service.tools.tool_libs.biobank_report.csv') as csv_mock:
            self.run_tool(
                BiobankReportTool,
                tool_args={'command': 'withdrawal'},
                server_config={config.BIOBANK_ID_PREFIX: 'Z'}
            )

            dict_writer_mock = csv_mock.DictWriter.return_value
            return [call.args[0] for call in dict_writer_mock.writerow.call_args_list]

    def _datetime_n_days_ago(self, days_ago: int) -> datetime:
        # Clearing microseconds to avoid rounding time up in database and causing test to fail
        return datetime.today().replace(microsecond=0) - timedelta(days=days_ago)
