from unittest import mock

from rdr_service import config
from tests.service_tests.test_genomic_datagen import GenomicDataGenMixin
from tests.workflow_tests.test_data.ppsc_data_feed_test_data import core_data_expected_sql, biospecimen_expected_sql, \
    ehr_expected_sql, health_data_sharing_expected_sql, ehr_expected_streaming_sql, core_data_expected_streaming_sql, \
    biospecimen_expected_streaming_sql, health_data_expected_streaming_sql
from rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed import InputFeed


class DataTransferInputTest(GenomicDataGenMixin):
    def setUp(self, *args, **kwargs) -> None:
        # pylint: disable=unused-argument
        self.temporarily_override_config_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, ["participant_ehr"])
        self.temporarily_override_config_setting(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION, ["org_ehr"])
        super().setUp()

    @mock.patch("google.cloud.bigquery.Client.query")
    def test_core_data_datafeed(self, mock_bq):
        feed = InputFeed()

        query = feed.get_datafeed_definition("core data")['staging_data'].strip()
        staging_data_expected_sql = core_data_expected_sql.strip()
        streaming_data_expected_sql = core_data_expected_streaming_sql.strip()

        self.assertEqual(staging_data_expected_sql, query)

        feed.run_datafeed("core data")

        self.assertEqual(mock_bq.call_count, 2)

        # Check the actual calls made
        expected_calls = [
            staging_data_expected_sql,
            streaming_data_expected_sql,
        ]
        actual_calls = [call.args[0].strip() for call in mock_bq.mock_calls if call.args]

        for expected, actual in zip(expected_calls, actual_calls):
            self.assertEqual(expected, actual,
                             f"Mismatch in SQL call:\nExpected:\n{expected}\n\nActual:\n{actual}")

    @mock.patch("google.cloud.bigquery.Client.query")
    def test_biospecimen_datafeed(self, mock_bq):
        feed = InputFeed()

        query = feed.get_datafeed_definition("biospecimen")['staging_data'].strip()
        staging_data_expected_sql = biospecimen_expected_sql.strip()
        streaming_data_expected_sql = biospecimen_expected_streaming_sql.strip()

        self.assertEqual(staging_data_expected_sql, query)

        feed.run_datafeed("biospecimen")

        self.assertEqual(mock_bq.call_count, 2)

        # Check the actual calls made
        expected_calls = [
            staging_data_expected_sql,
            streaming_data_expected_sql,
        ]
        actual_calls = [call.args[0].strip() for call in mock_bq.mock_calls if call.args]

        for expected, actual in zip(expected_calls, actual_calls):
            self.assertEqual(expected, actual,
                             f"Mismatch in SQL call:\nExpected:\n{expected}\n\nActual:\n{actual}")

    @mock.patch("google.cloud.bigquery.Client.query")
    def test_ehr_datafeed(self, mock_bq):
        feed = InputFeed()

        query = feed.get_datafeed_definition("ehr")['staging_data'].strip()
        staging_data_expected_sql = ehr_expected_sql.strip()
        streaming_data_expected_sql = ehr_expected_streaming_sql.strip()

        self.assertEqual(staging_data_expected_sql, query)

        feed.run_datafeed("ehr")

        self.assertEqual(mock_bq.call_count, 2)

        # Check the actual calls made
        expected_calls = [
            staging_data_expected_sql,
            streaming_data_expected_sql,
        ]
        actual_calls = [call.args[0].strip() for call in mock_bq.mock_calls if call.args]

        for expected, actual in zip(expected_calls, actual_calls):
            self.assertEqual(expected, actual,
                             f"Mismatch in SQL call:\nExpected:\n{expected}\n\nActual:\n{actual}")

    @mock.patch("google.cloud.bigquery.Client.query")
    def test_health_data_sharing_datafeed(self, mock_bq):
        feed = InputFeed()

        query = feed.get_datafeed_definition("health data sharing")['staging_data'].strip()
        staging_data_expected_sql = health_data_sharing_expected_sql.strip()
        streaming_data_expected_sql = health_data_expected_streaming_sql.strip()

        self.assertEqual(staging_data_expected_sql, query)

        feed.run_datafeed("health data sharing")

        self.assertEqual(mock_bq.call_count, 2)

        # Check the actual calls made
        expected_calls = [
            staging_data_expected_sql,
            streaming_data_expected_sql,
        ]
        actual_calls = [call.args[0].strip() for call in mock_bq.mock_calls if call.args]

        for expected, actual in zip(expected_calls, actual_calls):
            self.assertEqual(expected, actual,
                             f"Mismatch in SQL call:\nExpected:\n{expected}\n\nActual:\n{actual}")
