from unittest import mock

from rdr_service import config
from rdr_service.dao.ppsc_partner_transfer_dao import PPSCDataTransferBaseDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.model.ppsc_partner_data_transfer import PPSCEHR
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

    @mock.patch("rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed.InputFeed.make_datafeed_job")
    def test_run_datafeed(self, mock_make_datafeed_job):
        ppsc_data_gen = PPSCDataGenerator()
        test_participant = ppsc_data_gen.create_database_participant()
        # Test data
        mock_streaming_data_rows = [
            {"participant_id": test_participant.id, "event_date_time": "2024-11-20"},
        ]

        # Mock make_datafeed_job() to return the mocked streaming_data
        mock_make_datafeed_job.side_effect = lambda query: iter(
            mock_streaming_data_rows) if ehr_expected_streaming_sql in query else None

        # Test Feed
        feed = InputFeed()
        feed.get_datafeed_definition = mock.Mock(return_value={
            "staging_data": ehr_expected_sql,
            "streaming_data": ehr_expected_streaming_sql,
            "output_model": PPSCEHR
        })

        feed.run_datafeed("ehr")

        # Test insert into MySQL table worked
        ehr_dao = PPSCDataTransferBaseDao(PPSCEHR)
        actual_rows = ehr_dao.get_all()
        self.assertEqual(actual_rows[0].participant_id, mock_streaming_data_rows[0]['participant_id'])
