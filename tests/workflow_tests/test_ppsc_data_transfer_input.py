from unittest import mock

from rdr_service import config
from tests.service_tests.test_genomic_datagen import GenomicDataGenMixin
from tests.workflow_tests.test_data.ppsc_data_feed_test_data import core_data_expected_sql, biospecimen_expected_sql, \
    ehr_expected_sql
from rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed import InputFeed


class DataTransferInputTest(GenomicDataGenMixin):
    def setUp(self, *args, **kwargs) -> None:
        # pylint: disable=unused-argument
        self.temporarily_override_config_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, ["participant_ehr"])
        self.temporarily_override_config_setting(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION, ["org_ehr"])
        super().setUp()

    @mock.patch("rdr_service.cloud_utils.bigquery.BigQueryJob")
    def test_core_data_datafeed(self, mock_bq):
        feed = InputFeed()

        query = feed.get_datafeed_definition("core data")

        self.assertEqual(core_data_expected_sql.strip(), query.strip())

        feed.run_datafeed("core data")

        # Assert that BigQueryJob was instantiated with the correct SQL
        mock_bq.assert_called_once_with(core_data_expected_sql)

    @mock.patch("rdr_service.cloud_utils.bigquery.BigQueryJob")
    def test_biospecimen_datafeed(self, mock_bq):
        feed = InputFeed()

        query = feed.get_datafeed_definition("biospecimen")

        self.assertEqual(biospecimen_expected_sql.strip(), query.strip())

        feed.run_datafeed("biospecimen")

        # Assert that BigQueryJob was instantiated with the correct SQL
        mock_bq.assert_called_once_with(biospecimen_expected_sql)

    @mock.patch("rdr_service.cloud_utils.bigquery.BigQueryJob")
    def test_ehr_datafeed(self, mock_bq):
        feed = InputFeed()

        query = feed.get_datafeed_definition("ehr")

        self.assertEqual(ehr_expected_sql.strip(), query.strip())

        feed.run_datafeed("ehr")

        # Assert that BigQueryJob was instantiated with the correct SQL
        mock_bq.assert_called_once_with(ehr_expected_sql)
