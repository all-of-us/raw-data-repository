from unittest import mock

from rdr_service import config
from tests.service_tests.test_genomic_datagen import GenomicDataGenMixin
from rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed import InputFeed


class DataTransferInputTest(GenomicDataGenMixin):
    def setUp(self, *args, **kwargs) -> None:
        # pylint: disable=unused-argument
        super().setUp()

    @mock.patch("rdr_service.cloud_utils.bigquery.BigQueryJob")
    def test_get_core_data_definition(self, mock_bq):
        self.temporarily_override_config_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, ["participant_ehr"])
        self.temporarily_override_config_setting(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION, ["org_ehr"])
        core_data_expected_sql = """
INSERT INTO `test.ppsc_staging_data.datafeed_input_core_data` (
    participant_id,
    has_core_data,
    ignore_flag,
    event_date_time,
    created,
    modified)
SELECT DISTINCT
    c.participant_id,
    1 as has_core_data,
    0 as ignore_flag,
    GREATEST(
        c.event_authored_time,
        ehrc.event_authored_time,
        basics.event_authored_time,
        overall.event_authored_time,
        lifestyle.event_authored_time,
        pm.finalized,
        IFNULL(participant_ehr.authored_date, organization_ehr.authored_date)
    ) AS event_date_time,
    CURRENT_TIMESTAMP() AS created,
    CURRENT_TIMESTAMP() AS modified
FROM `test.ppsc_staging_data.ppsc_participant` p
JOIN `test.ppsc_staging_data.ppsc_consent_event` c
    ON c.participant_id = p.id

-- EHR Consent
JOIN `test.ppsc_staging_data.ppsc_consent_event` ehrc
    ON c.participant_id = ehrc.participant_id
    AND c.data_element_value = "submitted_yes"
    AND c.event_type_name = "EHR Authorization"

-- EHR Received
-- EHR tables - Record might exist in either table
LEFT JOIN `test-ehr-project.participant_ehr.ehr_upload_pids` participant_ehr
    ON c.participant_id = participant_ehr.person_id

LEFT JOIN `test-ehr-project.org_ehr.ehr_upload_pids` organization_ehr
    ON c.participant_id = organization_ehr.person_id

-- Basics Completion
JOIN `test.ppsc_staging_data.ppsc_survey_completion_event` basics
    ON basics.participant_id = c.participant_id
    AND basics.event_type_name = "The Basics"
    AND basics.data_element_value = "submitted_yes"

-- Overall Health Completion
JOIN `test.ppsc_staging_data.ppsc_survey_completion_event` overall
    ON overall.participant_id = c.participant_id
    AND overall.event_type_name = "Overall Health"
    AND overall.data_element_value = "submitted_yes"

-- Lifestyle Completion
JOIN `test.ppsc_staging_data.ppsc_survey_completion_event` lifestyle
    ON lifestyle.participant_id = c.participant_id
    AND lifestyle.event_type_name = "Lifestyle"
    AND lifestyle.data_element_value = "submitted_yes"

-- Physical Measurements
JOIN `test.ppsc_staging_data.rdr_physical_measurements` pm
    ON pm.participant_id = c.participant_id

-- Height Measurement
JOIN `test.ppsc_staging_data.rdr_measurement` height
    ON height.physical_measurements_id = pm.physical_measurements_id
    AND height.code_value = "height"

-- Weight Measurement
JOIN `test.ppsc_staging_data.rdr_measurement` weight
    ON weight.physical_measurements_id = pm.physical_measurements_id
    AND weight.code_value = "weight"

WHERE
    c.data_element_value = "submitted_yes"
    AND c.event_type_name = "Primary Consent"

    -- Ensure at least one EHR record exists
    AND (participant_ehr.person_id IS NOT NULL OR organization_ehr.person_id IS NOT NULL)

    -- Insert only if participant_id doesn't exist in the target table
    AND NOT EXISTS (
        SELECT 1
        FROM `test.ppsc_staging_data.datafeed_input_core_data` t
        WHERE t.participant_id = c.participant_id
    );"""

        feed = InputFeed()

        query = feed.get_datafeed_definition("core data")

        self.assertEqual(core_data_expected_sql.strip(), query.strip())

        feed.run_datafeed("core data")

        # Assert that BigQueryJob was instantiated with the correct SQL
        mock_bq.assert_called_once_with(core_data_expected_sql)
