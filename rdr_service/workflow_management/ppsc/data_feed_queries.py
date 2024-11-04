from rdr_service import config
from rdr_service.config import GAE_PROJECT


def insert_core_data(project: str, src_operational_dataset: str, destination_dataset: str) -> str:
    # EHR BQ Data
    org_ehr_dataset = config.getSettingJson(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION)[0]
    participant_ehr_dataset = config.getSettingJson(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT)[0]

    if GAE_PROJECT == "all-of-us-rdr-prod":
        ehr_proj = "aou-res-curation-prod"
    else:
        ehr_proj = "test-ehr-project"

    return f"""
INSERT INTO `{project}.{destination_dataset}.datafeed_input_core_data` (
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
FROM `{project}.{destination_dataset}.ppsc_participant` p
JOIN `{project}.{destination_dataset}.ppsc_consent_event` c
    ON c.participant_id = p.id

-- EHR Consent
JOIN `{project}.{destination_dataset}.ppsc_consent_event` ehrc
    ON c.participant_id = ehrc.participant_id
    AND c.data_element_value = "submitted_yes"
    AND c.event_type_name = "EHR Authorization"

-- EHR Received
-- EHR tables - Record might exist in either table
LEFT JOIN `{ehr_proj}.{participant_ehr_dataset}.ehr_upload_pids` participant_ehr
    ON c.participant_id = participant_ehr.person_id

LEFT JOIN `{ehr_proj}.{org_ehr_dataset}.ehr_upload_pids` organization_ehr
    ON c.participant_id = organization_ehr.person_id

-- Basics Completion
JOIN `{project}.{destination_dataset}.ppsc_survey_completion_event` basics
    ON basics.participant_id = c.participant_id
    AND basics.event_type_name = "The Basics"
    AND basics.data_element_value = "submitted_yes"

-- Overall Health Completion
JOIN `{project}.{destination_dataset}.ppsc_survey_completion_event` overall
    ON overall.participant_id = c.participant_id
    AND overall.event_type_name = "Overall Health"
    AND overall.data_element_value = "submitted_yes"

-- Lifestyle Completion
JOIN `{project}.{destination_dataset}.ppsc_survey_completion_event` lifestyle
    ON lifestyle.participant_id = c.participant_id
    AND lifestyle.event_type_name = "Lifestyle"
    AND lifestyle.data_element_value = "submitted_yes"

-- Physical Measurements
JOIN `{project}.{src_operational_dataset}.rdr_physical_measurements` pm
    ON pm.participant_id = c.participant_id

-- Height Measurement
JOIN `{project}.{src_operational_dataset}.rdr_measurement` height
    ON height.physical_measurements_id = pm.physical_measurements_id
    AND height.code_value = "height"

-- Weight Measurement
JOIN `{project}.{src_operational_dataset}.rdr_measurement` weight
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
        FROM `{project}.{destination_dataset}.datafeed_input_core_data` t
        WHERE t.participant_id = c.participant_id
    );"""


def insert_biospecimen(project: str, src_operational_dataset: str, destination_dataset: str) -> str:
    biospecimen_list = config.getSettingJson(config.PPSC_DATAFEED_BIOSPECIMEN_TYPES)
    formatted_values = ", ".join(f"'{value}'" for value in biospecimen_list)
    return f"""
INSERT INTO `{project}.{destination_dataset}.datafeed_input_biospecimen` (
    participant_id,
    ignore_flag,
    event_date_time,
    created,
    modified,
    specimen_type,
    specimen_status
)
SELECT DISTINCT
    p.id,
    0 as ignore_flag,
    ss.created as event_date_time,
    CURRENT_TIMESTAMP() AS created,
    CURRENT_TIMESTAMP() AS modified,
    CASE
        WHEN ss.test LIKE "%ED%" THEN 1 -- Blood
        WHEN ss.test LIKE "%SAL%" THEN 2 -- Saliva
        WHEN ss.test LIKE "%UR%" THEN 3 -- Urine
        ELSE null
    END AS specimen_type,
    1 as specimen_status
FROM `{project}.{src_operational_dataset}.ppsc_participant` p
  JOIN `{project}.rdr_operational_datastream.rdr_biobank_stored_sample` ss ON ss.biobank_id = p.biobank_id
WHERE TRUE
  AND ss.test IN ({formatted_values})
  AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_input_biospecimen` t
        WHERE t.participant_id = p.id
    )
;
"""

def insert_ehr_receipt(project: str, src_operational_dataset: str, destination_dataset: str) -> str:
    # EHR BQ Data
    org_ehr_dataset = config.getSettingJson(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION)[0]
    participant_ehr_dataset = config.getSettingJson(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT)[0]

    if GAE_PROJECT == "all-of-us-rdr-prod":
        ehr_proj = "aou-res-curation-prod"
    else:
        ehr_proj = "test-ehr-project"

    return f"""
INSERT INTO `{project}.{destination_dataset}.datafeed_input_ehr` (
    participant_id,
    ignore_flag,
    event_date_time,
    created,
    modified
)
SELECT DISTINCT
    p.id,
    0 as ignore_flag,
    IFNULL(participant_ehr.authored_date, organization_ehr.authored_date),
    CURRENT_TIMESTAMP() AS created,
    CURRENT_TIMESTAMP() AS modified
FROM `{project}.{src_operational_dataset}.ppsc_participant` p
    -- EHR tables - Record might exist in either table
    LEFT JOIN `{ehr_proj}.{participant_ehr_dataset}.ehr_upload_pids` participant_ehr
        ON p.participant_id = participant_ehr.person_id
    LEFT JOIN `{ehr_proj}.{org_ehr_dataset}.ehr_upload_pids` organization_ehr
        ON p.participant_id = organization_ehr.person_id
WHERE TRUE
  AND (participant_ehr.person_id IS NOT NULL OR organization_ehr.person_id IS NOT NULL)
  AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_input_ehr` t
        WHERE t.participant_id = p.id
    )
;
"""
