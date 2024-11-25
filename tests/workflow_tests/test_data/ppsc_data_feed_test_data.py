core_data_expected_sql = """
WITH earliest_ehr AS (
    SELECT participant_id,
           MIN(event_date_time) AS event_date_time
    FROM `test.ppsc_staging_data.datafeed_input_ehr`
    GROUP BY participant_id
)

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
        earliest_ehr.event_date_time
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

-- Earliest EHR Received
JOIN earliest_ehr
    ON c.participant_id = earliest_ehr.participant_id

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

    -- Insert only if participant_id doesn't exist in the target table
    AND NOT EXISTS (
        SELECT 1
        FROM `test.ppsc_staging_data.datafeed_input_core_data` t
        WHERE t.participant_id = c.participant_id
    );"""

core_data_expected_streaming_sql = """
    SELECT distinct participant_id, ignore_flag, event_date_time, has_core_data
    FROM `test.ppsc_staging_data.datafeed_input_core_data` s
    where TRUE
      AND NOT EXISTS (
            SELECT 1
            FROM `test.ppsc_staging_data.ppsc_ppsc_core` t
            WHERE t.participant_id = s.participant_id
                AND t.event_date_time = s.event_date_time
        )
    ;"""

biospecimen_expected_sql = """
INSERT INTO `test.ppsc_staging_data.datafeed_input_biospecimen` (
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
FROM `test.ppsc_staging_data.ppsc_participant` p
  JOIN `test.rdr_operational_datastream.rdr_biobank_stored_sample` ss ON ss.biobank_id = p.biobank_id
WHERE TRUE
  AND ss.test IN ('1ed04', '1ed10', '1ed02', '2ed02', '2ed04', '1sal2', '1sal', '2sal0', '3sal1', '1ur10', '1ur90')
  AND NOT EXISTS (
        SELECT 1
        FROM `test.ppsc_staging_data.datafeed_input_biospecimen` t
        WHERE t.participant_id = p.id
    )
;
"""

biospecimen_expected_streaming_sql = """
    SELECT distinct participant_id, ignore_flag, event_date_time, specimen_type, specimen_status
    FROM `test.ppsc_staging_data.datafeed_input_biospecimen` s
    where TRUE
      AND NOT EXISTS (
            SELECT 1
            FROM `test.ppsc_staging_data.ppsc_ppsc_biobank_sample` t
            WHERE t.participant_id = s.participant_id
                AND t.event_date_time = s.event_date_time
        )
    ;"""

ehr_expected_sql = """
INSERT INTO `test.ppsc_staging_data.datafeed_input_ehr` (
    participant_id,
    ignore_flag,
    event_date_time,
    created,
    modified
)
SELECT DISTINCT
    p.id,
    0 as ignore_flag,
    participant_ehr.latest_upload_time,
    CURRENT_TIMESTAMP() AS created,
    CURRENT_TIMESTAMP() AS modified
FROM `test.ppsc_staging_data.ppsc_participant` p
    -- EHR Ops table
    JOIN `test-ehr-project.participant_ehr.ehr_upload_pids` participant_ehr
        ON p.participant_id = participant_ehr.person_id
WHERE TRUE
  AND NOT EXISTS (
        SELECT 1
        FROM `test.ppsc_staging_data.datafeed_input_ehr` t
        WHERE t.participant_id = p.id
            AND t.event_date_time = participant_ehr.latest_upload_time
    )
;
"""

ehr_expected_streaming_sql = """
SELECT distinct participant_id, ignore_flag, event_date_time
FROM `test.ppsc_staging_data.datafeed_input_ehr` s
where TRUE
  AND NOT EXISTS (
        SELECT 1
        FROM `test.ppsc_staging_data.ppsc_ppsc_ehr` t
        WHERE t.participant_id = s.participant_id
            AND t.event_date_time = s.event_date_time
    )
;"""

health_data_sharing_expected_sql = """
INSERT INTO `test.ppsc_staging_data.datafeed_input_healthdata_sharing` (
    participant_id,
    ignore_flag,
    health_data_stream_sharing_status,
    event_date_time,
    created,
    modified
)
SELECT DISTINCT
    p.id,
    0 AS ignore_flag,
    CASE
        WHEN participant_ehr.person_id IS NOT NULL THEN 3
        ELSE 2
    END AS health_data_stream_sharing_status,
    iehr.event_date_time,
    CURRENT_TIMESTAMP() AS created,
    CURRENT_TIMESTAMP() AS modified
FROM `test.ppsc_staging_data.ppsc_participant` p
    -- PPSC Notified of EHR Received
    JOIN `test.ppsc_staging_data.datafeed_input_ehr` iehr
        ON iehr.participant_id = p.participant_id
    -- Participant in EHR Ops table
    LEFT JOIN `test-ehr-project.participant_ehr.ehr_upload_pids` participant_ehr
        ON p.participant_id = participant_ehr.person_id
WHERE TRUE
    -- Don't send if participant is already in the destination table with the same event time.
    AND NOT EXISTS (
        SELECT 1
        FROM `test.ppsc_staging_data.datafeed_input_healthdata_sharing` t
        WHERE t.participant_id = p.id
            AND t.event_date_time = iehr.event_date_time
    )
;
"""

health_data_expected_streaming_sql = """
    SELECT distinct participant_id, ignore_flag, event_date_time, health_data_stream_sharing_status
    FROM `test.ppsc_staging_data.datafeed_input_heathdata_sharing` s
    where TRUE
      AND NOT EXISTS (
            SELECT 1
            FROM `test.ppsc_staging_data.ppsc_ppsc_health_data` t
            WHERE t.participant_id = s.participant_id
                AND t.event_date_time = s.event_date_time
        )
    ;"""


# Intake-2-PPSC Test queries

consent_activity_expected_sql = f"""
WITH ranked_events AS (
  SELECT
    ce.participant_id,
    ce.event_type_name,
    ce.event_authored_time,
    ce.data_element_name,
    ce.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY ce.participant_id, ce.event_type_name
      ORDER BY ce.event_authored_time DESC
    ) AS rank
  FROM `test.ppsc_staging_data.ppsc_consent_event` ce
  WHERE ce.event_authored_time > (
    SELECT MAX(last_modified)
    FROM `test.ppsc_staging_data.rdr_participant_summary`
  )
  and data_element_name IN ("activity_status", '​activity_status')
)
SELECT
  participant_id,
  MAX(CASE WHEN event_type_name = 'EHR Authorization' AND data_element_name = 'activity_status' AND rank = 1
           THEN data_element_value END) AS ehr_authorization,
  MAX(CASE WHEN event_type_name = 'EHR Authorization' AND rank = 1
           THEN event_authored_time END) AS ehr_authorization_event_authored_time,
  MAX(CASE WHEN event_type_name = 'Primary Consent' AND data_element_name = 'activity_status' AND rank = 1
           THEN data_element_value END) AS primary_consent,
  MAX(CASE WHEN event_type_name = 'Primary Consent' AND rank = 1
           THEN event_authored_time END) AS primary_consent_event_authored_time
FROM ranked_events
GROUP BY participant_id
    """
