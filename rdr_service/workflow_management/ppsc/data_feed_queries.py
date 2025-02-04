from rdr_service import config
from rdr_service.model.awardee_insite import AwardeeInSite


def insert_core_data(project: str, src_operational_dataset: str, destination_dataset: str) -> str:
    return f"""
WITH earliest_ehr AS (
    SELECT participant_id,
           MIN(event_date_time) AS event_date_time
    FROM `{project}.{destination_dataset}.datafeed_input_ehr`
    GROUP BY participant_id
)

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
        aw4.created,
        basics.event_authored_time,
        overall.event_authored_time,
        lifestyle.event_authored_time,
        pm.finalized,
        earliest_ehr.event_date_time
    ) AS event_date_time,
    CURRENT_TIMESTAMP() AS created,
    CURRENT_TIMESTAMP() AS modified
FROM `{project}.{destination_dataset}.ppsc_participant` p
JOIN `{project}.{destination_dataset}.ppsc_consent_event` c
    ON c.participant_id = p.id
    AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_exclusion_list` excluded
        WHERE excluded.identifier = c.event_id
          AND excluded.identifier_type = "event_id"
    )

-- EHR Consent
JOIN `{project}.{destination_dataset}.ppsc_consent_event` ehrc
    ON c.participant_id = ehrc.participant_id
    AND ehrc.data_element_value = "Yes"
    AND ehrc.event_type_name = "EHR Authorization"
    AND ehrc.ignore_flag = 0
    AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_exclusion_list` excluded
        WHERE excluded.identifier = ehrc.event_id
          AND excluded.identifier_type = "event_id"
    )

-- Earliest EHR Received
JOIN earliest_ehr
    ON c.participant_id = earliest_ehr.participant_id

-- WGS Sequenced
JOIN `{project}.{destination_dataset}.rdr_genomic_aw4_raw` aw4
    ON aw4.biobank_id = CAST(p.biobank_id as STRING)
        AND aw4.genome_type = "aou_wgs"
        AND aw4.pipeline_id = "dragen_3.7.8"
        AND aw4.qc_status = "PASS"
        AND aw4.ignore_flag = 0

-- Basics Completion
JOIN `{project}.{destination_dataset}.ppsc_survey_completion_event` basics
    ON basics.participant_id = c.participant_id
    AND basics.event_type_name = "The Basics"
    AND basics.data_element_value = "submitted_complete"
    AND basics.ignore_flag = 0
    AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_exclusion_list` excluded
        WHERE excluded.identifier = basics.event_id
          AND excluded.identifier_type = "event_id"
    )

-- Overall Health Completion
JOIN `{project}.{destination_dataset}.ppsc_survey_completion_event` overall
    ON overall.participant_id = c.participant_id
    AND overall.event_type_name = "Overall Health"
    AND overall.data_element_value = "submitted_complete"
    AND overall.ignore_flag = 0
    AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_exclusion_list` excluded
        WHERE excluded.identifier = overall.event_id
          AND excluded.identifier_type = "event_id"
    )

-- Lifestyle Completion
JOIN `{project}.{destination_dataset}.ppsc_survey_completion_event` lifestyle
    ON lifestyle.participant_id = c.participant_id
    AND lifestyle.event_type_name = "Lifestyle"
    AND lifestyle.data_element_value = "submitted_complete"
    AND lifestyle.ignore_flag = 0
    AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_exclusion_list` excluded
        WHERE excluded.identifier = lifestyle.event_id
          AND excluded.identifier_type = "event_id"
    )

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
    c.data_element_value = "Yes"
    AND c.ignore_flag = 0
    AND c.event_type_name = "Primary Consent"

    -- Insert only if participant_id doesn't exist in the target table
    AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_input_core_data` t
        WHERE t.participant_id = c.participant_id
        AND t.ignore_flag = 0
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
  AND ss.created > "2024-12-02"
  AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_input_biospecimen` t
        WHERE t.participant_id = p.id
        AND t.ignore_flag = 0
    )
;
"""

def insert_ehr_receipt(project: str, src_operational_dataset: str, destination_dataset: str) -> str:
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
    participant_ehr.last_seen,
    CURRENT_TIMESTAMP() AS created,
    CURRENT_TIMESTAMP() AS modified
FROM `{project}.{src_operational_dataset}.ppsc_participant` p
    -- EHR Ops table
    JOIN `{project}.{destination_dataset}.rdr_participant_ehr_receipt` participant_ehr
        ON p.id = participant_ehr.participant_id
WHERE TRUE
  AND participant_ehr.file_timestamp > "2024-12-02"
  AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_input_ehr` t
        WHERE t.participant_id = p.id
            AND t.event_date_time = participant_ehr.last_seen
            AND t.ignore_flag = 0
    )
;
"""

def insert_health_data_sharing(project: str, src_operational_dataset: str, destination_dataset: str) -> str:
    return f"""
INSERT INTO `{project}.{destination_dataset}.datafeed_input_healthdata_sharing` (
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
        WHEN participant_ehr.participant_id IS NOT NULL THEN 3
        ELSE 2
    END AS health_data_stream_sharing_status,
    iehr.event_date_time,
    CURRENT_TIMESTAMP() AS created,
    CURRENT_TIMESTAMP() AS modified
FROM `{project}.{src_operational_dataset}.ppsc_participant` p
    -- PPSC Notified of EHR Received
    JOIN `{project}.{destination_dataset}.datafeed_input_ehr` iehr
        ON iehr.participant_id = p.id
    -- Participant in EHR Ops table
    LEFT JOIN `{project}.{destination_dataset}.rdr_participant_ehr_receipt` participant_ehr
        ON p.id = participant_ehr.participant_id
WHERE TRUE
    -- Don't send if participant is already in the destination table with the same event time.
    AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.datafeed_input_healthdata_sharing` t
        WHERE t.participant_id = p.id
            AND t.event_date_time = iehr.event_date_time
            AND t.ignore_flag = 0
    )
;
"""

def get_ppsc_core_to_stream(project: str, destination_dataset: str) -> str:
    return f"""
    SELECT distinct participant_id, ignore_flag, event_date_time, created, modified, has_core_data
    FROM `{project}.{destination_dataset}.datafeed_input_core_data` s
    where TRUE
      AND ignore_flag = 0
      AND NOT EXISTS (
            SELECT 1
            FROM `{project}.{destination_dataset}.ppsc_ppsc_core` t
            WHERE t.participant_id = s.participant_id
                AND t.event_date_time = s.event_date_time
                AND t.ignore_flag = 0
        )
    ;"""

def get_ppsc_biospecimen_to_stream(project: str, destination_dataset: str) -> str:
    return f"""
    SELECT distinct participant_id, ignore_flag, event_date_time, created, modified, specimen_type, specimen_status
    FROM `{project}.{destination_dataset}.datafeed_input_biospecimen` s
    where TRUE
      AND ignore_flag = 0
      AND NOT EXISTS (
            SELECT 1
            FROM `{project}.{destination_dataset}.ppsc_ppsc_biobank_sample` t
            WHERE t.participant_id = s.participant_id
                AND t.event_date_time = s.event_date_time
                AND t.ignore_flag = 0
        )
    ;"""

def get_ppsc_ehr_to_stream(project: str, destination_dataset: str) -> str:
    return f"""
SELECT distinct participant_id, ignore_flag, created, modified, event_date_time
FROM `{project}.{destination_dataset}.datafeed_input_ehr` s
where TRUE
  AND ignore_flag = 0
  AND NOT EXISTS (
        SELECT 1
        FROM `{project}.{destination_dataset}.ppsc_ppsc_ehr` t
        WHERE t.participant_id = s.participant_id
            AND t.event_date_time = s.event_date_time
            AND t.ignore_flag = 0
    )
;"""

def get_health_data_to_stream(project: str, destination_dataset: str) -> str:
    return f"""
    SELECT distinct participant_id, ignore_flag, event_date_time, created, modified, health_data_stream_sharing_status
    FROM `{project}.{destination_dataset}.datafeed_input_healthdata_sharing` s
    where TRUE
      AND ignore_flag = 0
      AND NOT EXISTS (
            SELECT 1
            FROM `{project}.{destination_dataset}.ppsc_ppsc_health_data` t
            WHERE t.participant_id = s.participant_id
                AND t.event_date_time = s.event_date_time
                AND t.ignore_flag = 0
        )
    ;"""


def get_awardee_insite_data_to_stream(project: str, destination_dataset: str) -> str:
    """Get data for Awardee InSite to stream to MySQL. The SQL will return new
    records or if an existing column value changed.
    """
    return f"""
        WITH awardee_insite_with_surrogate_key AS (
            SELECT participant_id
                , {AwardeeInSite.create_surrogate_key_sql()} AS surrogate_key
            FROM `{project}.{destination_dataset}.ppsc_awardee_insite`
         ),
         most_recent_datafeed_records AS (
              SELECT *
              FROM (
                SELECT *
                  , ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY created DESC) AS rn
                FROM `rdr_operational_datastream.datafeed_input_awardee_insite`
              )
              WHERE rn = 1
        )
         SELECT * EXCEPT (surrogate_key, created, rn)
         FROM most_recent_datafeed_records mrdr
         WHERE NOT EXISTS (
            SELECT 1
            FROM awardee_insite_with_surrogate_key ai
            WHERE mrdr.participant_id = ai.participant_id
                AND mrdr.surrogate_key = ai.surrogate_key
         )
        """


def insert_awardee_insite_data(
    project: str, src_operational_dataset: str, destination_dataset: str
) -> str:
    """Insert data into `datafeed_input_awardee_insite` table. Also takes care of withdrawn participants"""

    return f"""
        INSERT INTO `{project}.{destination_dataset}.datafeed_input_awardee_insite`
        (
          surrogate_key
          , created
          , participant_id
          , first_name
          , middle_name
          , last_name
          , zip_code
          , state
          , city
          , street_address
          , street_address2
          , phone_number
          , email
          , date_of_birth
          , organization
          , withdrawal_status
          , withdrawal_time
          , deactivation_status
          , deactivation_time
          , deceased_status
          , deceased_authored
          , consent_for_electronic_health_records
          , consent_for_electronic_health_records_authored
          , consent_for_electronic_health_records_first_yes_authored
          , first_ehr_receipt_time
          , latest_ehr_receipt_time
          , consent_for_study_enrollment
          , consent_for_study_enrollment_authored
          , enrollment_status
          , clinic_physical_measurements_status
          , clinic_physical_measurements_finalized_time
          , clinic_physical_measurements_finalized_site
          , self_reported_physical_measurements_status
          , self_reported_physical_measurements_authored
          , patient_status
          , biospecimen_source_site
          , biospecimen_order_time
          , biospecimen_status
          , sample_1sal2_collection_method
          , sample_status_1sal2
          , sample_order_status_1sal2
          , sample_order_status_1sal2_time
        )
        WITH
          participant_cte AS (
            SELECT id AS participant_id
            FROM `{project}.{src_operational_dataset}.ppsc_participant`
            WHERE ignore_flag = 0
          ),
          profile_pivot AS (
            SELECT participant_id
              , piiname_first AS first_name
              , piiname_middle AS middle_name
              , piiname_last AS last_name
              , streetaddress_piizip AS zip_code
              , streetaddress_piistate AS state
              , streetaddress_piicity AS city
              , piiaddress_streetaddress AS street_address
              , piiaddress_streetaddress2 AS street_address2
              , piicontactinformation_phone AS phone_number
              , piicontactinformation_email AS email
              , piibirthinformation_birthdate AS date_of_birth
            FROM
              (
                SELECT participant_id
                  , data_element_name
                  , data_element_value
                FROM `{project}.{src_operational_dataset}.ppsc_profile_updates_event`
                WHERE ignore_flag = 0
              )
            PIVOT(ANY_VALUE(data_element_value)
                FOR data_element_name IN
                    ('piiname_first'
                      , 'piiname_middle'
                      , 'piiname_last'
                      , 'streetaddress_piizip'
                      , 'streetaddress_piistate'
                      , 'streetaddress_piicity'
                      , 'piiaddress_streetaddress'
                      , 'piiaddress_streetaddress2'
                      , 'piicontactinformation_phone'
                      , 'piicontactinformation_email'
                      , 'piibirthinformation_birthdate'
                    )
                )
          ),
          organization_cte AS (
            SELECT participant_id
                , event_id
                , MAX(event_authored_time) AS activity_date_time
                , MAX(CASE WHEN REPLACE(data_element_name, '\u200B', '') = 'activity_status' THEN data_element_value END) AS activity_status
            FROM `{project}.{src_operational_dataset}.ppsc_attribution_event`
            WHERE event_type_name = 'Org Attribution' AND ignore_flag = 0
            GROUP BY 1, 2
          ),
          latest_organization AS (
            SELECT participant_id
                , activity_status AS organization
            FROM (
              SELECT participant_id
                , activity_status
                , activity_date_time
                , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY activity_date_time DESC) AS rn
              FROM organization_cte
            )
            WHERE rn = 1
          ),
          withdrawn_cte AS (
            SELECT participant_id
            , event_id
            , MAX(event_authored_time) AS activity_date_time
            , MAX(CASE WHEN REPLACE(data_element_name, '\u200B', '') = 'activity_status' THEN data_element_value END) AS activity_status
            FROM `{project}.{src_operational_dataset}.ppsc_withdrawal_event`
            WHERE LOWER(event_type_name) = 'withdrawal' AND ignore_flag = 0
            GROUP BY 1, 2
          ),
          latest_withdrawn AS (
            SELECT participant_id
            , activity_status AS withdrawal_status
            , activity_date_time AS withdrawal_time
            FROM (
              SELECT participant_id
                , activity_status
                , activity_date_time
                , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY activity_date_time DESC) AS rn
            FROM withdrawn_cte
            )
            WHERE rn = 1
          ),
          deactivation_cte AS (
              SELECT participant_id
                , event_id
                , MAX(event_authored_time) AS activity_date_time
                , MAX(CASE WHEN REPLACE(data_element_name, '\u200B', '') = 'activity_status' THEN data_element_value END) AS activity_status
              FROM `{project}.{src_operational_dataset}.ppsc_deactivation_event`
              WHERE LOWER(event_type_name) = 'deactivation' AND ignore_flag = 0
              GROUP BY 1, 2
          ),
          latest_deactivation AS (
              SELECT participant_id
                , activity_status AS deactivation_status
                , activity_date_time AS deactivation_time
              FROM (
                SELECT participant_id
                  , activity_status
                  , activity_date_time
                  , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY activity_date_time DESC) AS rn
                FROM deactivation_cte
              )
              WHERE rn = 1
          ),
          deceased_cte AS (
            SELECT participant_id
                , event_id
                , MAX(event_authored_time) AS activity_date_time
                , MAX(CASE WHEN REPLACE(data_element_name, '\u200B', '') = 'activity_status' THEN data_element_value END) AS activity_status
            FROM `{project}.{src_operational_dataset}.ppsc_participant_status_event`
            WHERE LOWER(event_type_name) = 'death' AND ignore_flag = 0
            GROUP BY 1, 2
          ),
          latest_deceased AS (
            SELECT participant_id
                , activity_status AS deceased_status
                , activity_date_time AS deceased_authored
            FROM (
                  SELECT participant_id
                    , activity_status
                    , activity_date_time
                    , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY activity_date_time DESC) AS rn
                FROM deceased_cte
            )
            WHERE rn = 1
          ),
          ehr_cte AS (
              SELECT participant_id
                , event_id
                , MAX(CASE WHEN data_element_name = 'activity_date_time' THEN data_element_value END) AS activity_date_time
                , MAX(CASE WHEN REPLACE(data_element_name, '\u200B', '') = 'activity_status' THEN data_element_value END) AS activity_status
              FROM `{project}.{src_operational_dataset}.ppsc_consent_event`
              WHERE event_type_name ='EHR Authorization' AND ignore_flag = 0
              GROUP BY 1, 2
          ),
          ehr_transformed_values AS (
            SELECT participant_id
              , event_id
              , SAFE_CAST(activity_date_time AS DATETIME) AS activity_date_time
              , CASE
                  WHEN LOWER(activity_status) = 'submitted_yes' THEN 'yes'
                  WHEN LOWER(activity_status) = 'submitted_no' THEN 'no'
                  ELSE LOWER(activity_status)
                END AS activity_status_cleaned
            FROM ehr_cte
          ),
          ehr_latest_submitted AS (
            SELECT participant_id
                , activity_status_cleaned AS consent_for_electronic_health_records
                , activity_date_time AS consent_for_electronic_health_records_authored
            FROM (
              SELECT participant_id
                , activity_status_cleaned
                , activity_date_time
                , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY activity_date_time DESC) AS rn
              FROM ehr_transformed_values
            )
            WHERE rn = 1
          ),
          ehr_first_yes_submitted AS (
            SELECT participant_id
                , MIN(activity_date_time) AS consent_for_electronic_health_records_first_yes_authored
            FROM ehr_transformed_values
            WHERE LOWER(activity_status_cleaned) = 'yes'
            GROUP BY 1
          ),
          primary_consent_cte AS (
            SELECT participant_id
                , event_id
                , MAX(event_authored_time) AS activity_date_time
                , MAX(CASE WHEN REPLACE(data_element_name, '\u200B', '') = 'activity_status' THEN data_element_value END) AS activity_status
            FROM `{project}.{src_operational_dataset}.ppsc_consent_event`
            WHERE event_type_name ='Primary Consent' AND ignore_flag = 0
            GROUP BY 1, 2
          ),
          primary_consent_cleaned_values AS (
            SELECT *
              , CASE
                  WHEN LOWER(activity_status) = 'submitted_yes' THEN 'yes'
                  WHEN LOWER(activity_status) = 'submitted_no' THEN 'no'
                  ELSE LOWER(activity_status)
                END AS activity_status_cleaned
            FROM primary_consent_cte
          ),
          primary_consent_latest_submitted AS (
              SELECT participant_id
                  , activity_status_cleaned AS consent_for_study_enrollment
                  , activity_date_time AS consent_for_study_enrollment_authored
              FROM (
                SELECT participant_id
                  , activity_status_cleaned
                  , activity_date_time
                  , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY activity_date_time DESC) AS rn
                FROM primary_consent_cleaned_values
              )
             WHERE rn = 1
          ),
          enrollment_status_cte AS (
            SELECT participant_id
                , data_element_name
                , MAX(CASE WHEN data_element_value IN ('0', '1') THEN data_element_value END) AS data_element_value
                , MAX(CASE WHEN data_element_value NOT IN ('0', '1') THEN data_element_value END) AS event_authored_time
            FROM `{project}.{src_operational_dataset}.ppsc_participant_status_event`
            WHERE LOWER(event_type_name) = 'enrollment status' AND ignore_flag = 0
              AND LOWER(data_element_name) IN
                ('registered', 'participant', 'participant_ehr_consent', 'enrolled', 'pmb_eligible', 'core_minus_pm', 'core_participant')
            GROUP BY 1, 2
          ),
          -- Get most recently received payload Enrollment Status event with a value of yes, but without a no after for that field name
          enrollment_status_recent_yes_ranked AS (
            SELECT es1.participant_id
              , es1.data_element_name
              , ROW_NUMBER() OVER (PARTITION BY es1.participant_id ORDER BY es1.event_authored_time DESC) AS rn
            FROM enrollment_status_cte es1
            LEFT JOIN enrollment_status_cte es2
            ON es1.participant_id = es2.participant_id
              AND es1.data_element_name = es2.data_element_name
              AND LOWER(es2.data_element_value) = 'no'
              AND es1.event_authored_time < es2.event_authored_time
            WHERE es2.participant_id IS NULL AND LOWER(es1.data_element_value) = '1'
          ),
          enrollment_status_recent_yes AS (
            SELECT participant_id
              , data_element_name AS enrollment_status
            FROM enrollment_status_recent_yes_ranked
            WHERE rn = 1
          ),
          participant_summary_cte AS (
            SELECT
              participant_id
              , ehr_receipt_time AS first_ehr_receipt_time
              , ehr_update_time AS latest_ehr_receipt_time
              , clinic_physical_measurements_status
              , clinic_physical_measurements_finalized_time
              , s1.google_group AS clinic_physical_measurements_finalized_site
              , self_reported_physical_measurements_status
              , self_reported_physical_measurements_authored
              , patient_status
              , s2.google_group AS biospecimen_source_site
              , biospecimen_order_time
              , biospecimen_status
              , sample_1sal2_collection_method
              , sample_status_1sal2
              , sample_order_status_1sal2
              , sample_order_status_1sal2_time
            FROM `{project}.{src_operational_dataset}.rdr_participant_summary` ps
            LEFT JOIN `{project}.{src_operational_dataset}.rdr_site` s1
            ON ps.clinic_physical_measurements_finalized_site_id = s1.site_id
            LEFT JOIN `{project}.{src_operational_dataset}.rdr_site` s2
            ON ps.biospecimen_source_site_id = s2.site_id
          ),
          default_filled_columns AS (
            SELECT
              participant_id
              , first_name
              , middle_name
              , last_name
              , zip_code
              , state
              , city
              , street_address
              , street_address2
              , phone_number
              , email
              , date_of_birth
              , organization
              , COALESCE(withdrawal_status, 'not_withdrawn') AS withdrawal_status
              , withdrawal_time
              , COALESCE(deactivation_status, 'not_deactivated') AS deactivation_status
              , deactivation_time
              , COALESCE(deceased_status, 'unset') AS deceased_status
              , deceased_authored
              , COALESCE(consent_for_electronic_health_records, 'no') AS consent_for_electronic_health_records
              , consent_for_electronic_health_records_authored
              , consent_for_electronic_health_records_first_yes_authored
              , first_ehr_receipt_time
              , latest_ehr_receipt_time
              , COALESCE(consent_for_study_enrollment, 'no') AS consent_for_study_enrollment
              , consent_for_study_enrollment_authored
              , enrollment_status
              , CASE clinic_physical_measurements_status
                  WHEN 0 THEN 'unset'
                  WHEN 1 THEN 'completed'
                  WHEN 2 THEN 'cancelled'
                  ELSE 'unset'
                END AS clinic_physical_measurements_status
              , clinic_physical_measurements_finalized_time
              , clinic_physical_measurements_finalized_site
              , CASE self_reported_physical_measurements_status
                  WHEN 0 THEN 'unset'
                  WHEN 1 THEN 'completed'
                  ELSE 'unset'
                END AS self_reported_physical_measurements_status
              , self_reported_physical_measurements_authored
              , COALESCE(patient_status, JSON_ARRAY()) AS patient_status
              , biospecimen_source_site
              , biospecimen_order_time
              , CASE biospecimen_status
                  WHEN 0 THEN 'unset'
                  WHEN 1 THEN 'created'
                  WHEN 2 THEN 'collected'
                  WHEN 3 THEN 'processed'
                  WHEN 4 THEN 'finalized'
                  ELSE 'unset'
                END AS biospecimen_status
              , CASE sample_1sal2_collection_method
                  WHEN 0 THEN 'unset'
                  WHEN 1 THEN 'mail_kit'
                  WHEN 2 THEN 'on_site'
                  ELSE 'unset'
                END AS sample_1sal2_collection_method
              , CASE sample_status_1sal2
                  WHEN 0 THEN 'unset'
                  WHEN 1 THEN 'received'
                  WHEN 10 THEN 'disposed'
                  WHEN 11 THEN 'consumed'
                  WHEN 12 THEN 'unknown'
                  WHEN 13 THEN 'sample_not_received'
                  WHEN 14 THEN 'sample_not_processed'
                  WHEN 15 THEN 'accessinging_error'
                  WHEN 16 THEN 'lab_accident'
                  WHEN 17 THEN 'qns_for_processing'
                  WHEN 18 THEN 'quality_issue'
                  ELSE 'unset'
                END AS sample_status_1sal2
              , CASE sample_order_status_1sal2
                  WHEN 0 THEN 'unset'
                  WHEN 1 THEN 'created'
                  WHEN 2 THEN 'collected'
                  WHEN 3 THEN 'processed'
                  WHEN 4 THEN 'finalized'
                  ELSE 'unset'
                END AS sample_order_status_1sal2
              , sample_order_status_1sal2_time
            FROM participant_cte
            LEFT JOIN profile_pivot
            USING (participant_id)
            LEFT JOIN latest_organization
            USING (participant_id)
            LEFT JOIN latest_withdrawn
            USING (participant_id)
            LEFT JOIN latest_deactivation
            USING (participant_id)
            LEFT JOIN latest_deceased
            USING (participant_id)
            LEFT JOIN ehr_latest_submitted
            USING (participant_id)
            LEFT JOIN ehr_first_yes_submitted
            USING (participant_id)
            LEFT JOIN primary_consent_latest_submitted
            USING (participant_id)
            LEFT JOIN enrollment_status_recent_yes
            USING (participant_id)
            LEFT JOIN participant_summary_cte
            USING (participant_id)
          ),
          withdrawn_update AS (
              SELECT
                participant_id,
                first_name,
                middle_name,
                last_name,
                IF(withdrawal_status = 'withdrawn', NULL, zip_code) AS zip_code,
                IF(withdrawal_status = 'withdrawn', NULL, state) AS state,
                IF(withdrawal_status = 'withdrawn', NULL, city) AS city,
                IF(withdrawal_status = 'withdrawn', NULL, street_address) AS street_address,
                IF(withdrawal_status = 'withdrawn', NULL, street_address2) AS street_address2,
                IF(withdrawal_status = 'withdrawn', NULL, phone_number) AS phone_number,
                IF(withdrawal_status = 'withdrawn', NULL, email) AS email,
                date_of_birth,
                organization,
                withdrawal_status,
                withdrawal_time,
                IF(withdrawal_status = 'withdrawn', 'unset', deactivation_status) AS deactivation_status,
                IF(withdrawal_status = 'withdrawn', NULL, deactivation_time) AS deactivation_time,
                IF(withdrawal_status = 'withdrawn', 'unset', deceased_status) AS deceased_status,
                IF(withdrawal_status = 'withdrawn', NULL, deceased_authored) AS deceased_authored,
                consent_for_electronic_health_records,
                consent_for_electronic_health_records_authored,
                IF(withdrawal_status = 'withdrawn', NULL, consent_for_electronic_health_records_first_yes_authored) AS consent_for_electronic_health_records_first_yes_authored,
                IF(withdrawal_status = 'withdrawn', NULL, first_ehr_receipt_time) AS first_ehr_receipt_time,
                IF(withdrawal_status = 'withdrawn', NULL, latest_ehr_receipt_time) AS latest_ehr_receipt_time,
                consent_for_study_enrollment,
                consent_for_study_enrollment_authored,
                enrollment_status,
                IF(withdrawal_status = 'withdrawn', 'unset', clinic_physical_measurements_status) AS clinic_physical_measurements_status,
                IF(withdrawal_status = 'withdrawn', NULL, clinic_physical_measurements_finalized_time) AS clinic_physical_measurements_finalized_time,
                IF(withdrawal_status = 'withdrawn', NULL, clinic_physical_measurements_finalized_site) AS clinic_physical_measurements_finalized_site,
                IF(withdrawal_status = 'withdrawn', 'unset', self_reported_physical_measurements_status) AS self_reported_physical_measurements_status,
                IF(withdrawal_status = 'withdrawn', NULL, self_reported_physical_measurements_authored) AS self_reported_physical_measurements_authored,
                IF(withdrawal_status = 'withdrawn', TO_JSON([]), patient_status) AS patient_status,
                IF(withdrawal_status = 'withdrawn', NULL, biospecimen_source_site) AS biospecimen_source_site,
                IF(withdrawal_status = 'withdrawn', NULL, biospecimen_order_time) AS biospecimen_order_time,
                IF(withdrawal_status = 'withdrawn', 'unset', biospecimen_status) AS biospecimen_status,
                IF(withdrawal_status = 'withdrawn', 'unset', sample_1sal2_collection_method) AS sample_1sal2_collection_method,
                IF(withdrawal_status = 'withdrawn', 'unset', sample_status_1sal2) AS sample_status_1sal2,
                IF(withdrawal_status = 'withdrawn', 'unset', sample_order_status_1sal2) AS sample_order_status_1sal2,
                IF(withdrawal_status = 'withdrawn', NULL, sample_order_status_1sal2_time) AS sample_order_status_1sal2_time
            FROM default_filled_columns
          ),
          -- creating surrogate key to detect changes
          final_result_with_surrogate_key AS (
            SELECT
                {AwardeeInSite.create_surrogate_key_sql()} AS surrogate_key
                , CURRENT_TIMESTAMP() AS created
                , *
            FROM withdrawn_update
          )

        SELECT *
        FROM final_result_with_surrogate_key fr
        WHERE NOT EXISTS (
            SELECT 1
            FROM `{project}.{destination_dataset}.datafeed_input_awardee_insite` staging_data
            WHERE staging_data.participant_id = fr.participant_id  -- to detect new pids
                AND staging_data.surrogate_key = fr.surrogate_key  -- to detect updated records
        );
    """
