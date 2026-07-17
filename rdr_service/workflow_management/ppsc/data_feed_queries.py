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
    participant_ehr.file_timestamp,
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
            AND t.event_date_time = participant_ehr.file_timestamp
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

def get_ppsc_ehr_to_stream(project: str, destination_dataset: str, start_date: str,
                           end_date: str, batch_size: int) -> str:
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
  AND event_date_time >= '{start_date}'
  AND event_date_time <= '{end_date}'
LIMIT {batch_size}
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
    curation_project = config.getSettingJson(config.CURATION_PROD_PROJECT)[0]

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
          , primary_language
          , gender_identity
          , awardee
          , is_ehr_data_available
          , aian
          , questionnaire_on_overall_health
          , questionnaire_on_overall_health_authored
          , questionnaire_on_lifestyle
          , questionnaire_on_lifestyle_authored
          , questionnaire_on_the_basics
          , questionnaire_on_the_basics_authored
          , questionnaire_on_healthcare_access
          , questionnaire_on_healthcare_access_authored
          , questionnaire_on_social_determinants_of_health
          , questionnaire_on_social_determinants_of_health_authored
          , questionnaire_on_personal_and_family_health_history
          , questionnaire_on_personal_and_family_health_history_authored
          , questionnaire_on_life_functioning
          , questionnaire_on_life_functioning_authored
          , questionnaire_on_emotional_health
          , questionnaire_on_emotional_health_authored
          , questionnaire_on_behavioral_health
          , questionnaire_on_behavioral_health_authored
          , questionnaire_on_social_factors_update
          , questionnaire_on_social_factors_update_authored
          , questionnaire_on_health_and_wellness_update
          , questionnaire_on_health_and_wellness_update_authored
          , questionnaire_on_mental_health_and_wellbeing_update
          , questionnaire_on_mental_health_and_wellbeing_update_authored
          , questionnaire_on_family_health_history_update
          , questionnaire_on_family_health_history_update_authored
          , questionnaire_on_pediatric_basics
          , questionnaire_on_pediatric_basics_authored
          , questionnaire_on_pediatric_overall_health
          , questionnaire_on_pediatric_overall_health_authored
          , questionnaire_on_pediatric_environmental_health
          , questionnaire_on_pediatric_environmental_health_authored
          , retention_eligible_status
          , retention_eligible_time
          , last_active_retention_activity_time
          , retention_type
          , sign_up_time
          , withdrawal_reason
          , duplicate_account_status
          , race
          , age_range
          , enrollment_status_time
        )
        WITH
          participant_cte AS (
            SELECT id AS participant_id
            , registered_date AS sign_up_time
            FROM `{project}.{src_operational_dataset}.ppsc_participant`
            WHERE ignore_flag = 0
          ),
          latest_profile_update_events AS (
            SELECT *
            , ROW_NUMBER() OVER(PARTITION BY participant_id, data_element_name ORDER BY event_authored_time DESC) AS rn
          FROM `{project}.{src_operational_dataset}.ppsc_profile_updates_event`
          WHERE ignore_flag = 0
          ),
          profile_pivot AS (
            SELECT participant_id
              , COALESCE(piiname_first,'') AS first_name
              , piiname_middle AS middle_name
              , COALESCE(piiname_last,'')  AS last_name
              , streetaddress_piizip AS zip_code
              , COALESCE(sm.state, streetaddress_piistate) AS state
              , streetaddress_piicity AS city
              , piiaddress_streetaddress AS street_address
              , piiaddress_streetaddress2 AS street_address2
              , piicontactinformation_phone AS phone_number
              , piicontactinformation_email AS email
              , piibirthinformation_birthdate AS date_of_birth
              , language_preference AS primary_language
            FROM
              (
                SELECT participant_id
                  , data_element_name
                  , data_element_value
                FROM latest_profile_update_events
                WHERE rn = 1
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
                      , 'language_preference'
                    )
                )
            LEFT JOIN `{project}.{src_operational_dataset}.state_mapping` sm
            ON sm.code_value = streetaddress_piistate
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
          latest_organization_cte AS (
              SELECT participant_id
                  , activity_status AS latest_organization
              FROM (
                SELECT participant_id
                  , activity_status
                  , activity_date_time
                  , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY activity_date_time DESC) AS rn
                FROM organization_cte
              )
              WHERE rn = 1
          ),
          hpo_cte AS (
            SELECT participant_id
                , h.name AS awardee
            FROM latest_organization_cte loc
            LEFT JOIN `{project}.{src_operational_dataset}.rdr_organization` o
            ON loc.latest_organization = o.external_id
            LEFT JOIN `{project}.{src_operational_dataset}.rdr_hpo` h
            ON o.hpo_id = h.hpo_id
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
            , TIMESTAMP_TRUNC(activity_date_time, SECOND) AS withdrawal_time
            FROM (
              SELECT participant_id
                , activity_status
                , activity_date_time
                , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY activity_date_time DESC) AS rn
            FROM withdrawn_cte
            )
            WHERE rn = 1
          ),
          withdrawal_reason_cte AS (
            SELECT participant_id
            , data_element_value AS withdrawal_reason
            FROM (
              SELECT participant_id
                , data_element_value
                , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY created DESC) AS rn
              FROM `{project}.{src_operational_dataset}.ppsc_withdrawal_event`
              WHERE LOWER(data_element_name) = 'withdrawal_reason' AND ignore_flag = 0
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
          earliest_deactivation AS (
              SELECT participant_id
                , activity_status AS deactivation_status
                , TIMESTAMP_TRUNC(activity_date_time, SECOND) AS deactivation_time
              FROM (
                SELECT participant_id
                  , activity_status
                  , activity_date_time
                  , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY activity_date_time ASC) AS rn
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
                , TIMESTAMP_TRUNC(activity_date_time, SECOND) AS deceased_authored
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
                , MAX(event_authored_time) AS activity_date_time
                , MAX(CASE WHEN REPLACE(data_element_name, '\u200B', '') = 'activity_status' THEN data_element_value END) AS activity_status
              FROM `{project}.{src_operational_dataset}.ppsc_consent_event`
              WHERE event_type_name IN ('EHR Authorization', 'Pediatric EHR Authorization') AND ignore_flag = 0
              GROUP BY 1, 2
          ),
          ehr_transformed_values AS (
            SELECT participant_id
              , event_id
              , SAFE_CAST(activity_date_time AS DATETIME) AS activity_date_time
              , CASE
                  WHEN LOWER(activity_status) = 'submitted_yes' THEN 'yes'
                  WHEN LOWER(activity_status) = 'submitted_complete' THEN 'yes'
                  WHEN LOWER(activity_status) = 'submitted_no' THEN 'no'
                  ELSE LOWER(activity_status)
                END AS activity_status_cleaned
            FROM ehr_cte
          ),
          ehr_latest_submitted AS (
            SELECT participant_id
                , activity_status_cleaned AS consent_for_electronic_health_records
                , TIMESTAMP_TRUNC(activity_date_time, SECOND) AS consent_for_electronic_health_records_authored
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
              FROM ehr_cte
              WHERE LOWER(activity_status) IN ('yes', 'submitted_yes', 'submitted_complete')
              GROUP BY 1
          ),
          primary_consent_cte AS (
            SELECT participant_id
                , event_id
                , MAX(event_authored_time) AS activity_date_time
                , MAX(CASE WHEN REPLACE(data_element_name, '\u200B', '') = 'activity_status' THEN data_element_value END) AS activity_status
            FROM `{project}.{src_operational_dataset}.ppsc_consent_event`
            WHERE event_type_name IN ('Primary Consent', 'Pediatric Permission') AND ignore_flag = 0
            GROUP BY 1, 2
          ),
          primary_consent_cleaned_values AS (
            SELECT *
              , CASE
                  WHEN LOWER(activity_status) = 'submitted_yes' THEN 'yes'
                  WHEN LOWER(activity_status) = 'submitted_complete' THEN 'yes'
                  WHEN LOWER(activity_status) = 'submitted_no' THEN 'no'
                  ELSE LOWER(activity_status)
                END AS activity_status_cleaned
            FROM primary_consent_cte
          ),
          primary_consent_latest_submitted AS (
              SELECT participant_id
                  , activity_status_cleaned AS consent_for_study_enrollment
              FROM (
                SELECT participant_id
                  , activity_status_cleaned
                  , activity_date_time
                  , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY activity_date_time DESC) AS rn
                FROM primary_consent_cleaned_values
              )
             WHERE rn = 1
          ),
          physical_measurement_cte AS (
            SELECT physical_measurements_id
                , participant_id
                , collect_type
                , status
                , finalized
                , finalized_site_id
                , created
                , log_position_id
            FROM `{project}.{src_operational_dataset}.rdr_physical_measurements`
            order by participant_id, created desc, log_position_id desc
          ),
          physical_measurement_latest_uncancelled AS (
            SELECT clinic_physical_measurements_id
                , participant_id
                , finalized AS clinic_physical_measurements_finalized_time
                , site.google_group AS clinic_physical_measurements_finalized_site
            FROM (
              SELECT physical_measurements_id AS clinic_physical_measurements_id
                , participant_id
                , finalized
                , created
                , log_position_id
                , finalized_site_id
                , ROW_NUMBER() OVER(
                    PARTITION BY participant_id
                    ORDER BY created DESC, log_position_id DESC
                ) AS rn
              FROM physical_measurement_cte
              WHERE (status is null or status != 2) and collect_type = 1
            ) measurement
            LEFT JOIN `{project}.{src_operational_dataset}.rdr_site` site
            ON site.site_id = measurement.finalized_site_id
            WHERE rn = 1
          ),
          physical_measurement_clinic_cancelled AS (
            SELECT cancelled_measurement_id
                , participant_id
            FROM (
              SELECT physical_measurements_id AS cancelled_measurement_id
                , participant_id
                , finalized
                , created
                , log_position_id
                , ROW_NUMBER() OVER(
                    PARTITION BY participant_id
                    ORDER BY created, log_position_id DESC
                ) AS rn
              FROM physical_measurement_cte
              WHERE (status = 2) and collect_type = 1
            ) clinic_measurement
            WHERE rn = 1
          ),
          physical_measurement_latest_self_reported AS (
            SELECT self_reported_physical_measurements_id
                , participant_id
                , self_reported_physical_measurements_authored
            FROM (
              SELECT physical_measurements_id AS self_reported_physical_measurements_id
                , participant_id
                , finalized AS self_reported_physical_measurements_authored
                , created
                , log_position_id
                , ROW_NUMBER() OVER(
                    PARTITION BY participant_id
                    ORDER BY created desc, log_position_id DESC
                ) AS rn
              FROM physical_measurement_cte
              WHERE collect_type = 2
            ) self_reported
            WHERE rn = 1
          ),
          enrollment_status_mapping AS (
            SELECT 1 AS enrollment_status_rank, "registered" AS status
            UNION ALL
            SELECT 2 AS enrollment_status_rank, "participant" AS status
            UNION ALL
            SELECT 3 AS enrollment_status_rank, "participant_ehr_consent" AS status
            UNION ALL
            SELECT 4 AS enrollment_status_rank, "enrolled" AS status
            UNION ALL
            SELECT 5 AS enrollment_status_rank, "pmb_eligible" AS status
            UNION ALL
            SELECT 6 AS enrollment_status_rank, "core_minus_pm" AS status
            UNION ALL
            SELECT 7 AS enrollment_status_rank, "core_participant" AS status
          ),
          enrollment_status_cte as (
            SELECT
            participant_id
            , data_element_name
            , data_element_value
            FROM `{project}.{src_operational_dataset}.ppsc_participant_status_event`
            WHERE  ignore_flag = 0
            AND event_type_name = "Enrollment Status"
            AND LOWER(data_element_name) IN
             (
                'registered', 'registered_date_time',
                'participant', 'participant_date_time',
                'participant_ehr_consent', 'participant_ehr_consent_date_time',
                'enrolled', 'enrolled_date_time',
                'pmb_eligible', 'pmb_eligible_date_time',
                'core_minus_pm', 'core_minus_pm_date_time',
                'core_participant', 'core_participant_date_time'
                )
          ),
          enrollment_status_transformed AS (
            SELECT e1.participant_id
              , e1.data_element_value AS event_authored
              , e2.data_element_name AS enrollment_status
              , e2.data_element_value as data_element_value
            FROM enrollment_status_cte AS e1 LEFT JOIN enrollment_status_cte AS e2
            ON e1.participant_id = e2.participant_id AND REGEXP_REPLACE(e1.data_element_name, "_date_time", "")  = e2.data_element_name
            WHERE e1.data_element_name LIKE "%date_time%" and e2.data_element_value = "yes"
          ),
          enrollment_status_drop_dupes AS (
              SELECT *
              FROM (
                SELECT *
                , ROW_NUMBER() OVER (PARTITION BY participant_id, enrollment_status ORDER BY event_authored DESC) AS rn
              FROM enrollment_status_transformed
              )
            WHERE rn = 1
          ),
          latest_enrollement_status AS (
            SELECT participant_id
              , enrollment_status
              , SAFE_CAST(REPLACE(event_authored, 'Z', '') AS DATETIME) AS enrollment_status_time
            FROM (
              SELECT * except(rn)
                , ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY map.enrollment_status_rank DESC) AS rn
              FROM enrollment_status_drop_dupes esdd LEFT JOIN enrollment_status_mapping map
              ON esdd.enrollment_status = map.status
              )
            WHERE rn = 1
          ),
          -- 2 BQ jobs are run daily in curation project."materialize_ehr_uploads_pids_view_into_table" changes the view
          -- to a table & "copy_rdr_operational_across_regions" moves the dataset from US to uscentral1 so it can be
          -- queried here
          latest_ehr_receipt_time_cte AS (
            SELECT person_id
            , MAX(CAST(FORMAT_TIMESTAMP("%Y-%m-%dT%H:%M:%S", latest_upload_time) AS DATETIME)) AS latest_ehr_receipt_time
            FROM `{curation_project}.rdr_operational_us_central.ehr_upload_pids`
            GROUP BY person_id
          ),
          participant_summary_cte AS (
            SELECT
              participant_id
              , ehr_receipt_time AS first_ehr_receipt_time
              , consent_for_study_enrollment_authored
              , patient_status
              , s2.google_group AS biospecimen_source_site
              , biospecimen_order_time
              , biospecimen_status
              , sample_1sal2_collection_method
              , sample_status_1sal2
              , sample_order_status_1sal2
              , sample_order_status_1sal2_time
              , o.external_id AS ps_organization
            FROM `{project}.{src_operational_dataset}.rdr_participant_summary` ps
            LEFT JOIN `{project}.{src_operational_dataset}.rdr_site` s2
            ON ps.biospecimen_source_site_id = s2.site_id
            LEFT JOIN `{project}.{src_operational_dataset}.rdr_organization` o
            ON ps.organization_id = o.organization_id
          ),
          latest_gender_identity AS (
              SELECT participant_id
                , data_element_value AS gender_identity
              FROM (
                SELECT *
                  , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY event_authored_time DESC) AS rn
                FROM `{project}.{src_operational_dataset}.ppsc_survey_completion_event`
                WHERE LOWER(event_type_name) = 'basics data'
                  AND LOWER(data_element_name) ='gender_genderidentity'
                  AND ignore_flag = 0
              )
              WHERE rn = 1
          ),
          aian_cte AS (
            SELECT DISTINCT participant_id
                , 'yes' AS aian
            FROM `{project}.{src_operational_dataset}.ppsc_survey_completion_event`
            WHERE LOWER(data_element_name) IN ('race_whatraceethnicity', 'race_whatraceethnicity_ped')
              AND LOWER(data_element_value) = 'whatraceethnicity_aian'
              AND ignore_flag = 0
          ),
          survey_completion_cte AS (
              SELECT
                  participant_id
                  , event_type_name
                  , event_id
                  , MAX(
                      SAFE_CAST(event_authored_time AS DATETIME)
                    ) AS event_authored_time
                  , MAX(
                      CASE
                        WHEN data_element_name = 'activity_status'
                        THEN data_element_value
                    END
                  ) AS activity_status
              FROM `{project}.{src_operational_dataset}.ppsc_survey_completion_event`
              WHERE LOWER(event_type_name) IN (
                'overall health',
                'lifestyle',
                'the basics',
                'health care access & utilization',
                'social determinants of health',
                'personal and family health history',
                'life functioning survey',
                'emotional health history and well-being',
                'behavioral health & personality',
                'social factors update',
                'health and wellness update',
                'mental health and wellbeing update',
                'personal and family health history update',
                'pediatric basics 0to6',
                'pediatric overall health 0to6',
                'pediatric environmental health 0to6'
              )
              AND ignore_flag = 0
              GROUP BY participant_id, event_type_name, event_id
          ),
          survey_completion_latest_submitted AS (
              SELECT * EXCEPT (event_id, rn)
              FROM (
                SELECT *
                  , ROW_NUMBER() OVER(PARTITION BY participant_id, event_type_name ORDER BY event_authored_time DESC) AS rn
                FROM survey_completion_cte
              )
              WHERE rn = 1
          ),
          survey_completion_pivot AS (
            SELECT participant_id
              -- overall health
              , MAX(CASE WHEN LOWER(event_type_name) = 'overall health' THEN activity_status END) AS questionnaire_on_overall_health
              , MAX(CASE WHEN LOWER(event_type_name) = 'overall health' THEN event_authored_time END) AS questionnaire_on_overall_health_authored

              -- lifestyle
              , MAX(CASE WHEN LOWER(event_type_name) = 'lifestyle' THEN activity_status END) AS questionnaire_on_lifestyle
              , MAX(CASE WHEN LOWER(event_type_name) = 'lifestyle' THEN event_authored_time END) AS questionnaire_on_lifestyle_authored

              -- the basics
              , MAX(CASE WHEN LOWER(event_type_name) = 'the basics' THEN activity_status END) AS questionnaire_on_the_basics
              , MAX(CASE WHEN LOWER(event_type_name) = 'the basics' THEN event_authored_time END) AS questionnaire_on_the_basics_authored

              -- health care access & utilization
              , MAX(CASE WHEN LOWER(event_type_name) = 'health care access & utilization' THEN activity_status END) AS questionnaire_on_healthcare_access
              , MAX(CASE WHEN LOWER(event_type_name) = 'health care access & utilization' THEN event_authored_time END) AS questionnaire_on_healthcare_access_authored

              -- social determinants of health
              , MAX(CASE WHEN LOWER(event_type_name) = 'social determinants of health' THEN activity_status END) AS questionnaire_on_social_determinants_of_health
              , MAX(CASE WHEN LOWER(event_type_name) = 'social determinants of health' THEN event_authored_time END) AS questionnaire_on_social_determinants_of_health_authored

              -- personal and family health history
              , MAX(CASE WHEN LOWER(event_type_name) = 'personal and family health history' THEN activity_status END) AS questionnaire_on_personal_and_family_health_history
              , MAX(CASE WHEN LOWER(event_type_name) = 'personal and family health history' THEN event_authored_time END) AS questionnaire_on_personal_and_family_health_history_authored

              -- life functioning survey
              , MAX(CASE WHEN LOWER(event_type_name) = 'life functioning survey' THEN activity_status END) AS questionnaire_on_life_functioning
              , MAX(CASE WHEN LOWER(event_type_name) = 'life functioning survey' THEN event_authored_time END) AS questionnaire_on_life_functioning_authored

              -- emotional health history and well-being
              , MAX(CASE WHEN LOWER(event_type_name) = 'emotional health history and well-being' THEN activity_status END) AS questionnaire_on_emotional_health
              , MAX(CASE WHEN LOWER(event_type_name) = 'emotional health history and well-being' THEN event_authored_time END) AS questionnaire_on_emotional_health_authored

              -- behavioral health and personality
              , MAX(CASE WHEN LOWER(event_type_name) = 'behavioral health & personality' THEN activity_status END) AS questionnaire_on_behavioral_health
              , MAX(CASE WHEN LOWER(event_type_name) = 'behavioral health & personality' THEN event_authored_time END) AS questionnaire_on_behavioral_health_authored

              -- social factors update
              , MAX(CASE WHEN LOWER(event_type_name) = 'social factors update' THEN activity_status END) AS questionnaire_on_social_factors_update
              , MAX(CASE WHEN LOWER(event_type_name) = 'social factors update' THEN event_authored_time END) AS questionnaire_on_social_factors_update_authored

              -- health and wellness update
              , MAX(CASE WHEN LOWER(event_type_name) = 'health and wellness update' THEN activity_status END) AS questionnaire_on_health_and_wellness_update
              , MAX(CASE WHEN LOWER(event_type_name) = 'health and wellness update' THEN event_authored_time END) AS questionnaire_on_health_and_wellness_update_authored

              -- mental health and wellbeing update
              , MAX(CASE WHEN LOWER(event_type_name) = 'mental health and wellbeing update' THEN activity_status END) AS questionnaire_on_mental_health_and_wellbeing_update
              , MAX(CASE WHEN LOWER(event_type_name) = 'mental health and wellbeing update' THEN event_authored_time END) AS questionnaire_on_mental_health_and_wellbeing_update_authored

              -- personal and family health history update
              , MAX(CASE WHEN LOWER(event_type_name) = 'personal and family health history update' THEN activity_status END) AS questionnaire_on_family_health_history_update
              , MAX(CASE WHEN LOWER(event_type_name) = 'personal and family health history update' THEN event_authored_time END) AS questionnaire_on_family_health_history_update_authored

              -- pediatric basics 0 to 6
              , MAX(CASE WHEN LOWER(event_type_name) = 'pediatric basics 0to6' THEN activity_status END) AS questionnaire_on_pediatric_basics
              , MAX(CASE WHEN LOWER(event_type_name) = 'pediatric basics 0to6' THEN event_authored_time END) AS questionnaire_on_pediatric_basics_authored

              -- pediatric overall health 0 to 6
              , MAX(CASE WHEN LOWER(event_type_name) = 'pediatric overall health 0to6' THEN activity_status END) AS questionnaire_on_pediatric_overall_health
              , MAX(CASE WHEN LOWER(event_type_name) = 'pediatric overall health 0to6' THEN event_authored_time END) AS questionnaire_on_pediatric_overall_health_authored

              -- pediatric environmental health 0 to 6
              , MAX(CASE WHEN LOWER(event_type_name) = 'pediatric environmental health 0to6' THEN activity_status END) AS questionnaire_on_pediatric_environmental_health
              , MAX(CASE WHEN LOWER(event_type_name) = 'pediatric environmental health 0to6' THEN event_authored_time END) AS questionnaire_on_pediatric_environmental_health_authored
            FROM survey_completion_latest_submitted
            GROUP BY participant_id
          ),
          retention_cte AS (
            SELECT participant_id
              , MAX(CASE WHEN data_element_name = 'activity_status' THEN data_element_value END) AS retention_eligible_status
              , SAFE_CAST(REPLACE(MAX(CASE WHEN data_element_name = 'activity_date_time' THEN data_element_value END), 'Z', '') AS DATETIME) AS retention_eligible_time
              , LOWER(MAX(CASE WHEN data_element_name = 'retention_type' THEN data_element_value END)) AS retention_type
              , SAFE_CAST(REPLACE(MAX(CASE WHEN data_element_name = 'last_retention_activity_date_time' THEN data_element_value END), 'Z', '') AS DATETIME) AS last_active_retention_activity_time
            FROM (
                SELECT *
                , DENSE_RANK() OVER(PARTITION BY participant_id ORDER BY event_id DESC) AS rn
                FROM `{project}.{src_operational_dataset}.ppsc_participant_status_event`
                WHERE LOWER(event_type_name) = 'retention status' AND ignore_flag = 0
            )
            WHERE rn = 1
            GROUP BY 1
          ),
          duplicate_account_cte AS (
            SELECT participant_id
              , CASE WHEN LOWER(data_element_value) = 'duplicate' THEN 'yes' END AS duplicate_account_status
            FROM `{project}.{src_operational_dataset}.ppsc_participant_status_event`
            WHERE LOWER(event_type_name) = 'duplicate account' AND ignore_flag = 0
          ),
          race_cte AS (
            SELECT participant_id
            , CASE
                WHEN LOWER(data_element_name) = 'whatraceethnicity_raceethnicitynoneofthese' THEN 'other_race'
                ELSE REPLACE(data_element_value, 'WhatRaceEthnicity_', '')
              END AS race
            FROM (
              SELECT *
                -- doing order by id since there are a lot of duplicates, where event_id, event_authored_time is the same but the race is different
                , ROW_NUMBER() OVER(PARTITION BY participant_id ORDER BY id DESC) AS rn
              FROM `{project}.{src_operational_dataset}.ppsc_survey_completion_event`
              WHERE LOWER(data_element_name) IN ('race_whatraceethnicity', 'whatraceethnicity_raceethnicitynoneofthese', 'race_whatraceethnicity_ped')
            )
            WHERE rn = 1
          ),
          age_range_cte AS (
            SELECT participant_id
                , CASE
                    WHEN age BETWEEN 0 AND 6 THEN '0-6'
                    WHEN age BETWEEN 7 AND 12 THEN '7-12'
                    WHEN age BETWEEN 13 AND 17 THEN '13-17'
                    WHEN age BETWEEN 18 AND 25 THEN '18-25'
                    WHEN age BETWEEN 26 AND 35 THEN '26-35'
                    WHEN age BETWEEN 36 AND 45 THEN '36-45'
                    WHEN age BETWEEN 46 AND 55 THEN '46-55'
                    WHEN age BETWEEN 56 AND 65 THEN '56-65'
                    WHEN age BETWEEN 66 AND 75 THEN '66-75'
                    WHEN age BETWEEN 76 AND 85 THEN '76-85'
                    WHEN age >= 86 THEN '86+'
                  END AS age_range
            FROM (
              SELECT participant_id
              , date_of_birth
              , DATE_DIFF(CURRENT_DATE(), SAFE_CAST(date_of_birth AS DATE), YEAR) - IF(EXTRACT(MONTH FROM SAFE_CAST(date_of_birth AS DATE)) * 100 + EXTRACT(DAY FROM SAFE_CAST(date_of_birth AS DATE)) > EXTRACT(MONTH FROM CURRENT_DATE()) * 100 + EXTRACT(DAY FROM CURRENT_DATE()), 1, 0) AS age
              FROM profile_pivot
            )
          ),
          default_filled_columns AS (
            SELECT
              participant_id
              , sign_up_time
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
              , primary_language
              , COALESCE(latest_organization, ps_organization) AS organization
              , COALESCE(withdrawal_status, 'not_withdrawn') AS withdrawal_status
              , withdrawal_time
              , withdrawal_reason
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
              , COALESCE(enrollment_status, 'registered') AS enrollment_status
              , enrollment_status_time
              , CASE
                  WHEN clinic_physical_measurements_id IS NOT NULL THEN 'completed'
                  WHEN cancelled_measurement_id IS NOT NULL THEN 'cancelled'
                  ELSE 'unset'
                END AS clinic_physical_measurements_status
              , clinic_physical_measurements_finalized_time
              , clinic_physical_measurements_finalized_site
              , CASE self_reported_physical_measurements_id IS NOT NULL
                  WHEN False THEN 'unset'
                  WHEN True THEN 'completed'
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
              , gender_identity
              , COALESCE(awardee, 'unset') AS awardee
              , CASE
                    WHEN lertc.latest_ehr_receipt_time IS NOT NULL OR LOWER(consent_for_electronic_health_records) = 'yes' THEN 'yes'
                    ELSE 'no'
                END AS is_ehr_data_available
              , COALESCE(aian, 'no') AS aian
              , questionnaire_on_overall_health
              , questionnaire_on_overall_health_authored
              , questionnaire_on_lifestyle
              , questionnaire_on_lifestyle_authored
              , questionnaire_on_the_basics
              , questionnaire_on_the_basics_authored
              , questionnaire_on_healthcare_access
              , questionnaire_on_healthcare_access_authored
              , questionnaire_on_social_determinants_of_health
              , questionnaire_on_social_determinants_of_health_authored
              , questionnaire_on_personal_and_family_health_history
              , questionnaire_on_personal_and_family_health_history_authored
              , questionnaire_on_life_functioning
              , questionnaire_on_life_functioning_authored
              , questionnaire_on_emotional_health
              , questionnaire_on_emotional_health_authored
              , questionnaire_on_behavioral_health
              , questionnaire_on_behavioral_health_authored
              , questionnaire_on_social_factors_update
              , questionnaire_on_social_factors_update_authored
              , questionnaire_on_health_and_wellness_update
              , questionnaire_on_health_and_wellness_update_authored
              , questionnaire_on_mental_health_and_wellbeing_update
              , questionnaire_on_mental_health_and_wellbeing_update_authored
              , questionnaire_on_family_health_history_update
              , questionnaire_on_family_health_history_update_authored
              , questionnaire_on_pediatric_basics
              , questionnaire_on_pediatric_basics_authored
              , questionnaire_on_pediatric_overall_health
              , questionnaire_on_pediatric_overall_health_authored
              , questionnaire_on_pediatric_environmental_health
              , questionnaire_on_pediatric_environmental_health_authored
              , retention_eligible_status
              , retention_eligible_time
              , last_active_retention_activity_time
              , COALESCE(retention_type, 'unset') AS retention_type
              , COALESCE(duplicate_account_status, 'no') AS duplicate_account_status
              , COALESCE(race, 'unset') AS race
              , age_range
            FROM participant_cte
            LEFT JOIN profile_pivot
            USING (participant_id)
            LEFT JOIN latest_withdrawn
            USING (participant_id)
            LEFT JOIN withdrawal_reason_cte
            USING (participant_id)
            LEFT JOIN earliest_deactivation
            USING (participant_id)
            LEFT JOIN latest_deceased
            USING (participant_id)
            LEFT JOIN ehr_latest_submitted
            USING (participant_id)
            LEFT JOIN ehr_first_yes_submitted
            USING (participant_id)
            LEFT JOIN primary_consent_latest_submitted
            USING (participant_id)
            LEFT JOIN participant_summary_cte
            USING (participant_id)
            LEFT JOIN physical_measurement_latest_uncancelled
            USING (participant_id)
            LEFT JOIN physical_measurement_clinic_cancelled
            USING (participant_id)
            LEFT JOIN physical_measurement_latest_self_reported
            USING (participant_id)
            LEFT JOIN latest_enrollement_status
            USING (participant_id)
            LEFT JOIN latest_ehr_receipt_time_cte lertc
            ON participant_cte.participant_id = lertc.person_id
            LEFT JOIN latest_organization_cte
            USING (participant_id)
            LEFT JOIN latest_gender_identity
            USING (participant_id)
            LEFT JOIN hpo_cte
            USING (participant_id)
            LEFT JOIN aian_cte
            USING (participant_id)
            LEFT JOIN survey_completion_pivot
            USING (participant_id)
            LEFT JOIN retention_cte
            USING (participant_id)
            LEFT JOIN duplicate_account_cte
            USING (participant_id)
            LEFT JOIN race_cte
            USING (participant_id)
            LEFT JOIN age_range_cte
            USING (participant_id)
          ),
          withdrawn_update AS (
              SELECT
                participant_id,
                IF(withdrawal_status = 'withdrawn', NULL, sign_up_time) AS sign_up_time,
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
                IF(withdrawal_status = 'withdrawn', NULL, primary_language) AS primary_language,
                organization,
                withdrawal_status,
                withdrawal_time,
                withdrawal_reason,
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
                IF(withdrawal_status = 'withdrawn', NULL, enrollment_status_time) AS enrollment_status_time,
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
              , IF(withdrawal_status = 'withdrawn', NULL, gender_identity) AS gender_identity
              , awardee
              , IF(withdrawal_status = 'withdrawn', NULL, is_ehr_data_available) AS is_ehr_data_available
              , IF(withdrawal_status = 'withdrawn', NULL, aian) AS aian
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_overall_health) AS questionnaire_on_overall_health
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_overall_health_authored) AS questionnaire_on_overall_health_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_lifestyle) AS questionnaire_on_lifestyle
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_lifestyle_authored) AS questionnaire_on_lifestyle_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_the_basics) AS questionnaire_on_the_basics
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_the_basics_authored) AS questionnaire_on_the_basics_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_healthcare_access) AS questionnaire_on_healthcare_access
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_healthcare_access_authored) AS questionnaire_on_healthcare_access_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_social_determinants_of_health) AS questionnaire_on_social_determinants_of_health
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_social_determinants_of_health_authored) AS questionnaire_on_social_determinants_of_health_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_personal_and_family_health_history) AS questionnaire_on_personal_and_family_health_history
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_personal_and_family_health_history_authored) AS questionnaire_on_personal_and_family_health_history_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_life_functioning) AS questionnaire_on_life_functioning
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_life_functioning_authored) AS questionnaire_on_life_functioning_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_emotional_health) AS questionnaire_on_emotional_health
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_emotional_health_authored) AS questionnaire_on_emotional_health_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_behavioral_health) AS questionnaire_on_behavioral_health
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_behavioral_health_authored) AS questionnaire_on_behavioral_health_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_social_factors_update) AS questionnaire_on_social_factors_update
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_social_factors_update_authored) AS questionnaire_on_social_factors_update_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_health_and_wellness_update) AS questionnaire_on_health_and_wellness_update
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_health_and_wellness_update_authored) AS questionnaire_on_health_and_wellness_update_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_mental_health_and_wellbeing_update) AS questionnaire_on_mental_health_and_wellbeing_update
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_mental_health_and_wellbeing_update_authored) AS questionnaire_on_mental_health_and_wellbeing_update_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_family_health_history_update) AS questionnaire_on_family_health_history_update
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_family_health_history_update_authored) AS questionnaire_on_family_health_history_update_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_pediatric_basics) AS questionnaire_on_pediatric_basics
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_pediatric_basics_authored) AS questionnaire_on_pediatric_basics_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_pediatric_overall_health) AS questionnaire_on_pediatric_overall_health
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_pediatric_overall_health_authored) AS questionnaire_on_pediatric_overall_health_authored
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_pediatric_environmental_health) AS questionnaire_on_pediatric_environmental_health
              , IF(withdrawal_status = 'withdrawn', NULL, questionnaire_on_pediatric_environmental_health_authored) AS questionnaire_on_pediatric_environmental_health_authored
              , IF(withdrawal_status = 'withdrawn', NULL, retention_eligible_status) AS retention_eligible_status
              , IF(withdrawal_status = 'withdrawn', NULL, retention_eligible_time) AS retention_eligible_time
              , IF(withdrawal_status = 'withdrawn', NULL, last_active_retention_activity_time) AS last_active_retention_activity_time
              , IF(withdrawal_status = 'withdrawn', NULL, retention_type) AS retention_type
              , IF(withdrawal_status = 'withdrawn', NULL, duplicate_account_status) AS duplicate_account_status
              , IF(withdrawal_status = 'withdrawn', NULL, race) AS race
              , IF(withdrawal_status = 'withdrawn', NULL, age_range) AS age_range
            FROM default_filled_columns
          ),
          -- creating surrogate key to detect changes
          final_result_with_surrogate_key AS (
            SELECT
                {AwardeeInSite.create_surrogate_key_sql()} AS surrogate_key
                , CURRENT_TIMESTAMP() AS created
                , *
            FROM withdrawn_update
          ),
          latest_datafeed_records AS (
              SELECT * EXCEPT(rn)
              FROM (
                SELECT *
                , ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY created DESC) AS rn
                FROM `{project}.{destination_dataset}.datafeed_input_awardee_insite`
              )
              WHERE rn = 1
          )

        SELECT surrogate_key
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
          , primary_language
          , gender_identity
          , awardee
          , is_ehr_data_available
          , aian
          , questionnaire_on_overall_health
          , questionnaire_on_overall_health_authored
          , questionnaire_on_lifestyle
          , questionnaire_on_lifestyle_authored
          , questionnaire_on_the_basics
          , questionnaire_on_the_basics_authored
          , questionnaire_on_healthcare_access
          , questionnaire_on_healthcare_access_authored
          , questionnaire_on_social_determinants_of_health
          , questionnaire_on_social_determinants_of_health_authored
          , questionnaire_on_personal_and_family_health_history
          , questionnaire_on_personal_and_family_health_history_authored
          , questionnaire_on_life_functioning
          , questionnaire_on_life_functioning_authored
          , questionnaire_on_emotional_health
          , questionnaire_on_emotional_health_authored
          , questionnaire_on_behavioral_health
          , questionnaire_on_behavioral_health_authored
          , questionnaire_on_social_factors_update
          , questionnaire_on_social_factors_update_authored
          , questionnaire_on_health_and_wellness_update
          , questionnaire_on_health_and_wellness_update_authored
          , questionnaire_on_mental_health_and_wellbeing_update
          , questionnaire_on_mental_health_and_wellbeing_update_authored
          , questionnaire_on_family_health_history_update
          , questionnaire_on_family_health_history_update_authored
          , questionnaire_on_pediatric_basics
          , questionnaire_on_pediatric_basics_authored
          , questionnaire_on_pediatric_overall_health
          , questionnaire_on_pediatric_overall_health_authored
          , questionnaire_on_pediatric_environmental_health
          , questionnaire_on_pediatric_environmental_health_authored
          , retention_eligible_status
          , retention_eligible_time
          , last_active_retention_activity_time
          , retention_type
          , sign_up_time
          , withdrawal_reason
          , duplicate_account_status
          , race
          , age_range
          , enrollment_status_time
        FROM final_result_with_surrogate_key fr
        WHERE NOT EXISTS (
            SELECT 1
            FROM latest_datafeed_records staging_data
            WHERE staging_data.participant_id = fr.participant_id  -- to detect new pids
                AND staging_data.surrogate_key = fr.surrogate_key  -- to detect updated records
        );
    """
