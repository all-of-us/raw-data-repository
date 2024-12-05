def get_consent_activity_to_stream(project: str,
                                              source_dataset: str,
                                              temp_table_name: str,
                                              sent_table_name: str) -> str:
    return f"""
CREATE OR REPLACE TABLE `{project}.{source_dataset}.{temp_table_name}` AS
WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC, se.event_id DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_consent_event` ce
  WHERE TRUE
  AND NOT EXISTS (
    SELECT 1
      FROM `{project}.{source_dataset}.{sent_table_name}` sent
      WHERE sent.participant_id = se.participant_id
        AND sent.event_id >= se.event_id
        AND sent.event_type_name = se.event_type_name
  )
  and data_element_name IN ("activity_status", '​activity_status')
)
SELECT
  participant_id,
  event_id,
  event_type_name,
  MAX(CASE WHEN event_type_name = 'EHR Authorization' AND data_element_name = 'activity_status' AND rank = 1
           THEN data_element_value END) AS ehr_authorization,
  MAX(CASE WHEN event_type_name = 'EHR Authorization' AND rank = 1
           THEN event_authored_time END) AS ehr_authorization_event_authored_time,
  MAX(CASE WHEN event_type_name = 'Primary Consent' AND data_element_name = 'activity_status' AND rank = 1
           THEN data_element_value END) AS primary_consent,
  MAX(CASE WHEN event_type_name = 'Primary Consent' AND rank = 1
           THEN event_authored_time END) AS primary_consent_event_authored_time
FROM ranked_events
WHERE rank = 1
GROUP BY participant_id, event_id, event_type_name
    """


def get_profile_updates_activity_to_stream(project: str,
                                              source_dataset: str,
                                              temp_table_name: str,
                                              sent_table_name: str) -> str:
    return f"""
CREATE OR REPLACE TABLE `{project}.{source_dataset}.{temp_table_name}` AS
WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC, se.event_id DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_profile_updates_event` se
  WHERE TRUE
  AND NOT EXISTS (
    SELECT 1
      FROM `{project}.{source_dataset}.{sent_table_name}` sent
      WHERE sent.participant_id = se.participant_id
        AND sent.event_id >= se.event_id
        AND sent.event_type_name = se.event_type_name
  )
  and data_element_name IN ("piiname_first","piiname_middle","piiname_last","streetaddress_piizip","streetaddress_piistate","streetaddress_piicity","piiaddress_streetaddress","piiaddress_streetaddress2","piicontactinformation_phone","piicontactinformation_email","language_preference","piibirthinformation_birthdate")
)
SELECT
  participant_id,
  event_id,
  event_type_name,
  -- First Name
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'piiname_first' AND rank = 1
           THEN data_element_value END) AS first_name,
  -- Middle Name
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'piiname_middle' AND rank = 1
           THEN data_element_value END) AS middle_name,
  -- Last Name
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'piiname_last' AND rank = 1
           THEN data_element_value END) AS last_name,
  -- Zip Code
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'streetaddress_piizip' AND rank = 1
           THEN data_element_value END) AS zip_code,
  -- State
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'streetaddress_piistate' AND rank = 1
           THEN data_element_value END) AS state,
  -- City
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'streetaddress_piicity' AND rank = 1
           THEN data_element_value END) AS city,
  -- Street Address
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'piiaddress_streetaddress' AND rank = 1
           THEN data_element_value END) AS street_address,
  -- Street Address Line 2
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'piiaddress_streetaddress2' AND rank = 1
           THEN data_element_value END) AS street_address_line_2,
  -- Phone Number
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'piicontactinformation_phone' AND rank = 1
           THEN data_element_value END) AS phone_number,
  -- Email
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'piicontactinformation_email' AND rank = 1
           THEN data_element_value END) AS email,
  -- Language Preference
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'language_preference' AND rank = 1
           THEN data_element_value END) AS language_preference,
  -- Birthdate
  MAX(CASE WHEN event_type_name = 'Profile Data' AND data_element_name = 'piibirthinformation_birthdate' AND rank = 1
           THEN data_element_value END) AS birthdate
FROM ranked_events
WHERE rank = 1
GROUP BY participant_id, event_id, event_type_name
    """


def get_withdrawal_activity_to_stream(project: str,
                                              source_dataset: str,
                                              temp_table_name: str,
                                              sent_table_name: str) -> str:
    return f"""
CREATE OR REPLACE TABLE `{project}.{source_dataset}.{temp_table_name}` AS
WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC, se.event_id DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_withdrawal_event` se
  WHERE TRUE
  AND NOT EXISTS (
    SELECT 1
      FROM `{project}.{source_dataset}.{sent_table_name}` sent
      WHERE sent.participant_id = se.participant_id
        AND sent.event_id >= se.event_id
        AND sent.event_type_name = se.event_type_name
  )
  and data_element_name IN ("activity_status", '​activity_status', 'withdrawal_reason')
)
SELECT
  participant_id,
  event_id,
  event_type_name,
  MAX(CASE WHEN event_type_name = 'Withdrawal'
      AND data_element_name IN ("activity_status", '​activity_status')
      AND rank = 1
    THEN data_element_value END) AS withdrawal_status,
  MAX(CASE WHEN event_type_name = 'Withdrawal'
      AND data_element_name IN ("activity_status", '​activity_status')
      AND rank = 1
    THEN event_authored_time END) AS withdrawal_status_authored,
  MAX(CASE WHEN event_type_name = 'Withdrawal'
      AND data_element_name IN ("withdrawal_reason")
      AND rank = 1
    THEN data_element_value END) AS withdrawal_reason
FROM ranked_events
WHERE rank = 1
GROUP BY participant_id, event_id, event_type_name
    """


def get_deactivation_activity_to_stream(project: str,
                                              source_dataset: str,
                                              temp_table_name: str,
                                              sent_table_name: str) -> str:
    return f"""
CREATE OR REPLACE TABLE `{project}.{source_dataset}.{temp_table_name}` AS
WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC, se.event_id DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_deactivation_event` se
  WHERE TRUE
  AND NOT EXISTS (
    SELECT 1
      FROM `{project}.{source_dataset}.{sent_table_name}` sent
      WHERE sent.participant_id = se.participant_id
        AND sent.event_id >= se.event_id
        AND sent.event_type_name = se.event_type_name
  )
  and data_element_name IN ("activity_status", '​activity_status')
)
SELECT
  participant_id,
  MAX(CASE WHEN event_type_name = 'Deactivation'
      AND data_element_name IN ("activity_status", '​activity_status')
      AND rank = 1
    THEN data_element_value END) AS deactivation_status,
  MAX(CASE WHEN event_type_name = 'Deactivation'
      AND data_element_name IN ("activity_status", '​activity_status')
      AND rank = 1
    THEN event_authored_time END) AS deactivation_status_time
FROM ranked_events
WHERE rank = 1
GROUP BY participant_id, event_id, event_type_name
    """


def get_participant_status_activity_to_stream(project: str,
                                              source_dataset: str,
                                              temp_table_name: str,
                                              sent_table_name: str) -> str:
    return f"""
CREATE OR REPLACE TABLE `{project}.{source_dataset}.{temp_table_name}` AS
WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC, se.event_id DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_participant_status_event` se
  WHERE TRUE
  AND NOT EXISTS (
    SELECT 1
      FROM `{project}.{source_dataset}.{sent_table_name}` sent
      WHERE sent.participant_id = se.participant_id
        AND sent.event_id >= se.event_id
        AND sent.event_type_name = se.event_type_name
  )
  and se.event_type_name IN ('Test Account', 'Death', 'Retention Status', 'Enrollment Status')
  and data_element_name IN ("activity_status", '​activity_status', "retention_type", "participant", "participant", "participant_ehr_consent", "enrolled", "pmb_eligible", "core_minus_pm", "core_participant")
)
SELECT
  participant_id,
  event_id,
  event_type_name,

  -- Test Account
  MAX(CASE WHEN event_type_name = 'Test Account' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS test_account,
  MAX(CASE WHEN event_type_name = 'Test Account' AND data_element_name IN ("activity_status", '​activity_status') THEN event_authored_time END) AS test_account_authored,

  -- Death
  MAX(CASE WHEN event_type_name = 'Death' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS deceased_status,
  MAX(CASE WHEN event_type_name = 'Death' AND data_element_name IN ("activity_status", '​activity_status') THEN event_authored_time END) AS deceased_authored,

  -- Retention Status
  MAX(CASE WHEN event_type_name = 'Retention Status' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS retention_eligible_status,
  MAX(CASE WHEN event_type_name = 'Retention Status' AND data_element_name IN ("activity_status", '​activity_status') THEN event_authored_time END) AS retention_eligible_status_authored,
  MAX(CASE WHEN event_type_name = 'Retention Status' AND data_element_name = "retention_type" THEN data_element_value END) AS retention_type,

  -- Enrollment Status
  MAX(CASE WHEN event_type_name = 'Enrollment Status' AND data_element_name = 'participant' AND data_element_value = "yes" THEN event_authored_time END) AS participant_time,
  MAX(CASE WHEN event_type_name = 'Enrollment Status' AND data_element_name = 'participant_ehr_consent' AND data_element_value = "yes" THEN event_authored_time END) AS participant_ehr_consent_time,
  MAX(CASE WHEN event_type_name = 'Enrollment Status' AND data_element_name = 'enrolled' AND data_element_value = "yes" THEN event_authored_time END) AS enrolledenrolled_time,
  MAX(CASE WHEN event_type_name = 'Enrollment Status' AND data_element_name = 'pmb_eligible' AND data_element_value = "yes" THEN event_authored_time END) AS pmb_eligible_time,
  MAX(CASE WHEN event_type_name = 'Enrollment Status' AND data_element_name = 'core_minus_pm' AND data_element_value = "yes" THEN event_authored_time END) AS core_minus_pm_time,
  MAX(CASE WHEN event_type_name = 'Enrollment Status' AND data_element_name = 'core_participant' AND data_element_value = "yes" THEN event_authored_time END) AS core_participant_time

FROM ranked_events
WHERE rank = 1
GROUP BY participant_id, event_id, event_type_name
    """


def get_survey_completion_activity_to_stream(project: str,
                                              source_dataset: str,
                                              temp_table_name: str,
                                              sent_table_name: str) -> str:
    return f"""
CREATE OR REPLACE TABLE `{project}.{source_dataset}.{temp_table_name}` AS
WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC, se.event_id DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_survey_completion_event` se
  WHERE TRUE
  AND NOT EXISTS (
    SELECT 1
      FROM `{project}.{source_dataset}.{sent_table_name}` sent
      WHERE sent.participant_id = se.participant_id
        AND sent.event_id >= se.event_id
        AND sent.event_type_name = se.event_type_name
  )
  and se.event_type_name IN ("Basics Data", "Basics Data", "Overall Health", "Lifestyle", "The Basics", "Health Care Access", "Social Determinants of Health", "Personal and Family Health History", "Life Functioning Survey", "Emotional Health History and Well Being", "Behavioral Health and Personality", "Pediatric Environmental Health")
  and data_element_name IN ("activity_status", '​activity_status', "gender_genderidentity","biologicalsexatbirth_sexatbirth","thebasics_sexualorientation","race_whatraceethnicity","educationlevel_highestgrade","income_annualincome")
)
SELECT
  participant_id,

  -- Basics Data
  MAX(CASE WHEN event_type_name = 'Basics Data' AND data_element_name = 'gender_genderidentity' THEN data_element_value END) AS gender_identity,
  MAX(CASE WHEN event_type_name = 'Basics Data' AND data_element_name = 'biologicalsexatbirth_sexatbirth' THEN data_element_value END) AS sex,
  MAX(CASE WHEN event_type_name = 'Basics Data' AND data_element_name = 'thebasics_sexualorientation' THEN data_element_value END) AS sexual_orientation,
  MAX(CASE WHEN event_type_name = 'Basics Data' AND data_element_name = 'race_whatraceethnicity' THEN data_element_value END) AS race,
  MAX(CASE WHEN event_type_name = 'Basics Data' AND data_element_name = 'educationlevel_highestgrade' THEN data_element_value END) AS education,
  MAX(CASE WHEN event_type_name = 'Basics Data' AND data_element_name = 'income_annualincome' THEN data_element_value END) AS income,
  MAX(CASE WHEN event_type_name = 'Basics Data'
    AND data_element_name = 'race_whatraceethnicity'
    AND data_element_value = "WhatRaceEthnicity_AIAN"
    THEN "yes" END) AS aian,

  -- Overall Health
  MAX(CASE WHEN event_type_name = 'Overall Health' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS questionnaire_on_overall_health,
  MAX(CASE WHEN event_type_name = 'Overall Health' AND data_element_name IN ("activity_status", '​activity_status') THEN event_authored_time END) AS questionnaire_on_overall_health_authored,

  -- Lifestyle
  MAX(CASE WHEN event_type_name = 'Lifestyle' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS questionnaire_on_lifestyle,
  MAX(CASE WHEN event_type_name = 'Lifestyle' AND data_element_name IN ("activity_status", '​activity_status') THEN event_authored_time END) AS questionnaire_on_lifestyle_authored,

  -- The Basics
  MAX(CASE WHEN event_type_name = 'The Basics' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS questionnaire_on_the_basics,
  MAX(CASE WHEN event_type_name = 'The Basics' AND data_element_name IN ("activity_status", '​activity_status') THEN event_authored_time END) AS questionnaire_on_the_basics_authored,

  -- Health Care Access
  MAX(CASE WHEN event_type_name = 'Health Care Access' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS questionnaire_on_healthcare_access,
  MAX(CASE WHEN event_type_name = 'Health Care Access' AND data_element_name IN ("activity_status", '​activity_status') THEN event_authored_time END) AS questionnaire_on_healthcare_access_authored,

  -- Social Determinants of Health
  MAX(CASE WHEN event_type_name = 'Social Determinants of Health' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS questionnaire_on_social_determinants_of_health,
  MAX(CASE WHEN event_type_name = 'Social Determinants of Health' AND data_element_name = 'activity_date_time' THEN event_authored_time END) AS questionnaire_on_social_determinants_of_health_authored,

  -- Personal and Family Health History
  MAX(CASE WHEN event_type_name = 'Personal and Family Health History' AND data_element_name = 'activity_status' THEN data_element_value END) AS questionnaire_on_personal_and_family_health_history,
  MAX(CASE WHEN event_type_name = 'Personal and Family Health History' AND data_element_name = 'activity_date_time' THEN event_authored_time END) AS questionnaire_on_personal_and_family_health_history_authored,

  -- Life Functioning Survey
  MAX(CASE WHEN event_type_name = 'Life Functioning Survey' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS questionnaire_on_life_functioning,
  MAX(CASE WHEN event_type_name = 'Life Functioning Survey' AND data_element_name = 'activity_date_time' THEN event_authored_time END) AS questionnaire_on_life_functioning_authored,

  -- Emotional Health History and Well Being
  MAX(CASE WHEN event_type_name = 'Emotional Health History and Well Being' AND data_element_name = 'activity_status' THEN data_element_value END) AS questionnaire_on_emotional_health_history_and_well_being,
  MAX(CASE WHEN event_type_name = 'Emotional Health History and Well Being' AND data_element_name = 'activity_date_time' THEN event_authored_time END) AS questionnaire_on_emotional_health_history_and_well_being_authored,

  -- Behavioral Health and Personality
  MAX(CASE WHEN event_type_name = 'Behavioral Health and Personality' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS questionnaire_on_behavioral_health_and_personality,
  MAX(CASE WHEN event_type_name = 'Behavioral Health and Personality' AND data_element_name = 'activity_date_time' THEN event_authored_time END) AS questionnaire_on_behavioral_health_and_personality_authored,

  -- Pediatric Environmental Health
  MAX(CASE WHEN event_type_name = 'Pediatric Environmental Health' AND data_element_name = 'activity_date_time' THEN event_authored_time END) AS questionnaire_on_environmental_exposures,
  MAX(CASE WHEN event_type_name = 'Pediatric Environmental Health' AND data_element_name IN ("activity_status", '​activity_status') THEN data_element_value END) AS questionnaire_on_environmental_exposures_authored

FROM ranked_events
WHERE rank = 1
GROUP BY participant_id, event_id, event_type_name
    """


def get_attribution_activity_to_stream(project: str,
                                              source_dataset: str,
                                              temp_table_name: str,
                                              sent_table_name: str) -> str:
    return f"""
CREATE OR REPLACE TABLE `{project}.{source_dataset}.{temp_table_name}` AS
WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC, se.event_id DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_attribution_event` se
  WHERE TRUE
  AND NOT EXISTS (
    SELECT 1
      FROM `{project}.{source_dataset}.{sent_table_name}` sent
      WHERE sent.participant_id = se.participant_id
        AND sent.event_id >= se.event_id
        AND sent.event_type_name = se.event_type_name
  )
  and se.event_type_name IN ('Org Attribution')
  and data_element_name IN ("activity_status", '​activity_status')
)
SELECT
  participant_id,
  MAX(CASE WHEN event_type_name = 'Org Attribution'
    AND data_element_name IN ("activity_status", '​activity_status') AND rank = 1
           THEN data_element_value END) AS organization
FROM ranked_events
WHERE rank = 1
GROUP BY participant_id, event_id, event_type_name
    """


def insert_intake_summary_records_sent(project: str,
                                       source_dataset: str,
                                       sent_table_name: str,
                                       temp_table_name: str,
                                       datafeed: str) -> str:
    return f"""
            INSERT INTO `{project}.{source_dataset}.{sent_table_name}` (
                participant_id,
                event_id,
                event_type_name,
                datafeed_name,
                created,
                modified,
                ignore_flag
            )
            SELECT
                participant_id,
                event_id,
                event_type_name,
                '{datafeed}' AS datafeed_name,
                CURRENT_DATETIME() AS created,
                CURRENT_DATETIME() AS modified,
                FALSE AS ignore_flag
            FROM `{project}.{source_dataset}.{temp_table_name}`
            """
