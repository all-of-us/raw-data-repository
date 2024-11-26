def get_consent_activity_to_stream(project: str, source_dataset: str) -> str:
    return f"""
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
  FROM `{project}.{source_dataset}.ppsc_consent_event` ce
  WHERE ce.event_authored_time > (
    SELECT MAX(last_modified)
    FROM `{project}.{source_dataset}.rdr_participant_summary`
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


def get_profile_updates_activity_to_stream(project: str, source_dataset: str) -> str:
    return f"""
WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_profile_updates_event` se
  WHERE se.event_authored_time > (
    SELECT MAX(last_modified)
    FROM `{project}.{source_dataset}.rdr_participant_summary`
  )
  and data_element_name IN ("piiname_first","piiname_middle","piiname_last","streetaddress_piizip","streetaddress_piistate","streetaddress_piicity","piiaddress_streetaddress","piiaddress_streetaddress2","piicontactinformation_phone","piicontactinformation_email","language_preference","piibirthinformation_birthdate")
)
SELECT
  participant_id,
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
GROUP BY participant_id
    """


def get_withdrawal_activity_to_stream(project: str, source_dataset: str) -> str:
    return f"""
WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_withdrawal_event` se
  WHERE se.event_authored_time > (
    SELECT MAX(last_modified)
    FROM `{project}.{source_dataset}.rdr_participant_summary`
  )
  and data_element_name IN ("activity_status", '​activity_status', 'withdrawal_reason')
)
SELECT
  participant_id,
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
GROUP BY participant_id
    """

def get_deactivation_activity_to_stream(project: str, source_dataset: str) -> str:
    return f"""
    WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_deactivation_event` se
  WHERE se.event_authored_time > (
    SELECT MAX(last_modified)
    FROM `{project}.{source_dataset}.rdr_participant_summary`
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
GROUP BY participant_id
    """


def get_participant_status_activity_to_stream(project: str, source_dataset: str) -> str:
    return f"""
WITH ranked_events AS (
  SELECT
    se.participant_id,
    se.event_type_name,
    se.event_authored_time,
    se.data_element_name,
    se.data_element_value,
    ROW_NUMBER() OVER (
      PARTITION BY se.participant_id, se.event_type_name, se.data_element_name
      ORDER BY se.event_authored_time DESC
    ) AS rank
  FROM `{project}.{source_dataset}.ppsc_participant_status_event` se
  WHERE TRUE
  AND se.event_authored_time > (
    SELECT MAX(last_modified)
    FROM `{project}.{source_dataset}.rdr_participant_summary`
  )
  and se.event_type_name IN ('Test Account', 'Death', 'Retention Status', 'Enrollment Status')
  and data_element_name IN ("activity_status", '​activity_status', "retention_type", "participant", "participant", "participant_ehr_consent", "enrolled", "pmb_eligible", "core_minus_pm", "core_participant")
)
SELECT
  participant_id,

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
GROUP BY participant_id
    """
