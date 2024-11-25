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
