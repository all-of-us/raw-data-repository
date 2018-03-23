update participant_summary ps
join participant p
    on p.participant_id = ps.participant_id
  set ps.last_modified = (
    select GREATEST(
        COALESCE(p.last_modified, 0),
        COALESCE(physical_measurements_time, 0),
        COALESCE(p.sign_up_time, 0),
        COALESCE(consent_for_study_enrollment_time, 0),
        COALESCE(consent_for_cabor_time, 0),
        COALESCE(consent_for_electronic_health_records_time, 0),
        COALESCE(questionnaire_on_family_health_time, 0),
        COALESCE(questionnaire_on_healthcare_access_time, 0),
        COALESCE(questionnaire_on_lifestyle_time, 0),
        COALESCE(questionnaire_on_medical_history_time, 0),
        COALESCE(questionnaire_on_medications_time, 0),
        COALESCE(questionnaire_on_overall_health_time, 0),
        COALESCE(questionnaire_on_the_basics_time, 0),
        COALESCE(sample_status_1pst8_time, 0),
        COALESCE(sample_status_1sst8_time, 0),
        COALESCE(sample_status_2pst8_time, 0),
        COALESCE(sample_status_2sst8_time, 0),
        COALESCE(sample_status_1hep4_time, 0),
        COALESCE(sample_status_1ed04_time, 0),
        COALESCE(sample_status_1ed10_time, 0),
        COALESCE(sample_status_2ed10_time, 0),
        COALESCE(sample_status_1ur10_time, 0),
        COALESCE(sample_status_1sal_time, 0),
        COALESCE(sample_status_1ps08_time, 0),
        COALESCE(sample_status_1ss08_time, 0),
        COALESCE(biospecimen_order_time, 0),
        COALESCE(physical_measurements_time, 0),
        COALESCE(physical_measurements_finalized_time, 0)
  ))
where ps.last_modified is null;
COMMIT;
