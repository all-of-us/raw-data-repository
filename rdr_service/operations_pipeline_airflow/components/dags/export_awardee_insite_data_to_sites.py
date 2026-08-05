import datetime
import logging

import pandas as pd
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.python import PythonOperator


AWARDEE_INSITE_QUERY = """
SELECT
        ai.participant_id AS participantId,
        ai.first_name AS firstName,
        ai.middle_name AS middleName,
        ai.last_name AS lastName,
        ai.zip_code AS zipCode,
        ai.state AS state,
        ai.city AS city,
        ai.street_address AS streetAddress,
        ai.street_address2 AS streetAddress2,
        ai.phone_number AS phoneNumber,
        ai.email AS email,
        ai.date_of_birth AS dateOfBirth,
        ai.organization AS organization,
        ai.withdrawal_status AS withdrawalStatus,
        ai.withdrawal_time AS withdrawalTime,
        ai.deactivation_status AS deactivationStatus,
        ai.deactivation_time AS deactivationTime,
        ai.deceased_status AS deceasedStatus,
        ai.deceased_authored AS deceasedAuthored,
        ai.clinic_physical_measurements_status AS clinicPhysicalMeasurementsStatus,
        ai.clinic_physical_measurements_finalized_time AS clinicPhysicalMeasurementsFinalizedTime,
        ai.clinic_physical_measurements_finalized_site AS clinicPhysicalMeasurementsFinalizedSite,
        ai.self_reported_physical_measurements_status AS selfReportedPhysicalMeasurementsStatus,
        ai.self_reported_physical_measurements_authored AS selfReportedPhysicalMeasurementsAuthored,
        ai.consent_for_electronic_health_records AS consentForElectronicHealthRecords,
        ai.consent_for_electronic_health_records_authored AS consentForElectronicHealthRecordsAuthored,
        ai.consent_for_electronic_health_records_first_yes_authored AS consentForElectronicHealthRecordsFirstYesAuthored,
        ai.first_ehr_receipt_time AS firstEhrReceiptTime,
        ai.latest_ehr_receipt_time AS latestEhrReceiptTime,
        ai.consent_for_study_enrollment AS consentForStudyEnrollment,
        ai.consent_for_study_enrollment_authored AS consentForStudyEnrollmentAuthored,
        ai.patient_status AS patientStatus,
        ai.enrollment_status AS enrollmentStatus,
        ai.biospecimen_source_site AS biospecimenSourceSite,
        ai.biospecimen_order_time AS biospecimenOrderTime,
        ai.biospecimen_status AS biospecimenStatus,
        ai.sample_1sal2_collection_method AS sample1SAL2CollectionMethod,
        ai.sample_status_1sal2 AS sampleStatus1SAL2,
        ai.sample_order_status_1sal2 AS sampleOrderStatus1SAL2,
        ai.sample_order_status_1sal2_time AS sampleOrderStatus1SAL2Time,
        ai.primary_language AS primaryLanguage,
        ai.aian,
        ai.awardee,
        ai.gender_identity AS genderIdentity,
        ai.is_ehr_data_available AS isEhrDataAvailable,
        ai.questionnaire_on_emotional_health AS questionnaireOnEmotionalHealthHistoryAndWellBeing,
        ai.questionnaire_on_emotional_health_authored AS questionnaireOnEmotionalHealthHistoryAndWellBeingAuthored,
        ai.questionnaire_on_healthcare_access AS questionnaireOnHealthcareAccess,
        ai.questionnaire_on_healthcare_access_authored AS questionnaireOnHealthcareAccessAuthored,
        ai.questionnaire_on_life_functioning AS questionnaireOnLifeFunctioning,
        ai.questionnaire_on_life_functioning_authored AS questionnaireOnLifeFunctioningAuthored,
        ai.questionnaire_on_lifestyle AS questionnaireOnLifestyle,
        ai.questionnaire_on_lifestyle_authored AS questionnaireOnLifestyleAuthored,
        ai.questionnaire_on_overall_health AS questionnaireOnOverallHealth,
        ai.questionnaire_on_overall_health_authored AS questionnaireOnOverallHealthAuthored,
        ai.questionnaire_on_personal_and_family_health_history AS questionnaireOnPersonalAndFamilyHealthHistory,
        ai.questionnaire_on_personal_and_family_health_history_authored AS questionnaireOnPersonalAndFamilyHealthHistoryAuthored,
        ai.questionnaire_on_social_determinants_of_health AS questionnaireOnSocialDeterminantsOfHealth,
        ai.questionnaire_on_social_determinants_of_health_authored AS questionnaireOnSocialDeterminantsOfHealthAuthored,
        ai.questionnaire_on_the_basics AS questionnaireOnTheBasics,
        ai.questionnaire_on_the_basics_authored AS questionnaireOnTheBasicsAuthored,
        ai.questionnaire_on_behavioral_health AS questionnaireOnBehavioralHealthAndPersonality,
        ai.questionnaire_on_behavioral_health_authored AS questionnaireOnBehavioralHealthAndPersonalityAuthored,
        ai.age_range AS ageRange,
        ai.duplicate_account_status AS duplicateAccountStatus,
        ai.last_active_retention_activity_time AS lastActiveRetentionActivityTime,
        ai.questionnaire_on_family_health_history_update AS questionnaireOnPersonalAndFamilyHealthHistoryUpdate,
        ai.questionnaire_on_family_health_history_update_authored AS questionnaireOnPersonalAndFamilyHealthHistoryUpdateAuthored,
        ai.questionnaire_on_health_and_wellness_update AS questionnaireOnHealthAndWellnessUpdate,
        ai.questionnaire_on_health_and_wellness_update_authored AS questionnaireOnHealthAndWellnessUpdateAuthored,
        ai.questionnaire_on_mental_health_and_wellbeing_update AS questionnaireOnMentalHealthAndWellBeingUpdate,
        ai.questionnaire_on_mental_health_and_wellbeing_update_authored AS questionnaireOnMentalHealthAndWellBeingUpdateAuthored,
        ai.questionnaire_on_pediatric_basics AS questionnaireOnPediatricBasics,
        ai.questionnaire_on_pediatric_basics_authored AS questionnaireOnPediatricBasicsAuthored,
        ai.questionnaire_on_pediatric_environmental_health AS questionnaireOnPediatricEnvironmentalHealth,
        ai.questionnaire_on_pediatric_environmental_health_authored AS questionnaireOnPediatricEnvironmentalHealthAuthored,
        ai.questionnaire_on_pediatric_overall_health AS questionnaireOnPediatricOverallHealth,
        ai.questionnaire_on_pediatric_overall_health_authored AS questionnaireOnPediatricOverallHealthAuthored,
        ai.questionnaire_on_social_factors_update AS questionnaireOnSocialFactorsUpdate,
        ai.questionnaire_on_social_factors_update_authored AS questionnaireOnSocialFactorsUpdateAuthored,
        ai.race,
        ai.retention_eligible_status AS retentionEligibleStatus,
        ai.retention_eligible_time AS retentionEligibleTime,
        ai.retention_type AS retentionType,
        ai.sign_up_time AS signUpTime,
        ai.withdrawal_reason AS withdrawalReason
      FROM `rdr_operational_datastream.ppsc_awardee_insite` ai
      WHERE ai.awardee = @awardee
      ORDER BY ai.participant_id;
"""

AWARDEE_BUCKET_MAPPING_QUERY = """
    SELECT awardee_id
        , ARRAY_AGG(bucket_name) AS buckets
    FROM `rdr_operational_datastream.rdr_ehr_daily_file_drop_sites_view_copy`
    WHERE ignore_flag = 0
    GROUP BY 1;
"""


def export_awardee_insite_data_to_sites() -> None:
    client = bigquery.Client()
    todays_date = datetime.datetime.today().strftime('%Y%m%d')

    awardee_and_bucket_list = [
        dict(row.items()) for row in client.query(AWARDEE_BUCKET_MAPPING_QUERY)
    ]

    for ele in awardee_and_bucket_list:
        awardee_id = ele["awardee_id"]

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("awardee", "STRING", awardee_id)]
        )
        df: pd.DataFrame = client.query(AWARDEE_INSITE_QUERY, job_config=job_config).to_dataframe()

        if df.empty:
            logging.info("No rows for awardee '%s', skipping.", awardee_id)
            continue

        filename = f"participant/awardee_filedrop_{awardee_id.lower()}_{todays_date}_new_3.csv"
        for bucket_name in ele["buckets"]:
            gcs_uri = f"gs://{bucket_name}/{filename}"
            df.to_csv(gcs_uri, index=False)
            logging.info(f"Wrote {len(df)} rows for {awardee_id} to {gcs_uri}")


with DAG(
    dag_id="ppsc_export_awardee_insite_data_to_sites",
    start_date=datetime.datetime(2026, 7, 15),
    is_paused_upon_creation=True,
    max_active_runs=1,
    schedule="0 8 * * *",
    catchup=False,
    tags=["awardee_insite"],
) as dag:
    export_task = PythonOperator(
        task_id="export_awardee_insite_data_to_sites",
        python_callable=export_awardee_insite_data_to_sites,
    )

