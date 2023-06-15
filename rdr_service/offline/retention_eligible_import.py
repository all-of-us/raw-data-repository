import datetime
import logging
from typing import Optional

from dateutil.parser import parse

from rdr_service.app_util import nonprod
from rdr_service.clock import CLOCK
from rdr_service.code_constants import (
    COHORT_1_REVIEW_CONSENT_YES_CODE, COPE_MODULE, COPE_NOV_MODULE, COPE_DEC_MODULE, COPE_FEB_MODULE,
    COPE_VACCINE_MINUTE_1_MODULE_CODE, COPE_VACCINE_MINUTE_2_MODULE_CODE, COPE_VACCINE_MINUTE_3_MODULE_CODE,
    COPE_VACCINE_MINUTE_4_MODULE_CODE, PRIMARY_CONSENT_UPDATE_MODULE, PRIMARY_CONSENT_UPDATE_QUESTION_CODE
)
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.retention_eligible_metrics_dao import RetentionEligibleMetricsDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.participant_enums import DeceasedStatus, RetentionType, RetentionStatus, WithdrawalStatus
from rdr_service.repository.questionnaire_response_repository import QuestionnaireResponseRepository
from rdr_service.services.retention_calculation import Consent, RetentionEligibility, RetentionEligibilityDependencies
from rdr_service.storage import GoogleCloudStorageCSVReader


_BATCH_SIZE = 1000


def import_retention_eligible_metrics_file(task_data):
    """
    Import PTSC retention eligible metric file from bucket.
    :param task_data: Cloud function event dict.
    """
    csv_file_cloud_path = task_data["file_path"]
    upload_date = task_data["upload_date"]
    dao = RetentionEligibleMetricsDao()

    # Copy bucket file to local temp file.
    logging.info(f"Reading gs://{csv_file_cloud_path}.")
    csv_reader = GoogleCloudStorageCSVReader(csv_file_cloud_path)

    batch_count = upsert_count = 0
    records = list()
    with dao.session() as session:
        for row in csv_reader:
            if not row[RetentionEligibleMetricCsvColumns.PARTICIPANT_ID]:
                continue
            record = _create_retention_eligible_metrics_obj_from_row(row, upload_date)

            existing_id, needs_update = RetentionEligibleMetricsDao.find_metric_with_session(
                session=session,
                metrics_obj=record
            )
            if existing_id:
                record.id = existing_id
            if needs_update:
                _supplement_with_rdr_calculations(metrics_data=record, session=session)
                records.append(record)
                batch_count += 1

            if batch_count == _BATCH_SIZE:
                upsert_count += dao.upsert_all_with_session(session, records)
                records.clear()
                batch_count = 0

        if records:
            upsert_count += dao.upsert_all_with_session(session, records)

    logging.info(f"Updating participant summary retention eligible flags for {upsert_count} participants...")
    ParticipantSummaryDao().bulk_update_retention_eligible_flags(upload_date)
    logging.info(f"Import and update completed for gs://{csv_file_cloud_path}")


@nonprod
def calculate_retention_eligible_metrics():
    # Calculate retention eligible metrics
    # This method is for lower env only, Prod env will import from file use above method
    retention_window = datetime.timedelta(days=547)
    eighteen_month_ago = CLOCK.now() - retention_window
    eighteen_month_ago_str = eighteen_month_ago.strftime('%Y-%m-%d %H:%M:%S')
    update_sql = """
        UPDATE participant_summary
        SET retention_eligible_status =
        CASE WHEN
            consent_for_study_enrollment = 1
            AND (consent_for_electronic_health_records = 1 OR consent_for_dv_electronic_health_records_sharing = 1)
            AND questionnaire_on_the_basics = 1
            AND questionnaire_on_overall_health = 1
            AND questionnaire_on_lifestyle = 1
            AND withdrawal_status = 1
            AND suspension_status = 1
            AND samples_to_isolate_dna = 1
            THEN 2 ELSE 1
        END,
        retention_eligible_time =
        CASE WHEN
            consent_for_study_enrollment = 1
            AND (consent_for_electronic_health_records = 1 OR consent_for_dv_electronic_health_records_sharing = 1)
            AND questionnaire_on_the_basics = 1
            AND questionnaire_on_overall_health = 1
            AND questionnaire_on_lifestyle = 1
            AND withdrawal_status = 1
            AND suspension_status = 1
            AND samples_to_isolate_dna = 1
            AND
              COALESCE(sample_status_1ed10_time, sample_status_2ed10_time, sample_status_1ed04_time,
                     sample_status_1sal_time, sample_status_1sal2_time, 0) != 0
            THEN GREATEST(
                GREATEST (consent_for_study_enrollment_authored,
                 questionnaire_on_the_basics_authored,
                 questionnaire_on_overall_health_authored,
                 questionnaire_on_lifestyle_authored,
                 COALESCE(consent_for_electronic_health_records_authored, consent_for_study_enrollment_authored),
                 COALESCE(consent_for_dv_electronic_health_records_sharing_authored, consent_for_study_enrollment_authored)
                ),
                LEAST(COALESCE(sample_status_1ed10_time, '9999-01-01'),
                    COALESCE(sample_status_2ed10_time, '9999-01-01'),
                    COALESCE(sample_status_1ed04_time, '9999-01-01'),
                    COALESCE(sample_status_1sal_time, '9999-01-01'),
                    COALESCE(sample_status_1sal2_time, '9999-01-01')
                )
            )
            ELSE NULL
        END,
        retention_type =
        CASE WHEN
            consent_for_study_enrollment = 1
            AND (consent_for_electronic_health_records = 1 OR consent_for_dv_electronic_health_records_sharing = 1)
            AND questionnaire_on_the_basics = 1
            AND questionnaire_on_overall_health = 1
            AND questionnaire_on_lifestyle = 1
            AND withdrawal_status = 1
            AND suspension_status = 1
            AND samples_to_isolate_dna = 1
            AND (
                    (questionnaire_on_healthcare_access_authored is not null and
                     questionnaire_on_healthcare_access_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_family_health_authored is not null and
                     questionnaire_on_family_health_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_medical_history_authored is not null and
                     questionnaire_on_medical_history_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_nov_authored is not null
                        and questionnaire_on_cope_nov_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_july_authored is not null
                        and questionnaire_on_cope_july_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_june_authored is not null
                        and questionnaire_on_cope_june_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_dec_authored is not null
                        and questionnaire_on_cope_dec_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_may_authored is not null
                        and questionnaire_on_cope_may_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_feb_authored is not null
                        and questionnaire_on_cope_feb_authored > '{eighteen_month_ago}') or
                    (consent_cohort = 1 and consent_for_study_enrollment_authored !=
                                            participant_summary.consent_for_study_enrollment_first_yes_authored and
                     consent_for_study_enrollment_authored > '{eighteen_month_ago}') or
                    (consent_cohort = 1 and consent_for_genomics_ror_authored is not null and
                     consent_for_genomics_ror_authored > '{eighteen_month_ago}') or
                    (consent_cohort = 2 and consent_for_genomics_ror_authored is not null and
                     consent_for_genomics_ror_authored > '{eighteen_month_ago}')
                )
            AND ehr_update_time is not null and ehr_update_time>'{eighteen_month_ago}'
            THEN 3
            WHEN
            consent_for_study_enrollment = 1
            AND (consent_for_electronic_health_records = 1 OR consent_for_dv_electronic_health_records_sharing = 1)
            AND questionnaire_on_the_basics = 1
            AND questionnaire_on_overall_health = 1
            AND questionnaire_on_lifestyle = 1
            AND withdrawal_status = 1
            AND suspension_status = 1
            AND samples_to_isolate_dna = 1
            AND (
                    (questionnaire_on_healthcare_access_authored is not null and
                     questionnaire_on_healthcare_access_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_family_health_authored is not null and
                     questionnaire_on_family_health_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_medical_history_authored is not null and
                     questionnaire_on_medical_history_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_nov_authored is not null
                        and questionnaire_on_cope_nov_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_july_authored is not null
                        and questionnaire_on_cope_july_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_june_authored is not null
                        and questionnaire_on_cope_june_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_dec_authored is not null
                        and questionnaire_on_cope_dec_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_may_authored is not null
                        and questionnaire_on_cope_may_authored > '{eighteen_month_ago}') or
                    (questionnaire_on_cope_feb_authored is not null
                        and questionnaire_on_cope_feb_authored > '{eighteen_month_ago}') or
                    (consent_cohort = 1 and consent_for_study_enrollment_authored !=
                                            participant_summary.consent_for_study_enrollment_first_yes_authored and
                     consent_for_study_enrollment_authored > '{eighteen_month_ago}') or
                    (consent_cohort = 1 and consent_for_genomics_ror_authored is not null and
                     consent_for_genomics_ror_authored > '{eighteen_month_ago}') or
                    (consent_cohort = 2 and consent_for_genomics_ror_authored is not null and
                     consent_for_genomics_ror_authored > '{eighteen_month_ago}')
                )
            THEN 1
            WHEN
            consent_for_study_enrollment = 1
            AND (consent_for_electronic_health_records = 1 OR consent_for_dv_electronic_health_records_sharing = 1)
            AND questionnaire_on_the_basics = 1
            AND questionnaire_on_overall_health = 1
            AND questionnaire_on_lifestyle = 1
            AND withdrawal_status = 1
            AND suspension_status = 1
            AND samples_to_isolate_dna = 1
            THEN 2
            ELSE 0
        END
        WHERE 1=1
    """.format(eighteen_month_ago=eighteen_month_ago_str)

    dao = ParticipantSummaryDao()
    with dao.session() as session:
        session.execute(update_sql)


def _parse_field(parser_func, field_str):
    return parser_func(field_str) if field_str not in ('', 'NULL') else None


def _create_retention_eligible_metrics_obj_from_row(row, upload_date) -> RetentionEligibleMetrics:
    retention_eligible = _parse_field(int, row[RetentionEligibleMetricCsvColumns.RETENTION_ELIGIBLE])
    eligible_time = _parse_field(parse, row[RetentionEligibleMetricCsvColumns.RETENTION_ELIGIBLE_TIME])
    last_active_eligible_activity_time = _parse_field(parse, row[RetentionEligibleMetricCsvColumns
                                                      .LAST_ACTIVE_RETENTION_ACTIVITY_TIME])
    actively_retained = _parse_field(int, row[RetentionEligibleMetricCsvColumns.ACTIVELY_RETAINED])
    passively_retained = _parse_field(int, row[RetentionEligibleMetricCsvColumns.PASSIVELY_RETAINED])

    retention_type = RetentionType.UNSET
    if actively_retained and passively_retained:
        retention_type = RetentionType.ACTIVE_AND_PASSIVE
    elif actively_retained:
        retention_type = RetentionType.ACTIVE
    elif passively_retained:
        retention_type = RetentionType.PASSIVE

    return RetentionEligibleMetrics(
        participantId=row[RetentionEligibleMetricCsvColumns.PARTICIPANT_ID],
        retentionEligible=retention_eligible,
        retentionEligibleTime=eligible_time,
        lastActiveRetentionActivityTime=last_active_eligible_activity_time,
        activelyRetained=actively_retained,
        passivelyRetained=passively_retained,
        fileUploadDate=upload_date,
        retentionEligibleStatus=RetentionStatus.ELIGIBLE if retention_eligible else RetentionStatus.NOT_ELIGIBLE,
        retentionType=retention_type
    )


def _supplement_with_rdr_calculations(metrics_data: RetentionEligibleMetrics, session):
    """Fill in the rdr eligibility calculations for comparison"""
    summary_dao = ParticipantSummaryDao()

    summary: ParticipantSummary = summary_dao.get_with_session(
        session=session,
        obj_id=metrics_data.participantId
    )

    if not summary:
        logging.error(f'no summary for P{metrics_data.participantId}')
        return

    dependencies = RetentionEligibilityDependencies(
        primary_consent=Consent(
            is_consent_provided=True,
            authored_timestamp=summary.consentForStudyEnrollmentFirstYesAuthored
        ),
        first_ehr_consent=_get_earliest_intent_for_ehr(
            session=session,
            participant_id=summary.participantId
        ),
        is_deceased=summary.deceasedStatus == DeceasedStatus.APPROVED,
        is_withdrawn=summary.withdrawalStatus != WithdrawalStatus.NOT_WITHDRAWN,
        dna_samples_timestamp=BiobankStoredSampleDao.get_earliest_confirmed_dna_sample_timestamp(
            session=session,
            biobank_id=summary.biobankId
        ),
        consent_cohort=summary.consentCohort,
        has_uploaded_ehr_file=summary.wasEhrDataAvailable,
        latest_ehr_upload_timestamp=summary.ehrUpdateTime,
        basics_response_timestamp=summary.questionnaireOnTheBasicsAuthored,
        overallhealth_response_timestamp=summary.questionnaireOnOverallHealthAuthored,
        lifestyle_response_timestamp=summary.questionnaireOnLifestyleAuthored,
        healthcare_access_response_timestamp=summary.questionnaireOnHealthcareAccessAuthored,
        family_health_response_timestamp=summary.questionnaireOnFamilyHealthAuthored,
        medical_history_response_timestamp=summary.questionnaireOnMedicalHistoryAuthored,
        fam_med_history_response_timestamp=summary.questionnaireOnPersonalAndFamilyHealthHistoryAuthored,
        sdoh_response_timestamp=summary.questionnaireOnSocialDeterminantsOfHealthTime,
        latest_cope_response_timestamp=_aggregate_response_timestamps(
            session=session,
            participant_id=summary.participantId,
            survey_code_list=[
                COPE_MODULE, COPE_NOV_MODULE, COPE_DEC_MODULE, COPE_FEB_MODULE, COPE_VACCINE_MINUTE_1_MODULE_CODE,
                COPE_VACCINE_MINUTE_2_MODULE_CODE, COPE_VACCINE_MINUTE_3_MODULE_CODE,
                COPE_VACCINE_MINUTE_4_MODULE_CODE
            ],
            aggregate_function=max  # Get the latest COPE or vaccine response
        ),
        remote_pm_response_timestamp=summary.selfReportedPhysicalMeasurementsAuthored,
        life_func_response_timestamp=summary.questionnaireOnLifeFunctioningAuthored,
        reconsent_response_timestamp=_aggregate_response_timestamps(
            session=session,
            participant_id=summary.participantId,
            survey_code_list=[PRIMARY_CONSENT_UPDATE_MODULE],
            aggregate_function=min  # Get the earliest cohort 1 reconsent response
        ),
        gror_response_timestamp=summary.consentForGenomicsRORAuthored
    )
    retention_data = RetentionEligibility(dependencies)

    metrics_data.rdr_retention_eligible = retention_data.is_eligible
    metrics_data.rdr_retention_eligible_time = retention_data.retention_eligible_date
    metrics_data.rdr_last_retention_activity_time = retention_data.last_active_retention_date
    metrics_data.rdr_is_actively_retained = retention_data.is_actively_retained
    metrics_data.rdr_is_passively_retained = retention_data.is_passively_retained


def _get_earliest_intent_for_ehr(session, participant_id) -> Optional[Consent]:
    date_range_list = QuestionnaireResponseRepository.get_interest_in_sharing_ehr_ranges(
        participant_id=participant_id,
        session=session
    )
    if not date_range_list:
        return None

    return Consent(
        is_consent_provided=True,
        authored_timestamp=min(date_range.start for date_range in date_range_list)
    )


def _aggregate_response_timestamps(session, participant_id, survey_code_list, aggregate_function) -> datetime:
    """Process all the responses to the given modules, and return a single datetime"""
    revised_consent_time_list = []
    response_collection = QuestionnaireResponseRepository.get_responses_to_surveys(
        session=session,
        survey_codes=survey_code_list,
        participant_ids=[participant_id]
    )
    if participant_id in response_collection:
        program_update_response_list = response_collection[participant_id].responses.values()
        for response in program_update_response_list:
            reconsent_answer = response.get_single_answer_for(PRIMARY_CONSENT_UPDATE_QUESTION_CODE)
            if reconsent_answer and reconsent_answer.value.lower() == COHORT_1_REVIEW_CONSENT_YES_CODE.lower():
                revised_consent_time_list.append(response.authored_datetime)

    if not revised_consent_time_list:
        return None

    # Process the dates (excluding any that might be None)
    return aggregate_function(timestamp for timestamp in revised_consent_time_list if timestamp)


class RetentionEligibleMetricCsvColumns(object):
    PARTICIPANT_ID = "participant_id"
    RETENTION_ELIGIBLE = "retention_eligible"
    RETENTION_ELIGIBLE_TIME = "retention_eligible_date"
    LAST_ACTIVE_RETENTION_ACTIVITY_TIME = "last_active_retention_activity_date"
    ACTIVELY_RETAINED = "actively_retained"
    PASSIVELY_RETAINED = "passively_retained"

    ALL = (PARTICIPANT_ID, RETENTION_ELIGIBLE, RETENTION_ELIGIBLE_TIME, LAST_ACTIVE_RETENTION_ACTIVITY_TIME,
           ACTIVELY_RETAINED, PASSIVELY_RETAINED)


class DataError(RuntimeError):
    def __init__(self, msg, external=False):
        super(DataError, self).__init__(msg)
        self.external = external
