import os
import datetime

from rdr_service import clock, config
from rdr_service.api_util import list_blobs, delete_cloud_file
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.participant_enums import DeceasedStatus, ParticipantCohortPilotFlag

CSV_HEADER = ['Last Name',
              'First Name',
              'Middle Initial',
              'Date of Birth',
              'PMI ID',
              'Biobank ID',
              'Participant Status',
              'Core Participant Date',
              'Withdrawal Status',
              'Withdrawal Date',
              'Withdrawal Reason',
              'Deactivation Status',
              'Deactivation Date',
              'Deceased',
              'Date of Death',
              'Date of Death Approval',
              'Participant Origination',
              'Consent Cohort',
              'Date of First Primary Consent',
              'Primary Consent Status',
              'Primary Consent Date',
              'Program Update',
              'Date of Program Update',
              'Date of First EHR Consent',
              'EHR Consent Status',
              'EHR Consent Date',
              'EHR Expiration Status',
              'EHR Expiration Date',
              'gRoR Consent Status',
              'gRoR Consent Date',
              'Language of Primary Consent',
              'DV-only EHR Sharing',
              'DV-only EHR Sharing Date',
              'CABoR Consent Status',
              'CABoR Consent Date',
              'Retention Eligible',
              'Date of Retention Eligibility',
              'Retention Status',
              'EHR Data Transfer',
              'Most Recent EHR Receipt',
              'Patient Status: Yes',
              'Patient Status: No',
              'Patient Status: No Access',
              'Patient Status: Unknown',
              'Street Address',
              'Street Address2',
              'City',
              'State',
              'Zip',
              'Email',
              'Login Phone',
              'Phone',
              'Required PPI Surveys Complete',
              'Completed Surveys',
              'Basics PPI Survey Complete',
              'Basics PPI Survey Completion Date',
              'Health PPI Survey Complete',
              'Health PPI Survey Completion Date',
              'Lifestyle PPI Survey Complete',
              'Lifestyle PPI Survey Completion Date',
              'Med History PPI Survey Complete',
              'Med History PPI Survey Completion Date',
              'Family History PPI Survey Complete',
              'Family History PPI Survey Completion Date',
              'Access PPI Survey Complete',
              'Access PPI Survey Completion Date',
              'COPE May PPI Survey Complete',
              'COPE May PPI Survey Completion Date',
              'COPE June PPI Survey Complete',
              'COPE June PPI Survey Completion Date',
              'COPE July PPI Survey Complete',
              'COPE July PPI Survey Completion Date',
              'COPE Nov PPI Survey Complete',
              'COPE Nov PPI Survey Completion Date',
              'COPE Dec PPI Survey Complete',
              'COPE Dec PPI Survey Completion Date',
              'Paired Site',
              'Paired Organization',
              'Physical Measurements Status',
              'Physical Measurements Completion Date',
              'Physical Measurements Site',
              'Samples to Isolate DNA',
              'Baseline Samples',
              'Biospecimens Site',
              '8 mL SST Received',
              '8 mL SST Received Date',
              '8 mL PST Received',
              '8 mL PST Received Date',
              '4 mL Na-Hep Received',
              '4 mL Na-Hep Received Date',
              '2 mL EDTA Received',
              '2 mL EDTA Received Date',
              '4 mL EDTA Received',
              '4 mL EDTA Received Date',
              '1st 10 mL EDTA Received',
              '1st 10 mL EDTA Received Date',
              '2nd 10 mL EDTA Received',
              '2nd 10 mL EDTA Received Date',
              'Cell-Free DNA Received',
              'Cell-Free DNA Received Date',
              'Paxgene RNA Received',
              'Paxgene RNA Received Date',
              'Urine 10 mL Received',
              'Urine 10 mL Received Date',
              'Urine 90 mL Received',
              'Urine 90 mL Received Date',
              'Saliva Received',
              'Saliva Received Date',
              'Saliva Collection',
              'Sex',
              'Gender Identity',
              'Race/Ethnicity',
              'Education',
              'COPE Feb PPI Survey Complete',
              'COPE Feb PPI Survey Completion Date',
              'Core Participant Minus PM Date',
              'Summer Minute PPI Survey Complete',
              'Summer Minute PPI Survey Completion Date',
              'Fall Minute PPI Survey Complete',
              'Fall Minute PPI Survey Completion Date',
              'Fitbit Consent',
              'Fitbit Consent Date',
              'Apple HealthKit Consent',
              'Apple HealthKit Consent Date',
              'Apple EHR Consent',
              'Apple EHR Consent Date',
              'Personal & Family Hx PPI Survey Complete',
              'Personal & Family Hx PPI Survey Completion Date',
              'SDOH PPI Survey Complete',
              'SDOH PPI Survey Completion Date',
              'Winter Minute PPI Survey Complete',
              'Winter Minute PPI Survey Completion Date',
              'New Year Minute PPI Survey Complete',
              'New Year Minute PPI Survey Completion Date',
              'Enrollment Site',
              ]

_INPUT_CSV_TIME_FORMAT_LENGTH = 18
_CSV_SUFFIX_LENGTH = 4
INPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
_MAX_FILE_AGE = datetime.timedelta(days=7)
FILE_PREFIX = 'va_daily_participant_wq_'


def _get_json_fields(participant):
    expanded_fields = {}
    patient_statuses = participant.get('patientStatus', [])
    for status in patient_statuses:
        pt_status = status["status"]
        if pt_status == "YES":
            expanded_fields["patient_status_yes"] = status["organization"]
        elif pt_status == "NO":
            expanded_fields["patient_status_no"] = status["organization"]
        elif pt_status == "UNKNOWN":
            expanded_fields["patient_status_unknown"] = status["organization"]
        elif pt_status == "NO_ACCESS":
            expanded_fields["patient_status_no_access"] = status["organization"]
    digital_health = participant.get('digitalHealthSharingStatus', [])
    if digital_health:
        for device, status in digital_health.items():
            if device == 'fitbit':
                expanded_fields["fitbit_consent"] = 1 if status["status"] == "YES" else 0
                expanded_fields["fitbit_consent_date"] = status["authoredTime"]
            elif device == 'appleHealthKit':
                expanded_fields["apple_healthkit_consent"] = 1 if status["status"] == "YES" else 0
                expanded_fields["apple_healthkit_consent_date"] = status["authoredTime"]
            elif device == 'appleEHR':
                expanded_fields["apple_ehr_consent"] = 1 if status["status"] == "YES" else 0
                expanded_fields["apple_ehr_consent_date"] = status["authoredTime"]
    return expanded_fields


def _export_datetime(api_datetime):
    if api_datetime:
        return datetime.datetime.fromisoformat(api_datetime.replace("Z", "")).strftime("%m/%d/%Y %H:%M:%S")
    else:
        return ""


def _export_date(api_date):
    if api_date:
        return datetime.datetime.fromisoformat(api_date).strftime("%m/%d/%Y")
    else:
        return ""


EDUCATION = {
    "HighestGrade_NineThroughEleven": "Grades 9 through 11(Some high school)",
    "HighestGrade_CollegeGraduate": "College 4 years or more(College graduate)",
    "HighestGrade_TwelveOrGED": "Grade 12 or GED(High school graduate)",
    "HighestGrade_FiveThroughEight": "Grades 5 through 8 (Middle School)",
    "HighestGrade_CollegeOnetoThree": "1 to 3 years (Some college, Associate's Degree or technical school)",
    "HighestGrade_OneThroughFour": "Grades 1 through 4 (Primary)",
    "HighestGrade_NeverAttended": "Never attended school or only attended kindergarten",
    "HighestGrade_AdvancedDegree": "Advanced degree (Master's, Doctorate, etc.)",
}
PARTICIPANT_STATUS = {
    'INTERESTED': 'Participant',
    'MEMBER': 'Participant + EHR Consent',
    'FULL_PARTICIPANT': 'Core Participant',
    'CORE_MINUS_PM': 'Core Participant Minus PM',
}
CONSENT = {
    'UNSET': 0,
    'SUBMITTED_NO_CONSENT': 0,
    'SUBMITTED_INVALID': 0,
    'SUBMITTED': 1,
    'SUBMITTED_NOT_SURE': 2
}
WITHDRAWAL = {
    'NOT_WITHDRAWN': 0,
    'NO_USE': 1
}

SUSPENSION_STATUS = {
    'NOT_SUSPENDED': 0,
    'NO_CONTACT': 1
}
SEX = {
    'SexAtBirth_Male': 'Male',
    'SexAtBirth_Female': 'Female',
    'SexAtBirth_Intersex': 'Intersex',
    'SexAtBirth_None': 'Other',
    'SexAtBirth_SexAtBirthNoneOfThese': 'Other',
    'PMI_PreferNotToAnswer': 'Other'
}
GENDER_IDENTITY = {
    'GenderIdentity_Man': 'Man',
    'GenderIdentity_Woman': 'Woman',
    'GenderIdentity_NonBinary': 'Non-Binary',
    'GenderIdentity_Transgender': 'Transgender',
    'GenderIdentity_MoreThanOne': 'More Than One Gender Identity',
    'GenderIdentity_AdditionalOptions': 'Other',
    'PMI_PreferNotToAnswer': 'Prefer Not to Answer'
}
RACE_AND_ETHNICITY = {
    'AMERICAN_INDIAN_OR_ALASKA_NATIVE': 'American Indian/Alaska Native',
    'BLACK_OR_AFRICAN_AMERICAN': 'Black or African American',
    'ASIAN': 'Asian',
    'NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER': 'Native Hawaiian or Other Pacific Islander',
    'WHITE': 'White',
    'HISPANIC_LATINO_OR_SPANISH': 'Hispanic, Latino, or Spanish',
    'MIDDLE_EASTERN_OR_NORTH_AFRICAN': 'Middle Eastern or North African',
    'HLS_AND_WHITE': 'H/L/S and White',
    'HLS_AND_BLACK': 'H/L/S and Black',
    'HLS_AND_ONE_OTHER_RACE': 'H/L/S and one other race',
    'HLS_AND_MORE_THAN_ONE_OTHER_RACE': 'H/L/S and more than one race',
    'UNSET': 'Other',
    'UNMAPPED': 'Other',
    '': 'Other',
    'MORE_THAN_ONE_RACE': 'Other',
    'OTHER_RACE': 'Other',
    'PREFER_NOT_TO_SAY': 'Other'
}

QUESTIONNAIRE_STATUS = {
    'UNSET': 0,
    'SUBMITTED_NO_CONSENT': 0,
    'SUBMITTED_NOT_SURE': 0,
    'SUBMITTED_INVALID': 0,
    'SUBMITTED': 1
}
SAMPLE_STATUS = {
    'UNSET': 0,
    'DISPOSED': 0,
    'CONSUMED': 0,
    'UNKNOWN': 0,
    'SAMPLE_NOT_RECEIVED': 0,
    'SAMPLE_NOT_PROCESSED': 0,
    'ACCESSIONING_ERROR': 0,
    'LAB_ACCIDENT': 0,
    'QNS_FOR_PROCESSING': 0,
    'QUALITY_ISSUE': 0,
    'RECEIVED': 1
}
PARTICIPANT_ORIGINATION = {
    'vibrent': 'PTSC Portal',
    'careevolution': 'DV Pilot Portal'
}
COHORT = {
    'COHORT_1': 'Cohort 1',
    'COHORT_2': 'Cohort 2',
    'COHORT_3': 'Cohort 3'
}
RETENTION_ELIGIBLE_STATUS = {
    'NOT_ELIGIBLE': 0,
    'ELIGIBLE': 1
}
RETENTION_STATUS = {
    'UNSET': 0,
    'PASSIVE': 1,
    'ACTIVE': 2,
    'ACTIVE_AND_PASSIVE': 3
}
EHR_DATA_AVAILABLE = {
    'FALSE': 0,
    'TRUE': 1
}
SALIVA_COLLECTION = {
    'UNSET': '',
    'MAIL_KIT': 'Mail Kit',
    'ON_SITE': 'On Site'
}
LANGUAGE = {
    'en': 'English',
    'es': 'Spanish'
}
PHYSICAL_MEASUREMENTS = {
    'UNSET': 0,
    'CANCELLED': 0,
    'COMPLETED': 1
}


def generate_workqueue_report():
    """ Creates csv file from ParticipantSummary table for participants paired to VA """
    hpo_dao = HPODao()
    summary_dao = ParticipantSummaryDao()
    bucket = config.getSetting(config.VA_WORKQUEUE_BUCKET_NAME)
    subfolder = config.getSetting(config.VA_WORKQUEUE_SUBFOLDER)
    file_timestamp = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")
    file_name = f'{FILE_PREFIX}{file_timestamp}.csv'
    participants = summary_dao.get_by_hpo(hpo_dao.get_by_name('VA'))
    participants_new = []
    for participant in participants:
        participant_filtered = summary_dao.to_client_json(participant)
        cohort_2_pilot_flag = int(ParticipantCohortPilotFlag(participant_filtered.get("cohort2PilotFlag", "UNSET")))
        ops_data_consent_cohort = participant_filtered.get("consentCohort")
        consent_cohort = "Cohort 2.1" if ops_data_consent_cohort == "COHORT_2" and cohort_2_pilot_flag == 1 else \
            COHORT.get(ops_data_consent_cohort)
        ehr_consent_expiration_status = 0 if participant_filtered.get("consentForElectronicHealthRecords") == \
            "SUBMITTED" and participant_filtered.get("ehrConsentExpireStatus") == "UNSET" else 1 \
            if participant_filtered.get("ehrConsentExpireStatus", "") == 'EXPIRED' else ''
        json_fields = _get_json_fields(participant_filtered)
        participant_row = [participant_filtered.get("lastName", ""),
                           participant_filtered.get("firstName", ""),
                           participant_filtered.get("middleName", ""),
                           _export_date(participant_filtered.get("dateOfBirth")),
                           participant_filtered.get("participantId", ""),
                           participant_filtered.get("biobankId", ""),
                           PARTICIPANT_STATUS.get(participant_filtered.get("enrollmentStatus"), ""),
                           _export_datetime(participant_filtered.get("enrollmentStatusCoreStoredSampleTime")),
                           WITHDRAWAL.get(participant_filtered.get("withdrawalStatus"), ""),
                           _export_datetime(participant_filtered.get("withdrawalAuthored")),
                           participant_filtered.get("withdrawalReason", "")
                           if participant_filtered.get("withdrawalReason") != "UNSET" else "",
                           SUSPENSION_STATUS.get(participant_filtered.get("suspensionStatus"), ""),
                           _export_datetime(participant_filtered.get("suspensionTime", "")),
                           int(DeceasedStatus(participant_filtered.get("deceasedStatus", "UNSET"))),
                           _export_date(participant_filtered.get("dateOfDeath", "")),
                           _export_datetime(participant_filtered.get("deceasedAuthored")),
                           PARTICIPANT_ORIGINATION.get(participant_filtered.get("participantOrigin")),
                           consent_cohort,
                           _export_datetime(participant_filtered.get("consentForStudyEnrollmentFirstYesAuthored")),
                           CONSENT.get(participant_filtered.get("consentForStudyEnrollment", "UNSET")),
                           _export_datetime(participant_filtered.get("consentForStudyEnrollmentAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnDnaProgram", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnDnaProgramAuthored")),
                           _export_datetime(participant_filtered.get(
                               "consentForElectronicHealthRecordsFirstYesAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("consentForElectronicHealthRecords",
                                                                             "UNSET")),
                           _export_datetime(participant_filtered.get("consentForElectronicHealthRecordsAuthored")),
                           ehr_consent_expiration_status,
                           _export_datetime(participant_filtered.get("ehrconsentExpireAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("consentForGenomicsROR", "UNSET")),
                           _export_datetime(participant_filtered.get("consentForGenomicsRORAuthored")),
                           LANGUAGE.get(participant_filtered.get("primaryLanguage")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get(
                               "consentForDvElectronicHealthRecordsSharing", "UNSET")),
                           _export_datetime(participant_filtered.get("consentForDvElectronicHealthRecordsSharingTime")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("consentForCABoR", "UNSET")),
                           _export_datetime(participant_filtered.get("consentForCABoRTimeAuthored")),
                           RETENTION_ELIGIBLE_STATUS.get(participant_filtered.get("retentionEligibleStatus")),
                           _export_datetime(participant_filtered.get("retentionEligibleTime")),
                           RETENTION_STATUS.get(participant_filtered.get("retentionType", "UNSET")),
                           1 if participant_filtered.get("isEhrDataAvailable", False) else 0,
                           _export_datetime(participant_filtered.get("latestEhrReceiptTime")),
                           json_fields.get("patient_status_yes", ""),
                           json_fields.get("patient_status_no", ""),
                           json_fields.get("patient_status_no_access", ""),
                           json_fields.get("patient_status_unknown", ""),
                           participant_filtered.get("streetAddress", ""),
                           participant_filtered.get("streetAddress2", ""),
                           participant_filtered.get("city", ""),
                           participant_filtered.get("state", "").replace("PIIState_", ""),
                           participant_filtered.get("zipCode", ""),
                           participant_filtered.get("email", ""),
                           participant_filtered.get("loginPhoneNumber", ""),
                           participant_filtered.get("phoneNumber", ""),
                           1 if participant_filtered.get("numCompletedBaselinePPIModules", 0) >= 3 else 0,
                           participant_filtered.get("numCompletedPPIModules", ""),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnTheBasics", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnTheBasicsAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnOverallHealth", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnOverallHealthAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnLifestyle", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnLifestyleAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnMedicalHistory", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnMedicalHistoryAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnFamilyHealth", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnFamilyHealthAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnHealthcareAccess",
                                                                             "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnHealthcareAccessAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnCopeMay", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnCopeMayTime")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnCopeJune", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnCopeJuneTime")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnCopeJuly", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnCopeJulyAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnCopeNov", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnCopeNovAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnCopeDec", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnCopeDecAuthored")),
                           participant_filtered.get("site", "").replace("hpo-site-", ""),
                           participant_filtered.get("organization", ""),
                           PHYSICAL_MEASUREMENTS.get(participant_filtered.get("clinicPhysicalMeasurementsStatus")),
                           _export_date(participant_filtered.get("clinicPhysicalMeasurementsFinalizedTime")),
                           participant_filtered.get("clinicPhysicalMeasurementsFinalizedSite", "").replace(
                               "hpo-site-", ""),
                           SAMPLE_STATUS.get(participant_filtered.get("samplesToIsolateDNA", "UNSET")),
                           participant_filtered.get("numBaselineSamplesArrived", ""),
                           participant_filtered.get("biospecimenSourceSite", "").replace("hpo-site-", ""),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1SST8", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1SST8Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1PST8", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1PST8Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1HEP4", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1HEP4Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1ED02", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1ED02Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1ED04", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1ED04Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1ED10", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1ED10Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus2ED10", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus2ED10Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1CFD9", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1CFD9Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1PXR2", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1PXR2Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1UR10", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1UR10Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1UR90", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1UR90Time")),
                           SAMPLE_STATUS.get(participant_filtered.get("sampleStatus1SAL", "UNSET")),
                           _export_date(participant_filtered.get("sampleStatus1SALTime")),
                           SALIVA_COLLECTION.get(participant_filtered.get("sample1SAL2CollectionMethod")),
                           SEX.get(participant_filtered.get("sex")),
                           GENDER_IDENTITY.get(participant_filtered.get("genderIdentity")),
                           RACE_AND_ETHNICITY.get(participant_filtered.get("race")),
                           EDUCATION.get(participant_filtered.get("education")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnCopeFeb", "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnCopeFebAuthored")),
                           _export_datetime(participant_filtered.get("enrollmentStatusCoreMinusPMTime")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnCopeVaccineMinute1",
                                                                             "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnCopeVaccineMinute1Authored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnCopeVaccineMinute2",
                                                                             "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnCopeVaccineMinute2Authored")),
                           json_fields.get("fitbit_consent", ""),
                           _export_datetime(json_fields.get("fitbit_consent_date")),
                           json_fields.get("apple_healthkit_consent", ""),
                           _export_datetime(json_fields.get("apple_healthkit_consent_date")),
                           json_fields.get("apple_ehr_consent", ""),
                           _export_datetime(json_fields.get("apple_ehr_consent_date")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get(
                               "questionnaireOnPersonalAndFamilyHealthHistory", "UNSET")),
                           _export_datetime(
                               participant_filtered.get("questionnaireOnPersonalAndFamilyHealthHistoryAuthored")),
                           QUESTIONNAIRE_STATUS.get(
                               participant_filtered.get("questionnaireOnSocialDeterminantsOfHealth",
                                                        "UNSET")),
                           _export_datetime(
                               participant_filtered.get("questionnaireOnSocialDeterminantsOfHealthAuthored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnCopeVaccineMinute3",
                                                                             "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnCopeVaccineMinute3Authored")),
                           QUESTIONNAIRE_STATUS.get(participant_filtered.get("questionnaireOnCopeVaccineMinute4",
                                                                             "UNSET")),
                           _export_datetime(participant_filtered.get("questionnaireOnCopeVaccineMinute4Authored")),
                           participant_filtered.get("enrollmentSite", "").replace("hpo-site-", "")
                           ]
        participants_new.append(participant_row)
    exporter = SqlExporter(bucket)
    with exporter.open_cloud_writer(subfolder + "/" + file_name) as writer:
        writer.write_header(CSV_HEADER)
        writer.write_rows(participants_new)


def delete_old_reports():
    """ Deletes export files that are more than 7 days old """
    bucket = config.getSetting(config.VA_WORKQUEUE_BUCKET_NAME)
    subfolder = config.getSetting(config.VA_WORKQUEUE_SUBFOLDER)
    now = clock.CLOCK.now()
    for file in list_blobs(bucket, subfolder):
        if file.name.endswith(".csv") and os.path.basename(file.name).startswith(FILE_PREFIX):
            file_time = _timestamp_from_filename(file.name)
            if now - file_time > _MAX_FILE_AGE:
                delete_cloud_file(bucket + "/" + file.name)


def _timestamp_from_filename(csv_filename):
    if len(csv_filename) < _INPUT_CSV_TIME_FORMAT_LENGTH + _CSV_SUFFIX_LENGTH:
        raise RuntimeError("Can't parse time from CSV filename: %s" % csv_filename)
    time_suffix = csv_filename[
                  len(csv_filename)
                  - (_INPUT_CSV_TIME_FORMAT_LENGTH + _CSV_SUFFIX_LENGTH)
                  - 1: len(csv_filename)
                       - _CSV_SUFFIX_LENGTH
                  ]
    try:
        timestamp = datetime.datetime.strptime(time_suffix, INPUT_CSV_TIME_FORMAT)
    except ValueError as timestamp_parse_error:
        raise RuntimeError("Can't parse time from CSV filename: %s" % csv_filename) from timestamp_parse_error
    return timestamp
