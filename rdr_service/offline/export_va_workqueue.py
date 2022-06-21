import os
import datetime

from rdr_service import clock, config
from rdr_service.api_util import list_blobs, delete_cloud_file
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.participant_enums import WithdrawalStatus, SuspensionStatus, DeceasedStatus, QuestionnaireStatus, \
    ConsentExpireStatus, RetentionStatus, RetentionType, PhysicalMeasurementsStatus, SampleStatus

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


def _describe_education(education):
    descriptions = {
        "HighestGrade_NineThroughEleven": "Grades 9 through 11(Some high school)",
        "HighestGrade_CollegeGraduate": "College 4 years or more(College graduate)",
        "HighestGrade_TwelveOrGED": "Grade 12 or GED(High school graduate)",
        "HighestGrade_FiveThroughEight": "Grades 5 through 8 (Middle School)",
        "HighestGrade_CollegeOnetoThree": "1 to 3 years (Some college, Associate's Degree or technical school)",
        "HighestGrade_OneThroughFour": "Grades 1 through 4 (Primary)",
        "HighestGrade_NeverAttended": "Never attended school or only attended kindergarten",
        "HighestGrade_AdvancedDegree": "Advanced degree (Master's, Doctorate, etc.)",
    }
    if education in descriptions:
        return descriptions[education]
    else:
        return ""

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
        json_fields = _get_json_fields(participant_filtered)
        participant_row = [participant_filtered.get("lastName", ""),
                           participant_filtered.get("firstName", ""),
                           participant_filtered.get("middleName", ""),
                           participant_filtered.get("dateOfBirth", ""),
                           participant_filtered.get("participantId", ""),
                           participant_filtered.get("biobankId", ""),
                           participant_filtered.get("enrollmentStatus", ""),
                           participant_filtered.get("enrollmentStatusCoreStoredSampleTime", ""),
                           int(WithdrawalStatus(participant_filtered.get("withdrawalStatus", "UNSET"))),
                           participant_filtered.get("withdrawalAuthored", ""),
                           participant_filtered.get("withdrawalReason", "")
                           if participant_filtered.get("withdrawalReason") != "UNSET" else "",
                           int(SuspensionStatus(participant_filtered.get("suspensionStatus")))
                           if participant_filtered.get("suspensionStatus") != "UNSET" else 0,
                           participant_filtered.get("suspensionTime", ""),
                           int(DeceasedStatus(participant_filtered.get("deceasedStatus", "UNSET"))),
                           participant_filtered.get("dateOfDeath", ""),
                           participant_filtered.get("deceasedAuthored", ""),
                           participant_filtered.get("participantOrigin", ""),
                           participant_filtered.get("consentCohort", ""),
                           participant_filtered.get("consentForStudyEnrollmentFirstYesAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("consentForStudyEnrollment", "UNSET"))),
                           participant_filtered.get("consentForStudyEnrollmentAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnDnaProgram", "UNSET"))),
                           participant_filtered.get("questionnaireOnDnaProgramAuthored", ""),
                           participant_filtered.get("consentForElectronicHealthRecordsFirstYesAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("consentForElectronicHealthRecords",
                                                                            "UNSET"))),
                           participant_filtered.get("consentForElectronicHealthRecordsAuthored", ""),
                           int(ConsentExpireStatus(participant_filtered.get("ehrConsentExpireStatus", "UNSET"))),
                           participant_filtered.get("ehrconsentExpireAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("consentForGenomicsROR", "UNSET"))),
                           participant_filtered.get("consentForGenomicsRORAuthored", ""),
                           participant_filtered.get("primaryLanguage", ""),
                           int(QuestionnaireStatus(participant_filtered.get(
                               "consentForDvElectronicHealthRecordsSharing", "UNSET"))),
                           participant_filtered.get("consentForDvElectronicHealthRecordsSharingTime", ""),
                           int(QuestionnaireStatus(participant_filtered.get("consentForCABoR", "UNSET"))),
                           participant_filtered.get("consentForCABoRTimeAuthored", ""),
                           int(RetentionStatus(participant_filtered.get("retentionEligibleStatus")))
                           if participant_filtered.get("retentionEligibleStatus") != "UNSET" else 0,
                           participant_filtered.get("retentionEligibleTime", ""),
                           int(RetentionType(participant_filtered.get("retentionType", "UNSET"))),
                           1 if participant_filtered.get("isEhrDataAvailable", False) else 0,
                           participant_filtered.get("latestEhrReceiptTime", ""),
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
                           participant_filtered.get("numCompletedBaselinePPIModules", ""),
                           participant_filtered.get("numCompletedPPIModules", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnTheBasics", "UNSET"))),
                           participant_filtered.get("questionnaireOnTheBasicsAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnOverallHealth", "UNSET"))),
                           participant_filtered.get("questionnaireOnOverallHealthAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnLifestyle", "UNSET"))),
                           participant_filtered.get("questionnaireOnLifestyleAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnMedicalHistory", "UNSET"))),
                           participant_filtered.get("questionnaireOnMedicalHistoryAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnFamilyHealth", "UNSET"))),
                           participant_filtered.get("questionnaireOnFamilyHealthAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnHealthcareAccess",
                                                                            "UNSET"))),
                           participant_filtered.get("questionnaireOnHealthcareAccessAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnCopeMay", "UNSET"))),
                           participant_filtered.get("questionnaireOnCopeMayTime", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnCopeJune", "UNSET"))),
                           participant_filtered.get("questionnaireOnCopeJuneTime", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnCopeJuly", "UNSET"))),
                           participant_filtered.get("questionnaireOnCopeJulyAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnCopeNov", "UNSET"))),
                           participant_filtered.get("questionnaireOnCopeNovAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnCopeDec", "UNSET"))),
                           participant_filtered.get("questionnaireOnCopeDecAuthored", ""),
                           participant_filtered.get("site", "").replace("hpo-site-", ""),
                           participant_filtered.get("organization", ""),
                           int(PhysicalMeasurementsStatus(participant_filtered.get("physicalMeasurementsStatus",
                                                                                   "UNSET"))),
                           participant_filtered.get("physicalMeasurementsFinalizedTime", ""),
                           participant_filtered.get("physicalMeasurementsFinalizedSite", "").replace("hpo-site-", ""),
                           int(SampleStatus(participant_filtered.get("samplesToIsolateDNA", "UNSET"))),
                           participant_filtered.get("numBaselineSamplesArrived", ""),
                           participant_filtered.get("biospecimenSourceSite", "").replace("hpo-site-", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1SST8", "UNSET"))),
                           participant_filtered.get("sampleStatus1SST8Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1PST8", "UNSET"))),
                           participant_filtered.get("sampleStatus1PST8Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1HEP4", "UNSET"))),
                           participant_filtered.get("sampleStatus1HEP4Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1ED02", "UNSET"))),
                           participant_filtered.get("sampleStatus1ED02Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1ED04", "UNSET"))),
                           participant_filtered.get("sampleStatus1ED04Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1ED10", "UNSET"))),
                           participant_filtered.get("sampleStatus1ED10Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus2ED10", "UNSET"))),
                           participant_filtered.get("sampleStatus2ED10Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1CFD9", "UNSET"))),
                           participant_filtered.get("sampleStatus1CFD9Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1PXR2", "UNSET"))),
                           participant_filtered.get("sampleStatus1PXR2Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1UR10", "UNSET"))),
                           participant_filtered.get("sampleStatus1UR10Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1UR90", "UNSET"))),
                           participant_filtered.get("sampleStatus1UR90Time", ""),
                           int(SampleStatus(participant_filtered.get("sampleStatus1SAL", "UNSET"))),
                           participant_filtered.get("sampleStatus1SALTime", ""),
                           participant_filtered.get("sample1SAL2CollectionMethod", ""),
                           participant_filtered.get("sex", "").replace("SexAtBirth_", ""),
                           participant_filtered.get("genderIdentity", "").replace("GenderIdentity_", ""),
                           participant_filtered.get("race", ""),
                           _describe_education(participant_filtered.get("education", "")),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnCopeFeb", "UNSET"))),
                           participant_filtered.get("questionnaireOnCopeFebAuthored", ""),
                           participant_filtered.get("enrollmentStatusCoreMinusPMTime", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnCopeVaccineMinute1",
                                                                            "UNSET"))),
                           participant_filtered.get("questionnaireOnCopeVaccineMinute1Authored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnCopeVaccineMinute2",
                                                                            "UNSET"))),
                           participant_filtered.get("questionnaireOnCopeVaccineMinute2Authored", ""),
                           json_fields.get("fitbit_consent", ""),
                           json_fields.get("fitbit_consent_date", ""),
                           json_fields.get("apple_healthkit_consent", ""),
                           json_fields.get("apple_healthkit_consent_date", ""),
                           json_fields.get("apple_ehr_consent", ""),
                           json_fields.get("apple_ehr_consent_date", ""),
                           int(QuestionnaireStatus(participant_filtered.get(
                               "questionnaireOnPersonalAndFamilyHealthHistory", "UNSET"))),
                           participant_filtered.get("questionnaireOnPersonalAndFamilyHealthHistoryAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnSocialDeterminantsOfHealth",
                                                                            "UNSET"))),
                           participant_filtered.get("questionnaireOnSocialDeterminantsOfHealthAuthored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnCopeVaccineMinute3",
                                                                            "UNSET"))),
                           participant_filtered.get("questionnaireOnCopeVaccineMinute3Authored", ""),
                           int(QuestionnaireStatus(participant_filtered.get("questionnaireOnCopeVaccineMinute4",
                                                                            "UNSET"))),
                           participant_filtered.get("questionnaireOnCopeVaccineMinute4Authored", ""),
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
