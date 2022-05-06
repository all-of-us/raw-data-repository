import os
import datetime

from rdr_service import clock, config
from rdr_service.api_util import list_blobs, delete_cloud_file
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao

CSV_HEADER = ['PMI ID',
              'Biobank ID',
              'Last Name',
              'First Name',
              'Middle Initial',
              'Date of Birth',
              'Participant Status',
              'Primary Consent Status',
              'Primary Consent Date',
              'EHR Consent Status',
              'EHR Consent Date',
              'CABoR Consent Status',
              'CABoR Consent Date',
              'Withdrawal Status',
              'Withdrawal Date',
              'Deactivation Status',
              'Deactivation Date',
              'Withdrawal Reason',
              'Street Address',
              'Street Address 2',
              'City',
              'State',
              'ZIP',
              'Email',
              'Phone',
              'Age',
              'Sex',
              'Gender Identity',
              'Race and Ethnicity',
              'Education',
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
              'Physical Measurements Status',
              'Physical Measurements Completion Date',
              'Paired Site',
              'Paired Organization',
              'Physical Measurements Site',
              'Samples to Isolate DNA',
              'Baseline Samples Received',
              '8 mL SST Received',
              '8 mL SST Collection Date',
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
              'Biospecimens Site',
              'Language of Primary Consent',
              'DV-only EHR Sharing',
              'Login Phone',
              'Patient Status',
              'Core Participant Date',
              'Participant Origination',
              'Genetic Return of Results Consent Status',
              'Genetic Return of Results Consent Date',
              'COPE May PPI Survey Complete',
              'COPE May Date',
              'COPE June PPI Survey Complete',
              'COPE June Date',
              'COPE July PPI Survey Complete',
              'COPE July Date',
              'Consent Cohort',
              'Program Update',
              'Date of Program Update',
              'EHR Expiration Status',
              'Date of EHR Expiration',
              'Date of First Primary Consent',
              'Date of First EHR Consent',
              'Retention Eligible',
              'Date of Retention Eligibility',
              'Deceased',
              'Date of Death',
              'Date of Approval',
              'COPE Nov PPI Survey Complete',
              'COPE Nov PPI Survey Completion Date',
              'Retention Status',
              'EHR Data Transfer',
              'Most Recent EHR Receipt',
              'Saliva Collection',
              'COPE Dec PPI Survey Complete',
              'COPE Dec PPI Survey Completion Date',
              'COPE Feb PPI Survey Complete',
              'COPE Feb PPI Survey Completion Date',
              'Core Participant Minus PM Date',
              'Summer Minute PPI Survey Complete',
              'Summer Minute PPI Survey Completion Date',
              'Fall Minute PPI Survey Complete',
              'Fall Minute PPI Survey Completion Date',
              'Digital Health Consent',
              'Personal & Family Hx PPI Survey Complete',
              'Personal & Family Hx PPI Survey Completion Date',
              'SDOH PPI Survey Complete',
              'SDOH PPI Survey Completion Date',
              'Winter Minute PPI Survey Complete',
              'Winter Minute PPI Survey Completion Date',
              'New Year Minute PPI Survey Complete',
              'New Year Minute PPI Survey Completion Date'
              ]
_INPUT_CSV_TIME_FORMAT_LENGTH = 18
_CSV_SUFFIX_LENGTH = 4
INPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
_MAX_FILE_AGE = datetime.timedelta(days=7)
FILE_PREFIX = 'va_daily_participant_wq_'


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
        participant_row = [participant.participantId,
                           participant.biobankId,
                           participant.lastName,
                           participant.firstName,
                           participant.middleName,
                           participant.dateOfBirth,
                           participant.enrollmentStatus,
                           participant.consentForStudyEnrollment,
                           participant.consentForStudyEnrollmentAuthored,
                           participant.consentForElectronicHealthRecords,
                           participant.consentForElectronicHealthRecordsAuthored,
                           participant.consentForCABoR,
                           participant.consentForCABoRAuthored,
                           participant.withdrawalStatus,
                           participant.withdrawalAuthored,
                           participant.suspensionStatus,
                           participant.suspensionTime,
                           participant.withdrawalReason,
                           participant.streetAddress,
                           participant.streetAddress2,
                           participant.city,
                           participant.state,
                           participant.zipCode,
                           participant.email,
                           participant.phoneNumber,
                           participant.ageRange,
                           participant.sex,
                           participant.genderIdentity,
                           participant.race,
                           participant.education,
                           participant.numCompletedBaselinePPIModules,
                           participant.numCompletedPPIModules,
                           participant.questionnaireOnTheBasics,
                           participant.questionnaireOnTheBasicsAuthored,
                           participant.questionnaireOnOverallHealth,
                           participant.questionnaireOnOverallHealthAuthored,
                           participant.questionnaireOnLifestyle,
                           participant.questionnaireOnLifestyleAuthored,
                           participant.questionnaireOnMedicalHistory,
                           participant.questionnaireOnMedicalHistoryAuthored,
                           participant.questionnaireOnFamilyHealth,
                           participant.questionnaireOnFamilyHealthAuthored,
                           participant.questionnaireOnHealthcareAccess,
                           participant.questionnaireOnHealthcareAccessAuthored,
                           participant.physicalMeasurementsStatus,
                           participant.physicalMeasurementsFinalizedTime,
                           participant.site,
                           participant.organization,
                           participant.physicalMeasurementsFinalizedSite,
                           participant.samplesToIsolateDNA,
                           participant.numBaselineSamplesArrived,
                           participant.sampleStatus1SST8,
                           participant.sampleStatus1SST8Time,
                           participant.sampleStatus1PST8,
                           participant.sampleStatus1PST8Time,
                           participant.sampleStatus1HEP4,
                           participant.sampleStatus1HEP4Time,
                           participant.sampleStatus1ED02,
                           participant.sampleStatus1ED02Time,
                           participant.sampleStatus1ED04,
                           participant.sampleStatus1ED04Time,
                           participant.sampleStatus1ED10,
                           participant.sampleStatus1ED10Time,
                           participant.sampleStatus2ED10,
                           participant.sampleStatus2ED10Time,
                           participant.sampleStatus1CFD9,
                           participant.sampleStatus1CFD9Time,
                           participant.sampleStatus1PXR2,
                           participant.sampleStatus1PXR2Time,
                           participant.sampleStatus1UR10,
                           participant.sampleStatus1UR10Time,
                           participant.sampleStatus1UR90,
                           participant.sampleStatus1UR90Time,
                           participant.sampleStatus1SAL,
                           participant.sampleStatus1SALTime,
                           participant.biospecimenSourceSite,
                           participant.primaryLanguage,
                           participant.consentForDvElectronicHealthRecordsSharing,
                           participant.loginPhoneNumber,
                           participant.patientStatus,
                           participant.enrollmentStatusCoreStoredSampleTime,
                           participant.participantOrigin,
                           participant.consentForGenomicsROR,
                           participant.consentForGenomicsRORAuthored,
                           participant.questionnaireOnCopeMay,
                           participant.questionnaireOnCopeMayTime,
                           participant.questionnaireOnCopeJune,
                           participant.questionnaireOnCopeJuneTime,
                           participant.questionnaireOnCopeJuly,
                           participant.questionnaireOnCopeJulyAuthored,
                           participant.consentCohort,
                           participant.questionnaireOnDnaProgram,
                           participant.questionnaireOnDnaProgramAuthored,
                           participant.ehrConsentExpireStatus,
                           participant.ehrConsentExpireAuthored,
                           participant.consentForStudyEnrollmentFirstYesAuthored,
                           participant.consentForElectronicHealthRecordsFirstYesAuthored,
                           participant.retentionEligibleStatus,
                           participant.retentionEligibleTime,
                           participant.deceasedStatus,
                           participant.dateOfDeath,
                           participant.deceasedAuthored,
                           participant.questionnaireOnCopeNov,
                           participant.questionnaireOnCopeNovAuthored,
                           participant.retentionType,
                           participant.isEhrDataAvailable,
                           participant.latestEhrReceiptTime,
                           participant.sample1SAL2CollectionMethod,
                           participant.questionnaireOnCopeDec,
                           participant.questionnaireOnCopeDecAuthored,
                           participant.questionnaireOnCopeFeb,
                           participant.questionnaireOnCopeFebAuthored,
                           participant.enrollmentStatusCoreMinusPMTime,
                           participant.questionnaireOnCopeVaccineMinute1,
                           participant.questionnaireOnCopeVaccineMinute1Authored,
                           participant.questionnaireOnCopeVaccineMinute2,
                           participant.questionnaireOnCopeVaccineMinute2Authored,
                           participant.digitalHealthSharingStatus,
                           participant.questionnaireOnPersonalAndFamilyHealthHistory,
                           participant.questionnaireOnPersonalAndFamilyHealthHistoryAuthored,
                           participant.questionnaireOnSocialDeterminantsOfHealth,
                           participant.questionnaireOnSocialDeterminantsOfHealthAuthored,
                           participant.questionnaireOnCopeVaccineMinute3,
                           participant.questionnaireOnCopeVaccineMinute3Authored,
                           participant.questionnaireOnCopeVaccineMinute4,
                           participant.questionnaireOnCopeVaccineMinute4Authored]
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
