import os
import datetime
from sqlalchemy import or_

from rdr_service import clock, config
from rdr_service.api_util import list_blobs, delete_cloud_file
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.hpo import HPO
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


def get_workqueue_participants():
    with ParticipantSummaryDao().session() as session:
        return session.query(ParticipantSummary
                            ).join(HPO, HPO.hpoId == ParticipantSummary.hpoId
                            ).join(Participant, Participant.participantId == ParticipantSummary.participantId
                            ).filter(HPO.name == 'VA',
                                     Participant.isTestParticipant != 1,
                                     # Just filtering on isGhostId != 1 will return no results
                                     or_(Participant.isGhostId != 1, Participant.isGhostId == None)
                            ).all()


def generate_workqueue_report():
    """ Creates csv file from ParticipantSummary table for participants paired to VA """
    bucket = config.getSetting(config.VA_WORKQUEUE_BUCKET_NAME)
    subfolder = config.getSetting(config.VA_WORKQUEUE_SUBFOLDER)
    file_timestamp = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")
    file_name = f'{FILE_PREFIX}{file_timestamp}.csv'
    participants = get_workqueue_participants()
    participants_new = []
    for p in participants:
        p_new = [p.participantId,
                 p.biobankId,
                 p.lastName,
                 p.firstName,
                 p.middleName,
                 p.dateOfBirth,
                 p.enrollmentStatus,
                 p.consentForStudyEnrollment,
                 p.consentForStudyEnrollmentAuthored,
                 p.consentForElectronicHealthRecords,
                 p.consentForElectronicHealthRecordsAuthored,
                 p.consentForCABoR,
                 p.consentForCABoRAuthored,
                 p.withdrawalStatus,
                 p.withdrawalAuthored,
                 p.suspensionStatus,
                 p.suspensionTime,
                 p.withdrawalReason,
                 p.streetAddress,
                 p.streetAddress2,
                 p.city,
                 p.state,
                 p.zipCode,
                 p.email,
                 p.phoneNumber,
                 p.ageRange,
                 p.sex,
                 p.genderIdentity,
                 p.race,
                 p.education,
                 p.numCompletedBaselinePPIModules,
                 p.numCompletedPPIModules,
                 p.questionnaireOnTheBasics,
                 p.questionnaireOnTheBasicsAuthored,
                 p.questionnaireOnOverallHealth,
                 p.questionnaireOnOverallHealthAuthored,
                 p.questionnaireOnLifestyle,
                 p.questionnaireOnLifestyleAuthored,
                 p.questionnaireOnMedicalHistory,
                 p.questionnaireOnMedicalHistoryAuthored,
                 p.questionnaireOnFamilyHealth,
                 p.questionnaireOnFamilyHealthAuthored,
                 p.questionnaireOnHealthcareAccess,
                 p.questionnaireOnHealthcareAccessAuthored,
                 p.physicalMeasurementsStatus,
                 p.physicalMeasurementsFinalizedTime,
                 p.site,
                 p.organization,
                 p.physicalMeasurementsFinalizedSite,
                 p.samplesToIsolateDNA,
                 p.numBaselineSamplesArrived,
                 p.sampleStatus1SST8,
                 p.sampleStatus1SST8Time,
                 p.sampleStatus1PST8,
                 p.sampleStatus1PST8Time,
                 p.sampleStatus1HEP4,
                 p.sampleStatus1HEP4Time,
                 p.sampleStatus1ED02,
                 p.sampleStatus1ED02Time,
                 p.sampleStatus1ED04,
                 p.sampleStatus1ED04Time,
                 p.sampleStatus1ED10,
                 p.sampleStatus1ED10Time,
                 p.sampleStatus2ED10,
                 p.sampleStatus2ED10Time,
                 p.sampleStatus1CFD9,
                 p.sampleStatus1CFD9Time,
                 p.sampleStatus1PXR2,
                 p.sampleStatus1PXR2Time,
                 p.sampleStatus1UR10,
                 p.sampleStatus1UR10Time,
                 p.sampleStatus1UR90,
                 p.sampleStatus1UR90Time,
                 p.sampleStatus1SAL,
                 p.sampleStatus1SALTime,
                 p.biospecimenSourceSite,
                 p.primaryLanguage,
                 p.consentForDvElectronicHealthRecordsSharing,
                 p.loginPhoneNumber,
                 p.patientStatus,
                 p.enrollmentStatusCoreStoredSampleTime,
                 p.participantOrigin,
                 p.consentForGenomicsROR,
                 p.consentForGenomicsRORAuthored,
                 p.questionnaireOnCopeMay,
                 p.questionnaireOnCopeMayTime,
                 p.questionnaireOnCopeJune,
                 p.questionnaireOnCopeJuneTime,
                 p.questionnaireOnCopeJuly,
                 p.questionnaireOnCopeJulyAuthored,
                 p.consentCohort,
                 p.questionnaireOnDnaProgram,
                 p.questionnaireOnDnaProgramAuthored,
                 p.ehrConsentExpireStatus,
                 p.ehrConsentExpireAuthored,
                 p.consentForStudyEnrollmentFirstYesAuthored,
                 p.consentForElectronicHealthRecordsFirstYesAuthored,
                 p.retentionEligibleStatus,
                 p.retentionEligibleTime,
                 p.deceasedStatus,
                 p.dateOfDeath,
                 p.deceasedAuthored,
                 p.questionnaireOnCopeNov,
                 p.questionnaireOnCopeNovAuthored,
                 p.retentionType,
                 p.isEhrDataAvailable,
                 p.latestEhrReceiptTime,
                 p.sample1SAL2CollectionMethod,
                 p.questionnaireOnCopeDec,
                 p.questionnaireOnCopeDecAuthored,
                 p.questionnaireOnCopeFeb,
                 p.questionnaireOnCopeFebAuthored,
                 p.enrollmentStatusCoreMinusPMTime,
                 p.questionnaireOnCopeVaccineMinute1,
                 p.questionnaireOnCopeVaccineMinute1Authored,
                 p.questionnaireOnCopeVaccineMinute2,
                 p.questionnaireOnCopeVaccineMinute2Authored,
                 p.digitalHealthSharingStatus,
                 p.questionnaireOnPersonalAndFamilyHealthHistory,
                 p.questionnaireOnPersonalAndFamilyHealthHistoryAuthored,
                 p.questionnaireOnSocialDeterminantsOfHealth,
                 p.questionnaireOnSocialDeterminantsOfHealthAuthored,
                 p.questionnaireOnCopeVaccineMinute3,
                 p.questionnaireOnCopeVaccineMinute3Authored,
                 p.questionnaireOnCopeVaccineMinute4,
                 p.questionnaireOnCopeVaccineMinute4Authored]
        participants_new.append(p_new)
    exporter = SqlExporter(bucket)
    with exporter.open_cloud_writer(subfolder+"/"+file_name) as writer:
        writer.write_header(CSV_HEADER)
        writer.write_rows(participants_new)


def delete_old_reports():
    """ Deletes export files that more than 7 days old """
    bucket = config.getSetting(config.VA_WORKQUEUE_BUCKET_NAME)
    subfolder = config.getSetting(config.VA_WORKQUEUE_SUBFOLDER)
    for file in list_blobs(bucket, subfolder):
        if file.name.endswith(".csv") and os.path.basename(file.name).startswith(FILE_PREFIX):
            file_time = _timestamp_from_filename(file.name)
            now = clock.CLOCK.now()
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
    except ValueError:
        raise RuntimeError("Can't parse time from CSV filename: %s" % csv_filename)
    return timestamp
