import datetime
import time
import mock

from rdr_service.clock import FakeClock
from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.concepts import Concept
from rdr_service.dao.calendar_dao import CalendarDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.metrics_cache_dao import MetricsEnrollmentStatusCacheDao
from rdr_service.researchers_offline.participant_counts_over_time import calculate_participant_metrics
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.calendar import Calendar
from rdr_service.model.code import Code, CodeType
from rdr_service.model.hpo import HPO
from rdr_service.model.site import Site
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import (
    EnrollmentStatus,
    OrganizationType,
    TEST_HPO_ID,
    TEST_HPO_NAME,
    make_primary_provider_link_for_name, MetricsCacheType,
)
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.mysql_helper_data import PITT_HPO_ID

TIME_1 = datetime.datetime(2017, 12, 31)
TIME_2 = datetime.datetime(2018, 1, 15)
TIME_3 = datetime.datetime(2018, 2, 10)


def _questionnaire_response_url(participant_id):
    return "Participant/%s/QuestionnaireResponse" % participant_id


class PublicMetricsApiTest(BaseTestCase):

    provider_link = {"primary": True, "organization": {"display": None, "reference": "Organization/PITT"}}

    az_provider_link = {"primary": True, "organization": {"display": None, "reference": "Organization/AZ_TUCSON"}}

    code_link_ids = (
        "race",
        "genderIdentity",
        "state",
        "sex",
        "sexualOrientation",
        "recontactMethod",
        "language",
        "education",
        "income",
    )

    string_link_ids = ("firstName", "middleName", "lastName", "streetAddress", "city", "phoneNumber", "zipCode")

    test_job_time = datetime.datetime.now().replace(microsecond=0)

    def setUp(self):
        super(PublicMetricsApiTest, self).setUp()
        self.dao = ParticipantDao()
        self.ps_dao = ParticipantSummaryDao()
        self.ps = ParticipantSummary()
        self.calendar_dao = CalendarDao()
        self.hpo_dao = HPODao()
        self.org_dao = OrganizationDao()
        self.code_dao = CodeDao()

        self.hpo_dao.insert(
            HPO(hpoId=TEST_HPO_ID, name=TEST_HPO_NAME, displayName="Test", organizationType=OrganizationType.UNSET)
        )

        self.time1 = datetime.datetime(2017, 12, 31)
        self.time2 = datetime.datetime(2018, 1, 1)
        self.time3 = datetime.datetime(2018, 1, 2)
        self.time4 = datetime.datetime(2018, 1, 3)
        self.time5 = datetime.datetime(2018, 1, 4)

        # Insert 2 weeks of dates
        curr_date = datetime.date(2017, 12, 22)
        for _ in range(0, 18):
            calendar_day = Calendar(day=curr_date)
            CalendarDao().insert(calendar_day)
            curr_date = curr_date + datetime.timedelta(days=1)

    def tearDown(self):
        self.clear_table_after_test("rdr.metrics_enrollment_status_cache")
        self.clear_table_after_test("rdr.metrics_gender_cache")
        self.clear_table_after_test("rdr.metrics_age_cache")
        self.clear_table_after_test("rdr.metrics_race_cache")
        self.clear_table_after_test("rdr.metrics_region_cache")
        self.clear_table_after_test("rdr.metrics_lifecycle_cache")
        self.clear_table_after_test("rdr.metrics_language_cache")

        super().tearDown()

    def _insert(
        self,
        participant,
        first_name=None,
        last_name=None,
        hpo_name=None,
        org_name=None,
        unconsented=False,
        time_int=None,
        time_study=None,
        time_mem=None,
        time_fp=None,
        time_fp_stored=None,
        gender_id=None,
        dob=None,
        state_id=None,
        primary_language=None,
        gender_identity=None,
    ):
        """
    Create a participant in a transient test database.

    :param participant: Participant object
    :param first_name: First name
    :param last_name: Last name
    :param hpo_name: HPO name (one of PITT or AZ_TUCSON)
    :param org_name: Org external_id (one of PITT_BANNER_HEALTH or AZ_TUCSON_BANNER_HEALTH)
    :param time_int: Time that participant fulfilled INTERESTED criteria
    :param time_mem: Time that participant fulfilled MEMBER criteria
    :param time_fp: Time that participant fulfilled FULL_PARTICIPANT criteria
    :return: Participant object
    """

        if unconsented is True:
            enrollment_status = None
        elif time_mem is None:
            enrollment_status = EnrollmentStatus.INTERESTED
        elif time_fp is None:
            enrollment_status = EnrollmentStatus.MEMBER
        else:
            enrollment_status = EnrollmentStatus.FULL_PARTICIPANT

        with FakeClock(time_int):
            self.dao.insert(participant)

        participant.providerLink = make_primary_provider_link_for_name(hpo_name)
        with FakeClock(time_mem):
            self.dao.update(participant)

        summary = self.participant_summary(participant)

        if first_name:
            summary.firstName = first_name
        if last_name:
            summary.lastName = last_name

        if gender_id:
            summary.genderIdentityId = gender_id
        if gender_identity:
            summary.genderIdentity = gender_identity
        if dob:
            summary.dateOfBirth = dob
        else:
            summary.dateOfBirth = datetime.date(1978, 10, 10)
        if state_id:
            summary.stateId = state_id

        if primary_language:
            summary.primaryLanguage = primary_language

        summary.enrollmentStatus = enrollment_status

        summary.enrollmentStatusMemberTime = time_mem
        summary.enrollmentStatusCoreOrderedSampleTime = time_fp
        summary.enrollmentStatusCoreStoredSampleTime = time_fp_stored

        summary.hpoId = self.hpo_dao.get_by_name(hpo_name).hpoId
        if org_name:
            summary.organizationId = self.org_dao.get_by_external_id(org_name).organizationId

        if time_study is not None:
            with FakeClock(time_mem):
                summary.consentForStudyEnrollmentTime = time_study

        if time_mem is not None:
            with FakeClock(time_mem):
                summary.consentForElectronicHealthRecords = 1
                summary.consentForElectronicHealthRecordsTime = time_mem

        if time_fp is not None:
            with FakeClock(time_fp):
                if not summary.consentForElectronicHealthRecords:
                    summary.consentForElectronicHealthRecords = 1
                    summary.consentForElectronicHealthRecordsTime = time_fp
                summary.questionnaireOnTheBasicsTime = time_fp
                summary.questionnaireOnLifestyleTime = time_fp
                summary.questionnaireOnOverallHealthTime = time_fp
                summary.questionnaireOnHealthcareAccessTime = time_fp
                summary.questionnaireOnMedicalHistoryTime = time_fp
                summary.questionnaireOnMedicationsTime = time_fp
                summary.questionnaireOnFamilyHealthTime = time_fp
                summary.clinicPhysicalMeasurementsFinalizedTime = time_fp
                summary.clinicPhysicalMeasurementsTime = time_fp
                summary.sampleOrderStatus1ED04Time = time_fp
                summary.sampleOrderStatus1SALTime = time_fp
                summary.sampleStatus1ED04Time = time_fp
                summary.sampleStatus1SALTime = time_fp

        self.ps_dao.insert(summary)

        return summary

    def update_participant_summary(
        self, participant_id, time_mem=None, time_fp=None, time_fp_stored=None, time_study=None
    ):

        participant = self.dao.get(participant_id)
        summary = self.participant_summary(participant)
        if time_mem is None:
            enrollment_status = EnrollmentStatus.INTERESTED
        elif time_fp is None:
            enrollment_status = EnrollmentStatus.MEMBER
        else:
            enrollment_status = EnrollmentStatus.FULL_PARTICIPANT

        summary.enrollmentStatus = enrollment_status

        summary.enrollmentStatusMemberTime = time_mem
        summary.enrollmentStatusCoreOrderedSampleTime = time_fp
        summary.enrollmentStatusCoreStoredSampleTime = time_fp_stored

        if time_study is not None:
            with FakeClock(time_mem):
                summary.consentForStudyEnrollmentTime = time_study

        if time_mem is not None:
            with FakeClock(time_mem):
                summary.consentForElectronicHealthRecords = 1
                summary.consentForElectronicHealthRecordsTime = time_mem

        if time_fp is not None:
            with FakeClock(time_fp):
                if not summary.consentForElectronicHealthRecords:
                    summary.consentForElectronicHealthRecords = 1
                    summary.consentForElectronicHealthRecordsTime = time_fp
                summary.questionnaireOnTheBasicsTime = time_fp
                summary.questionnaireOnLifestyleTime = time_fp
                summary.questionnaireOnOverallHealthTime = time_fp
                summary.questionnaireOnHealthcareAccessTime = time_fp
                summary.questionnaireOnMedicalHistoryTime = time_fp
                summary.questionnaireOnMedicationsTime = time_fp
                summary.questionnaireOnFamilyHealthTime = time_fp
                summary.clinicPhysicalMeasurementsFinalizedTime = time_fp
                summary.clinicPhysicalMeasurementsTime = time_fp
                summary.sampleOrderStatus1ED04Time = time_fp
                summary.sampleOrderStatus1SALTime = time_fp
                summary.sampleStatus1ED04Time = time_fp
                summary.sampleStatus1SALTime = time_fp

        self.ps_dao.update(summary)

        return summary

    def get_mock_data(self):
        insert_time = self.test_job_time

        enrollment_results_az = [
            {
                "dateInserted": insert_time,
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "registeredCount": "0",
                "participantCount": "1",
                "consentedCount": "0",
                "coreCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "registeredCount": "0",
                "participantCount": "2",
                "consentedCount": "0",
                "coreCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "registeredCount": "0",
                "participantCount": "1",
                "consentedCount": "1",
                "coreCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "registeredCount": "0",
                "participantCount": "1",
                "consentedCount": "0",
                "coreCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "registeredCount": "0",
                "participantCount": "1",
                "consentedCount": "0",
                "coreCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-07",
                "registeredCount": "0",
                "participantCount": "1",
                "consentedCount": "0",
                "coreCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-08",
                "registeredCount": "0",
                "participantCount": "1",
                "consentedCount": "0",
                "coreCount": "1",
                "participantOrigin": "example"
            },
        ]
        enrollment_results_unst = [
            {
                "dateInserted": insert_time,
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-31",
                "registeredCount": "1",
                "participantCount": "0",
                "consentedCount": "0",
                "coreCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-01",
                "registeredCount": "1",
                "participantCount": "0",
                "consentedCount": "0",
                "coreCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-02",
                "registeredCount": "1",
                "participantCount": "0",
                "consentedCount": "0",
                "coreCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-03",
                "registeredCount": "1",
                "participantCount": "0",
                "consentedCount": "0",
                "coreCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-07",
                "registeredCount": "1",
                "participantCount": "0",
                "consentedCount": "0",
                "coreCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-08",
                "registeredCount": "1",
                "participantCount": "0",
                "consentedCount": "0",
                "coreCount": "0",
                "participantOrigin": "example"
            }
        ]
        age_results_az = [
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "ageRange": "18-29",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "ageRange": "18-29",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "ageRange": "18-29",
                "ageCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "ageRange": "18-29",
                "ageCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "ageRange": "18-29",
                "ageCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "ageRange": "18-29",
                "ageCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "ageRange": "18-29",
                "ageCount": "2",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "ageRange": "30-39",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "ageRange": "30-39",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "ageRange": "30-39",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "ageRange": "30-39",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {"dateInserted": insert_time,
             "type": "PUBLIC_METRICS_EXPORT_API",
             "enrollment_status": "consented",
             "hpoId": 4,
             "hpoName": "AZ_TUCSON",
             "date": "2017-12-31",
             "ageRange": "40-49",
             "ageCount": "0",
             "participantOrigin": "example"
             },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "ageRange": "50-59",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "ageRange": "60-69",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "ageRange": "70-79",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "ageRange": "80-89",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "ageRange": "90-",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "ageRange": "UNSET",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "ageRange": "40-49",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "ageRange": "50-59",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "ageRange": "60-69",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "ageRange": "70-79",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "ageRange": "80-89",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "ageRange": "90-",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "ageRange": "UNSET",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "ageRange": "40-49",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "ageRange": "50-59",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "ageRange": "60-69",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "ageRange": "70-79",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "ageRange": "80-89",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "ageRange": "90-",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "ageRange": "UNSET",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "ageRange": "40-49",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "ageRange": "50-59",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "ageRange": "60-69",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "ageRange": "70-79",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "ageRange": "80-89",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "ageRange": "90-",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "ageRange": "UNSET",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "ageRange": "18-29",
                "ageCount": "3",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "ageRange": "30-39",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "ageRange": "40-49",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "ageRange": "50-59",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "ageRange": "60-69",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "ageRange": "70-79",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "ageRange": "80-89",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "ageRange": "90-",
                "ageCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "ageRange": "UNSET",
                "ageCount": "0",
                "participantOrigin": "example"
            }
        ]
        age_results_unst = [
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-31",
                "ageRange": "30-39",
                "ageCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-01",
                "ageRange": "30-39",
                "ageCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-02",
                "ageRange": "30-39",
                "ageCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-03",
                "ageRange": "30-39",
                "ageCount": "1",
                "participantOrigin": "example"
            },
        ]
        lang_results_az = [
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-30",
                "languageName": "ES",
                "languageCount": "0",
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "languageName": "UNSET",
                "languageCount": "2",
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "languageName": "ES",
                "languageCount": "0",
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "languageName": "ES",
                "languageCount": "1",
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "languageName": "UNSET",
                "languageCount": "2",
            }
        ]
        lang_results_unst = [
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "registered",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-30",
                "languageName": "UNSET",
                "languageCount": "0",
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "registered",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-30",
                "languageName": "EN",
                "languageCount": "0",
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "registered",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-31",
                "languageName": "EN",
                "languageCount": "1",
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "registered",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-03",
                "languageName": "EN",
                "languageCount": "1",
            }
        ]
        region_results_az = [
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "stateName": "PIIState_IN",
                "stateCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "stateName": "PIIState_IN",
                "stateCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "stateName": "PIIState_IN",
                "stateCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "stateName": "PIIState_CA",
                "stateCount": "1",
                "participantOrigin": "example"
            }
        ]
        region_results_unst = [
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-31",
                "stateName": "PIIState_IL",
                "stateCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-01",
                "stateName": "PIIState_IL",
                "stateCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-02",
                "stateName": "PIIState_IL",
                "stateCount": "1",
                "participantOrigin": "example"
            },
        ]
        region_results_pitt = [
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2017-12-31",
                "stateName": "PIIState_PR",
                "stateCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-01",
                "stateName": "PIIState_PR",
                "stateCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-01",
                "stateName": "PIIState_IN",
                "stateCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-02",
                "stateName": "PIIState_IN",
                "stateCount": "2",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "enrollmentStatus": "consented",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-02",
                "stateName": "PIIState_PR",
                "stateCount": "1",
                "participantOrigin": "example"
            }
        ]
        lifecycle_results_az = [
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "registered": "0",
                "consentEnrollment": "0",
                "consentComplete": "0",
                "ppiBasics": "0",
                "ppiOverallHealth": "0",
                "ppiLifestyle": "0",
                "ppiHealthcareAccess": "0",
                "ppiMedicalHistory": "0",
                "ppiMedications": "0",
                "ppiFamilyHealth": "0",
                "ppiBaselineComplete": "0",
                "retentionModulesEligible": "0",
                "retentionModulesComplete": "0",
                "physicalMeasurement": "0",
                "sampleReceived": "0",
                "fullParticipant": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "registered": "1",
                "consentEnrollment": "1",
                "consentComplete": "1",
                "ppiBasics": "1",
                "ppiOverallHealth": "1",
                "ppiLifestyle": "1",
                "ppiHealthcareAccess": "1",
                "ppiMedicalHistory": "1",
                "ppiMedications": "1",
                "ppiFamilyHealth": "1",
                "ppiBaselineComplete": "1",
                "retentionModulesEligible": "1",
                "retentionModulesComplete": "1",
                "physicalMeasurement": "1",
                "sampleReceived": "1",
                "fullParticipant": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "registered",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "registered": "2",
                "consentEnrollment": "2",
                "consentComplete": "2",
                "ppiBasics": "2",
                "ppiOverallHealth": "1",
                "ppiLifestyle": "1",
                "ppiHealthcareAccess": "1",
                "ppiMedicalHistory": "1",
                "ppiMedications": "1",
                "ppiFamilyHealth": "1",
                "ppiBaselineComplete": "1",
                "retentionModulesEligible": "1",
                "retentionModulesComplete": "1",
                "physicalMeasurement": "1",
                "sampleReceived": "2",
                "fullParticipant": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-06",
                "registered": "2",
                "consentEnrollment": "2",
                "consentComplete": "2",
                "ppiBasics": "2",
                "ppiOverallHealth": "2",
                "ppiLifestyle": "2",
                "ppiHealthcareAccess": "2",
                "ppiMedicalHistory": "2",
                "ppiMedications": "2",
                "ppiFamilyHealth": "2",
                "ppiBaselineComplete": "2",
                "retentionModulesEligible": "2",
                "retentionModulesComplete": "2",
                "physicalMeasurement": "2",
                "sampleReceived": "2",
                "fullParticipant": "2",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-08",
                "registered": "2",
                "consentEnrollment": "2",
                "consentComplete": "2",
                "ppiBasics": "2",
                "ppiOverallHealth": "2",
                "ppiLifestyle": "2",
                "ppiHealthcareAccess": "2",
                "ppiMedicalHistory": "2",
                "ppiMedications": "2",
                "ppiFamilyHealth": "2",
                "ppiBaselineComplete": "2",
                "retentionModulesEligible": "2",
                "retentionModulesComplete": "2",
                "physicalMeasurement": "2",
                "sampleReceived": "2",
                "fullParticipant": "2",
                "participantOrigin": "example"
            }
        ]
        lifecycle_results_unst = [
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "registered",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-03",
                "registered": "1",
                "consentEnrollment": "1",
                "consentComplete": "1",
                "ppiBasics": "1",
                "ppiOverallHealth": "1",
                "ppiLifestyle": "1",
                "ppiHealthcareAccess": "1",
                "ppiMedicalHistory": "1",
                "ppiMedications": "1",
                "ppiFamilyHealth": "1",
                "ppiBaselineComplete": "1",
                "retentionModulesEligible": "1",
                "retentionModulesComplete": "1",
                "physicalMeasurement": "1",
                "sampleReceived": "1",
                "fullParticipant": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "consented",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-08",
                "registered": "1",
                "consentEnrollment": "1",
                "consentComplete": "1",
                "ppiBasics": "1",
                "ppiOverallHealth": "1",
                "ppiLifestyle": "1",
                "ppiHealthcareAccess": "1",
                "ppiMedicalHistory": "1",
                "ppiMedications": "1",
                "ppiFamilyHealth": "1",
                "ppiBaselineComplete": "1",
                "retentionModulesEligible": "1",
                "retentionModulesComplete": "1",
                "physicalMeasurement": "1",
                "sampleReceived": "1",
                "fullParticipant": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "registered",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-31",
                "registered": "1",
                "consentEnrollment": "1",
                "consentComplete": "1",
                "ppiBasics": "1",
                "ppiOverallHealth": "1",
                "ppiLifestyle": "1",
                "ppiHealthcareAccess": "1",
                "ppiMedicalHistory": "1",
                "ppiMedications": "1",
                "ppiFamilyHealth": "1",
                "ppiBaselineComplete": "1",
                "retentionModulesEligible": "1",
                "retentionModulesComplete": "1",
                "physicalMeasurement": "1",
                "sampleReceived": "1",
                "fullParticipant": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "registered",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-02",
                "registered": "1",
                "consentEnrollment": "1",
                "consentComplete": "1",
                "ppiBasics": "1",
                "ppiOverallHealth": "1",
                "ppiLifestyle": "1",
                "ppiHealthcareAccess": "1",
                "ppiMedicalHistory": "1",
                "ppiMedications": "1",
                "ppiFamilyHealth": "1",
                "ppiBaselineComplete": "1",
                "retentionModulesEligible": "1",
                "retentionModulesComplete": "1",
                "physicalMeasurement": "1",
                "sampleReceived": "1",
                "fullParticipant": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "consented",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-06",
                "registered": "1",
                "consentEnrollment": "1",
                "consentComplete": "1",
                "ppiBasics": "1",
                "ppiOverallHealth": "1",
                "ppiLifestyle": "1",
                "ppiHealthcareAccess": "1",
                "ppiMedicalHistory": "1",
                "ppiMedications": "1",
                "ppiFamilyHealth": "1",
                "ppiBaselineComplete": "1",
                "retentionModulesEligible": "1",
                "retentionModulesComplete": "1",
                "physicalMeasurement": "1",
                "sampleReceived": "1",
                "fullParticipant": "1",
                "participantOrigin": "example"
            }
        ]
        lifecycle_results_pitt = [
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "registered",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-03",
                "registered": "2",
                "consentEnrollment": "2",
                "consentComplete": "1",
                "ppiBasics": "0",
                "ppiOverallHealth": "1",
                "ppiLifestyle": "1",
                "ppiHealthcareAccess": "1",
                "ppiMedicalHistory": "1",
                "ppiMedications": "1",
                "ppiFamilyHealth": "1",
                "ppiBaselineComplete": "1",
                "retentionModulesEligible": "0",
                "retentionModulesComplete": "0",
                "physicalMeasurement": "1",
                "sampleReceived": "0",
                "fullParticipant": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "consented",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-08",
                "registered": "2",
                "consentEnrollment": "2",
                "consentComplete": "2",
                "ppiBasics": "2",
                "ppiOverallHealth": "2",
                "ppiLifestyle": "2",
                "ppiHealthcareAccess": "2",
                "ppiMedicalHistory": "2",
                "ppiMedications": "2",
                "ppiFamilyHealth": "2",
                "ppiBaselineComplete": "2",
                "retentionModulesEligible": "2",
                "retentionModulesComplete": "2",
                "physicalMeasurement": "2",
                "sampleReceived": "2",
                "fullParticipant": "2",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "registered",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2017-12-31",
                "registered": "0",
                "consentEnrollment": "0",
                "consentComplete": "0",
                "ppiBasics": "0",
                "ppiOverallHealth": "0",
                "ppiLifestyle": "0",
                "ppiHealthcareAccess": "0",
                "ppiMedicalHistory": "0",
                "ppiMedications": "0",
                "ppiFamilyHealth": "0",
                "ppiBaselineComplete": "0",
                "retentionModulesEligible": "0",
                "retentionModulesComplete": "0",
                "physicalMeasurement": "0",
                "sampleReceived": "0",
                "fullParticipant": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "registered",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-02",
                "registered": "0",
                "consentEnrollment": "0",
                "consentComplete": "0",
                "ppiBasics": "0",
                "ppiOverallHealth": "0",
                "ppiLifestyle": "0",
                "ppiHealthcareAccess": "0",
                "ppiMedicalHistory": "0",
                "ppiMedications": "0",
                "ppiFamilyHealth": "0",
                "ppiBaselineComplete": "0",
                "retentionModulesEligible": "0",
                "retentionModulesComplete": "0",
                "physicalMeasurement": "0",
                "sampleReceived": "0",
                "fullParticipant": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollmentStatus": "consented",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-06",
                "registered": "2",
                "consentEnrollment": "2",
                "consentComplete": "2",
                "ppiBasics": "2",
                "ppiOverallHealth": "2",
                "ppiLifestyle": "2",
                "ppiHealthcareAccess": "2",
                "ppiMedicalHistory": "2",
                "ppiMedications": "2",
                "ppiFamilyHealth": "2",
                "ppiBaselineComplete": "2",
                "retentionModulesEligible": "2",
                "retentionModulesComplete": "2",
                "physicalMeasurement": "2",
                "sampleReceived": "2",
                "fullParticipant": "2",
                "participantOrigin": "example"
            }
        ]
        gender_results_az = [
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "genderName": "Man",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "genderName": "Man",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "genderName": "Transgender",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "genderName": "Man",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "genderName": "Transgender",
                "genderCount": "2",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "genderName": "Man",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "genderName": "Transgender",
                "genderCount": "2",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "genderName": "More than one gender identity",
                "genderCount": "1",
                "participantOrigin": "example"
            },
        ]
        gender_results_unst = [
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-31",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-01",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-02",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-03",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "enrollment_status": "",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-04",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            }
        ]
        gender_v2_results_az = [
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "genderName": "Man",
                "genderCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "genderName": "Woman",
                "genderCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "genderName": "Man",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "genderName": "Woman",
                "genderCount": "0",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "genderName": "Man",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "genderName": "Transgender",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "genderName": "Man",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-03",
                "genderName": "Transgender",
                "genderCount": "2",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "genderName": "Man",
                "genderCount": "2",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "genderName": "Transgender",
                "genderCount": "2",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "consented",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-04",
                "genderName": "More than one gender identity",
                "genderCount": "1",
                "participantOrigin": "example"
            },
        ]
        gender_v2_results_unst = [
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-31",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-01",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-02",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-03",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "enrollment_status": "",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-04",
                "genderName": "Woman",
                "genderCount": "1",
                "participantOrigin": "example"
            }
        ]
        race_results_az = [
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "0",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "1",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "0",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "1",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "1",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            }
        ]
        race_results_unst = [
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-31",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "1",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-01",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "1",
                "preferNotToAnswer": "0",
                "multiAncestry": "1",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-02",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "1",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "1",
                "preferNotToAnswer": "0",
                "multiAncestry": "1",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "1"
            }
        ]
        race_results_pitt = [
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2017-12-31",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "0",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-01",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "0",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-02",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "0",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-03",
                "registeredFlag": 1,
                "participantFlag": 1,
                "consentedFlag": 1,
                "coreFlag": 1,
                "americanIndianAlaskaNative": "1",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "1",
                "preferNotToAnswer": "0",
                "multiAncestry": "2",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "PUBLIC_METRICS_EXPORT_API",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-04",
                "registeredFlag": 1,
                "participantFlag": 1,
                "consentedFlag": 1,
                "coreFlag": 1,
                "americanIndianAlaskaNative": "1",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "1",
                "noAncestryChecked": "1",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            }
        ]
        race_v2_results_az = [
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2017-12-31",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "0",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-01",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "1",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "0",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 4,
                "hpoName": "AZ_TUCSON",
                "date": "2018-01-02",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "2",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "1",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "1",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            }
        ]
        race_v2_results_unst = [
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2017-12-31",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "1",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-01",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "1",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 0,
                "hpoName": "UNSET",
                "date": "2018-01-02",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "1",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "0",
                "hispanicLatinoSpanish": "0",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "1",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "1"
            }
        ]
        race_v2_results_pitt = [
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2017-12-31",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "1",
                "hispanicLatinoSpanish": "1",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "0",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-01",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "1",
                "hispanicLatinoSpanish": "1",
                "noneOfTheseFullyDescribeMe": "1",
                "preferNotToAnswer": "0",
                "multiAncestry": "0",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-02",
                "registeredFlag": 0,
                "participantFlag": 0,
                "consentedFlag": 0,
                "coreFlag": 0,
                "americanIndianAlaskaNative": "0",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "1",
                "hispanicLatinoSpanish": "1",
                "noneOfTheseFullyDescribeMe": "1",
                "preferNotToAnswer": "0",
                "multiAncestry": "0",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-03",
                "registeredFlag": 1,
                "participantFlag": 1,
                "consentedFlag": 1,
                "coreFlag": 1,
                "americanIndianAlaskaNative": "1",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "2",
                "hispanicLatinoSpanish": "2",
                "noneOfTheseFullyDescribeMe": "1",
                "preferNotToAnswer": "0",
                "multiAncestry": "2",
                "noAncestryChecked": "0",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            },
            {
                "dateInserted": insert_time,
                "type": "METRICS_V2_API",
                "hpoId": 2,
                "hpoName": "PITT",
                "date": "2018-01-04",
                "registeredFlag": 1,
                "participantFlag": 1,
                "consentedFlag": 1,
                "coreFlag": 1,
                "americanIndianAlaskaNative": "1",
                "asian": "0",
                "blackAfricanAmerican": "0",
                "middleEasternNorthAfrican": "0",
                "nativeHawaiianOtherPacificIslander": "0",
                "white": "1",
                "hispanicLatinoSpanish": "1",
                "noneOfTheseFullyDescribeMe": "0",
                "preferNotToAnswer": "0",
                "multiAncestry": "1",
                "noAncestryChecked": "1",
                "participantOrigin": "example",
                "unsetNoBasics": "0"
            }
        ]

        return [lifecycle_results_unst, lifecycle_results_pitt, lifecycle_results_az,
                gender_results_unst, [], gender_results_az,
                age_results_unst, [], age_results_az,
                race_results_unst, race_results_pitt, race_results_az,
                enrollment_results_unst, [], enrollment_results_az,
                region_results_unst, region_results_pitt, region_results_az,
                lang_results_unst, [], lang_results_az,
                gender_v2_results_unst, [], gender_v2_results_az,  [],
                [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [],
                [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [],
                [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [],
                [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [],
                race_v2_results_unst, race_v2_results_pitt, race_v2_results_az]

    def get_api_mock_data(self, start_date, end_date, hpo_ids=None, participant_origins=None):
        test_dao = MetricsEnrollmentStatusCacheDao(MetricsCacheType.PUBLIC_METRICS_EXPORT_API)
        return test_dao.get_active_buckets(start_date, end_date, hpo_ids, participant_origins)

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_enrollment_status_api(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        calculate_participant_metrics()

        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 72)

        qs = "&stratification=ENROLLMENT_STATUS" "&startDate=2018-01-01" "&endDate=2018-01-08"
        big_query().query.side_effect = [self.get_api_mock_data('2018-01-01', '2018-01-08')]
        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn({"date": "2018-01-01", "metrics": {"consented": 0, "core": 0, "registered": 1}}, results)
        self.assertIn({"date": "2018-01-02", "metrics": {"consented": 1, "core": 0, "registered": 1}}, results)
        self.assertIn({"date": "2018-01-03", "metrics": {"consented": 0, "core": 1, "registered": 1}}, results)

        qs = "&stratification=ENROLLMENT_STATUS" "&startDate=2018-01-01" "&endDate=2018-01-08" "&awardee=AZ_TUCSON"
        big_query().query.side_effect = [self.get_api_mock_data('2018-01-01', '2018-01-08', [4])]
        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn({"date": "2018-01-01", "metrics": {"consented": 0, "core": 0, "registered": 0}}, results)
        self.assertIn({"date": "2018-01-02", "metrics": {"consented": 1, "core": 0, "registered": 0}}, results)
        self.assertIn({"date": "2018-01-03", "metrics": {"consented": 0, "core": 1, "registered": 0}}, results)

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_gender_api(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        calculate_participant_metrics()

        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 72)

        qs = "&stratification=GENDER_IDENTITY" "&startDate=2017-12-31" "&endDate=2018-01-08"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2017-12-31",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 0,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 1,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-03",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 2,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-04",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 2,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 1,
                },
            },
            results,
        )

        qs = "&stratification=GENDER_IDENTITY" "&startDate=2017-12-31" "&endDate=2018-01-08" "&awardee=AZ_TUCSON"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 1,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-03",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 2,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-04",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 2,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 1,
                },
            },
            results,
        )

        qs = (
            "&stratification=GENDER_IDENTITY"
            "&startDate=2017-12-31"
            "&endDate=2018-01-08"
            "&awardee=AZ_TUCSON"
            "&enrollmentStatus=MEMBER"
        )

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-04",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 2,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 1,
                },
            },
            results,
        )

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_gender_api_v2(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        with FakeClock(TIME_2):
            calculate_participant_metrics()

        time.sleep(2)

        # test copy historical cache for stage two
        with FakeClock(TIME_3):
            calculate_participant_metrics()

        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 132)

        qs = "&stratification=GENDER_IDENTITY" "&startDate=2017-12-31" "&endDate=2018-01-08" "&version=2"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2017-12-31",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 0,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 1,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-03",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 2,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-04",
                "metrics": {
                    "Woman": 2,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 2,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 2,
                    "More than one gender identity": 1,
                },
            },
            results,
        )

        qs = (
            "&stratification=GENDER_IDENTITY"
            "&startDate=2017-12-31"
            "&endDate=2018-01-08"
            "&awardee=AZ_TUCSON"
            "&version=2"
        )

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2017-12-31",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 0,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 1,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-03",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 2,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-04",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 2,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 2,
                    "More than one gender identity": 1,
                },
            },
            results,
        )

        qs = (
            "&stratification=GENDER_IDENTITY"
            "&startDate=2017-12-31"
            "&endDate=2018-01-08"
            "&awardee=AZ_TUCSON"
            "&enrollmentStatus=MEMBER"
            "&version=2"
        )

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2017-12-31",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 0,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 0,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "Woman": 0,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 0,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 1,
                    "More than one gender identity": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-04",
                "metrics": {
                    "Woman": 1,
                    "PMI_Skip": 0,
                    "Other/Additional Options": 0,
                    "Non-Binary": 0,
                    "UNMAPPED": 0,
                    "Transgender": 2,
                    "Prefer not to say": 0,
                    "UNSET": 0,
                    "Man": 2,
                    "More than one gender identity": 1,
                },
            },
            results,
        )

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_age_range_api(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        with FakeClock(TIME_2):
            calculate_participant_metrics()

        time.sleep(2)

        # test copy historical cache for stage two
        with FakeClock(TIME_3):
            calculate_participant_metrics()

        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 132)

        qs = "&stratification=AGE_RANGE" "&startDate=2017-12-31" "&endDate=2018-01-08"

        results = self.send_get("PublicMetrics", query_string=qs)

        self.assertIn(
            {
                "date": "2017-12-31",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 1,
                    "40-49": 0,
                    "UNSET": 0,
                    "80-89": 0,
                    "90-": 0,
                    "18-29": 0,
                    "70-79": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 1,
                    "40-49": 0,
                    "18-29": 1,
                    "80-89": 0,
                    "90-": 0,
                    "UNSET": 0,
                    "70-79": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 1,
                    "40-49": 0,
                    "18-29": 2,
                    "80-89": 0,
                    "70-79": 0,
                    "UNSET": 0,
                    "90-": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-03",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 1,
                    "40-49": 0,
                    "18-29": 3,
                    "80-89": 0,
                    "70-79": 0,
                    "UNSET": 0,
                    "90-": 0,
                },
            },
            results,
        )

        qs = "&stratification=AGE_RANGE" "&startDate=2017-12-31" "&endDate=2018-01-08" "&awardee=AZ_TUCSON"

        results = self.send_get("PublicMetrics", query_string=qs)

        self.assertIn(
            {
                "date": "2017-12-31",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 0,
                    "40-49": 0,
                    "UNSET": 0,
                    "80-89": 0,
                    "90-": 0,
                    "18-29": 0,
                    "70-79": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 0,
                    "40-49": 0,
                    "18-29": 1,
                    "80-89": 0,
                    "90-": 0,
                    "UNSET": 0,
                    "70-79": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 0,
                    "40-49": 0,
                    "18-29": 2,
                    "80-89": 0,
                    "70-79": 0,
                    "UNSET": 0,
                    "90-": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-03",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 0,
                    "40-49": 0,
                    "18-29": 3,
                    "80-89": 0,
                    "70-79": 0,
                    "UNSET": 0,
                    "90-": 0,
                },
            },
            results,
        )

        qs = (
            "&stratification=AGE_RANGE"
            "&startDate=2017-12-31"
            "&endDate=2018-01-08"
            "&awardee=AZ_TUCSON"
            "&enrollmentStatus=MEMBER"
        )

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2017-12-31",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 0,
                    "40-49": 0,
                    "UNSET": 0,
                    "80-89": 0,
                    "90-": 0,
                    "18-29": 0,
                    "70-79": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 0,
                    "40-49": 0,
                    "18-29": 0,
                    "80-89": 0,
                    "90-": 0,
                    "UNSET": 0,
                    "70-79": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 0,
                    "40-49": 0,
                    "18-29": 1,
                    "80-89": 0,
                    "70-79": 0,
                    "UNSET": 0,
                    "90-": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-03",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 0,
                    "40-49": 0,
                    "18-29": 1,
                    "80-89": 0,
                    "70-79": 0,
                    "UNSET": 0,
                    "90-": 0,
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-04",
                "metrics": {
                    "50-59": 0,
                    "60-69": 0,
                    "30-39": 0,
                    "40-49": 0,
                    "18-29": 3,
                    "80-89": 0,
                    "70-79": 0,
                    "UNSET": 0,
                    "90-": 0,
                },
            },
            results,
        )

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_total_api(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        calculate_participant_metrics()

        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 72)

        qs = "&stratification=TOTAL" "&startDate=2017-12-31" "&endDate=2018-01-08"

        response = self.send_get("PublicMetrics", query_string=qs)

        self.assertIn({"date": "2017-12-31", "metrics": {"TOTAL": 2}}, response)
        self.assertIn({"date": "2018-01-01", "metrics": {"TOTAL": 3}}, response)
        self.assertIn({"date": "2018-01-02", "metrics": {"TOTAL": 3}}, response)
        self.assertIn({"date": "2018-01-07", "metrics": {"TOTAL": 3}}, response)
        self.assertIn({"date": "2018-01-08", "metrics": {"TOTAL": 3}}, response)

        qs = "&stratification=TOTAL" "&startDate=2017-12-31" "&endDate=2018-01-08" "&awardee=AZ_TUCSON"

        response = self.send_get("PublicMetrics", query_string=qs)

        self.assertIn({"date": "2017-12-31", "metrics": {"TOTAL": 1}}, response)
        self.assertIn({"date": "2018-01-01", "metrics": {"TOTAL": 2}}, response)
        self.assertIn({"date": "2018-01-02", "metrics": {"TOTAL": 2}}, response)
        self.assertIn({"date": "2018-01-07", "metrics": {"TOTAL": 2}}, response)
        self.assertIn({"date": "2018-01-08", "metrics": {"TOTAL": 2}}, response)

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_race_api(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        calculate_participant_metrics()

        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 72)

        qs = "&stratification=RACE" "&startDate=2017-12-31" "&endDate=2018-01-08"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2017-12-31",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 0,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 1,
                    "American_Indian_Alaska_Native": 0,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 0,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 0,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 1,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 1,
                    "American_Indian_Alaska_Native": 1,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 0,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 0,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 1,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 2,
                    "American_Indian_Alaska_Native": 2,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 0,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 0,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 1
                },
            },
            results,
        )

        qs = "&stratification=RACE" "&startDate=2017-12-31" "&endDate=2018-01-08" "&awardee=AZ_TUCSON"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 0,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 0,
                    "American_Indian_Alaska_Native": 1,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 0,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 0,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 0,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 1,
                    "American_Indian_Alaska_Native": 1,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 0,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 0,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )

        qs = (
            "&stratification=RACE"
            "&startDate=2017-12-31"
            "&endDate=2018-01-08"
            "&awardee=PITT"
            "&enrollmentStatus=MEMBER"
        )

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2018-01-03",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 1,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 2,
                    "American_Indian_Alaska_Native": 1,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 0,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 0,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-04",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 0,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 1,
                    "American_Indian_Alaska_Native": 1,
                    "No_Ancestry_Checked": 1,
                    "Black_African_American": 0,
                    "White": 0,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 0,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_race_api_v2(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        with FakeClock(TIME_2):
            calculate_participant_metrics()

        time.sleep(2)

        # test copy historical cache for stage two
        with FakeClock(TIME_3):
            calculate_participant_metrics()

        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 132)

        qs = "&stratification=RACE" "&startDate=2017-12-31" "&endDate=2018-01-08" "&version=2"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2017-12-31",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 0,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 1,
                    "American_Indian_Alaska_Native": 0,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 1,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 1,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 1,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 1,
                    "American_Indian_Alaska_Native": 1,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 1,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 1,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 1,
                    "Middle_Eastern_North_African": 1,
                    "Multi_Ancestry": 2,
                    "American_Indian_Alaska_Native": 3,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 1,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 1,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 1
                },
            },
            results,
        )

        qs = "&stratification=RACE" "&startDate=2017-12-31" "&endDate=2018-01-08" "&awardee=AZ_TUCSON" "&version=2"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2018-01-01",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 0,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 0,
                    "American_Indian_Alaska_Native": 1,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 0,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 0,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-02",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 0,
                    "Middle_Eastern_North_African": 1,
                    "Multi_Ancestry": 1,
                    "American_Indian_Alaska_Native": 2,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 0,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 0,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )

        qs = (
            "&stratification=RACE"
            "&startDate=2017-12-31"
            "&endDate=2018-01-08"
            "&awardee=PITT"
            "&enrollmentStatus=MEMBER"
            "&version=2"
        )

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {
                "date": "2018-01-03",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 1,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 2,
                    "American_Indian_Alaska_Native": 1,
                    "No_Ancestry_Checked": 0,
                    "Black_African_American": 0,
                    "White": 2,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 2,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )
        self.assertIn(
            {
                "date": "2018-01-04",
                "metrics": {
                    "None_Of_These_Fully_Describe_Me": 0,
                    "Middle_Eastern_North_African": 0,
                    "Multi_Ancestry": 1,
                    "American_Indian_Alaska_Native": 1,
                    "No_Ancestry_Checked": 1,
                    "Black_African_American": 0,
                    "White": 1,
                    "Prefer_Not_To_Answer": 0,
                    "Hispanic_Latino_Spanish": 1,
                    "Native_Hawaiian_other_Pacific_Islander": 0,
                    "Asian": 0,
                    "Unset_No_Basics": 0
                },
            },
            results,
        )

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_region_api(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        calculate_participant_metrics()

        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 72)

        qs1 = "&stratification=GEO_STATE" "&endDate=2017-12-31"

        results1 = self.send_get("PublicMetrics", query_string=qs1)

        qs2 = "&stratification=GEO_CENSUS" "&endDate=2018-01-01"

        results2 = self.send_get("PublicMetrics", query_string=qs2)

        qs3 = "&stratification=GEO_AWARDEE" "&endDate=2018-01-02"

        results3 = self.send_get("PublicMetrics", query_string=qs3)

        self.assertIn(
            {
                "date": "2017-12-31",
                "metrics": {
                    "WA": 0,
                    "DE": 0,
                    "DC": 0,
                    "WI": 0,
                    "WV": 0,
                    "HI": 0,
                    "FL": 0,
                    "WY": 0,
                    "NH": 0,
                    "NJ": 0,
                    "NM": 0,
                    "TX": 0,
                    "LA": 0,
                    "AK": 0,
                    "NC": 0,
                    "ND": 0,
                    "NE": 0,
                    "TN": 0,
                    "NY": 0,
                    "PA": 0,
                    "RI": 0,
                    "NV": 0,
                    "VA": 0,
                    "CO": 0,
                    "CA": 0,
                    "AL": 0,
                    "AR": 0,
                    "VT": 0,
                    "IL": 1,
                    "GA": 0,
                    "IN": 1,
                    "IA": 0,
                    "MA": 0,
                    "AZ": 0,
                    "ID": 0,
                    "CT": 0,
                    "ME": 0,
                    "MD": 0,
                    "OK": 0,
                    "OH": 0,
                    "UT": 0,
                    "MO": 0,
                    "MN": 0,
                    "MI": 0,
                    "KS": 0,
                    "MT": 0,
                    "MS": 0,
                    "SC": 0,
                    "KY": 0,
                    "OR": 0,
                    "SD": 0,
                    "AS": 0,
                    "FM": 0,
                    "GU": 0,
                    "MH": 0,
                    "MP": 0,
                    "PR": 1,
                    "PW": 0,
                    "VI": 0
                },
            },
            results1,
        )
        self.assertIn(
            {"date": "2018-01-01", "metrics": {"WEST": 0, "NORTHEAST": 0, "MIDWEST": 3, "SOUTH": 0, "TERRITORIES": 1}},
            results2
        )
        self.assertIn({"date": "2018-01-02", "count": 1, "hpo": "UNSET"}, results3)
        self.assertIn({"date": "2018-01-02", "count": 3, "hpo": "PITT"}, results3)
        self.assertIn({"date": "2018-01-02", "count": 2, "hpo": "AZ_TUCSON"}, results3)

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_lifecycle_api(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        with FakeClock(TIME_2):
            calculate_participant_metrics()

        time.sleep(2)

        # test copy historical cache for stage two
        with FakeClock(TIME_3):
            calculate_participant_metrics()

        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 132)

        qs1 = "&stratification=LIFECYCLE" "&endDate=2018-01-03"

        results1 = self.send_get("PublicMetrics", query_string=qs1)
        self.assertEqual(
            results1,
            [
                {
                    "date": "2018-01-03",
                    "metrics": {
                        "not_completed": {
                            "Full_Participant": 3,
                            "PPI_Module_The_Basics": 2,
                            "Consent_Complete": 1,
                            "Consent_Enrollment": 0,
                            "PPI_Module_Lifestyle": 2,
                            "Baseline_PPI_Modules_Complete": 2,
                            "PPI_Module_Family_Health": 2,
                            "PPI_Module_Overall_Health": 2,
                            "PPI_Module_Medications": 2,
                            "Physical_Measurements": 2,
                            "Registered": 0,
                            "PPI_Module_Medical_History": 2,
                            "PPI_Module_Healthcare_Access": 2,
                            "Samples_Received": 2,
                        },
                        "completed": {
                            "Full_Participant": 2,
                            "PPI_Module_The_Basics": 3,
                            "Consent_Complete": 4,
                            "Consent_Enrollment": 5,
                            "PPI_Module_Lifestyle": 3,
                            "Baseline_PPI_Modules_Complete": 3,
                            "PPI_Module_Family_Health": 3,
                            "PPI_Module_Overall_Health": 3,
                            "PPI_Module_Medications": 3,
                            "Physical_Measurements": 3,
                            "Registered": 5,
                            "PPI_Module_Medical_History": 3,
                            "PPI_Module_Healthcare_Access": 3,
                            "Samples_Received": 3,
                        },
                    },
                }
            ],
        )

        qs2 = "&stratification=LIFECYCLE" "&endDate=2018-01-08"

        results2 = self.send_get("PublicMetrics", query_string=qs2)
        self.assertEqual(
            results2,
            [
                {
                    "date": "2018-01-08",
                    "metrics": {
                        "not_completed": {
                            "Full_Participant": 0,
                            "PPI_Module_The_Basics": 0,
                            "Consent_Complete": 0,
                            "Consent_Enrollment": 0,
                            "PPI_Module_Lifestyle": 0,
                            "Baseline_PPI_Modules_Complete": 0,
                            "PPI_Module_Family_Health": 0,
                            "PPI_Module_Overall_Health": 0,
                            "PPI_Module_Medications": 0,
                            "Physical_Measurements": 0,
                            "Registered": 0,
                            "PPI_Module_Medical_History": 0,
                            "PPI_Module_Healthcare_Access": 0,
                            "Samples_Received": 0,
                        },
                        "completed": {
                            "Full_Participant": 5,
                            "PPI_Module_The_Basics": 5,
                            "Consent_Complete": 5,
                            "Consent_Enrollment": 5,
                            "PPI_Module_Lifestyle": 5,
                            "Baseline_PPI_Modules_Complete": 5,
                            "PPI_Module_Family_Health": 5,
                            "PPI_Module_Overall_Health": 5,
                            "PPI_Module_Medications": 5,
                            "Physical_Measurements": 5,
                            "Registered": 5,
                            "PPI_Module_Medical_History": 5,
                            "PPI_Module_Healthcare_Access": 5,
                            "Samples_Received": 5,
                        },
                    },
                }
            ],
        )

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_language_api(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        calculate_participant_metrics()
        qs = "&stratification=LANGUAGE" "&startDate=2017-12-30" "&endDate=2018-01-03"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 72)
        self.assertIn({"date": "2017-12-30", "metrics": {"EN": 0, "UNSET": 0, "ES": 0}}, results)
        self.assertIn({"date": "2017-12-31", "metrics": {"EN": 1, "UNSET": 2, "ES": 0}}, results)
        self.assertIn({"date": "2018-01-03", "metrics": {"EN": 1, "UNSET": 2, "ES": 1}}, results)

    @mock.patch('rdr_service.dao.participant_counts_over_time_service.ParticipantCountsOverTimeService.JOB_TIME',
                test_job_time)
    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_primary_consent_api(self, big_query):
        big_query().query.side_effect = self.get_mock_data()

        with FakeClock(TIME_2):
            calculate_participant_metrics()

        time.sleep(2)

        # test copy historical cache for stage two
        with FakeClock(TIME_3):
            calculate_participant_metrics()

        qs = "&stratification=PRIMARY_CONSENT" "&startDate=2017-12-31" "&endDate=2018-01-08"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertTrue(big_query().query.called)
        self.assertEqual(big_query().query.call_count, 132)
        self.assertIn({"date": "2017-12-31", "metrics": {"Primary_Consent": 1}}, results)
        self.assertIn({"date": "2018-01-02", "metrics": {"Primary_Consent": 2}}, results)
        self.assertIn({"date": "2018-01-06", "metrics": {"Primary_Consent": 5}}, results)

    @mock.patch('google.cloud.bigquery.Client')
    def test_public_metrics_get_ehr_consent_api(self, big_query):

        p1 = Participant(participantId=1, biobankId=4)
        self._insert(
            p1,
            "Alice",
            "Aardvark",
            "UNSET",
            time_int=self.time1,
            time_study=self.time1,
            time_mem=self.time1,
            time_fp=self.time1,
            time_fp_stored=self.time1,
        )

        p2 = Participant(participantId=2, biobankId=5)
        self._insert(
            p2,
            "Bob",
            "Builder",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time2,
            time_study=self.time2,
            time_mem=self.time2,
            time_fp=self.time3,
            time_fp_stored=self.time3,
        )

        p3 = Participant(participantId=3, biobankId=6)
        self._insert(
            p3,
            "Chad",
            "Caterpillar",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time3,
            time_study=self.time4,
            time_mem=self.time4,
            time_fp=self.time5,
            time_fp_stored=self.time5,
        )

        p4 = Participant(participantId=4, biobankId=7)
        self._insert(
            p4,
            "Chad2",
            "Caterpillar2",
            "PITT",
            "PITT_BANNER_HEALTH",
            time_int=self.time3,
            time_study=self.time4,
            time_mem=self.time5,
            time_fp=self.time5,
            time_fp_stored=self.time5,
        )

        p4 = Participant(participantId=6, biobankId=9)
        self._insert(
            p4,
            "Chad3",
            "Caterpillar3",
            "PITT",
            "PITT_BANNER_HEALTH",
            time_int=self.time3,
            time_study=self.time4,
            time_mem=self.time4,
            time_fp=self.time4,
            time_fp_stored=self.time5,
        )

        # ghost participant should be filtered out
        p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
        self._insert(
            p_ghost,
            "Ghost",
            "G",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time1,
            time_study=self.time1,
            time_mem=self.time1,
            time_fp=self.time1,
            time_fp_stored=self.time1,
        )

        calculate_participant_metrics()

        qs = "&stratification=EHR_METRICS" "&startDate=2017-12-31" "&endDate=2018-01-08"

        self.assertTrue(big_query().query.called)
        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {"date": "2017-12-31", "metrics": {"ORGANIZATIONS_ACTIVE": 0, "EHR_RECEIVED": 0, "EHR_CONSENTED": 1}},
            results,
        )
        self.assertIn(
            {"date": "2018-01-02", "metrics": {"ORGANIZATIONS_ACTIVE": 0, "EHR_RECEIVED": 0, "EHR_CONSENTED": 2}},
            results,
        )
        self.assertIn(
            {"date": "2018-01-03", "metrics": {"ORGANIZATIONS_ACTIVE": 0, "EHR_RECEIVED": 0, "EHR_CONSENTED": 4}},
            results,
        )
        self.assertIn(
            {"date": "2018-01-06", "metrics": {"ORGANIZATIONS_ACTIVE": 0, "EHR_RECEIVED": 0, "EHR_CONSENTED": 5}},
            results,
        )

        qs = "&stratification=EHR_METRICS" "&startDate=2017-12-31" "&endDate=2018-01-08" "&awardee=AZ_TUCSON,PITT"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn(
            {"date": "2017-12-31", "metrics": {"ORGANIZATIONS_ACTIVE": 0, "EHR_RECEIVED": 0, "EHR_CONSENTED": 0}},
            results,
        )
        self.assertIn(
            {"date": "2018-01-02", "metrics": {"ORGANIZATIONS_ACTIVE": 0, "EHR_RECEIVED": 0, "EHR_CONSENTED": 1}},
            results,
        )
        self.assertIn(
            {"date": "2018-01-03", "metrics": {"ORGANIZATIONS_ACTIVE": 0, "EHR_RECEIVED": 0, "EHR_CONSENTED": 3}},
            results,
        )
        self.assertIn(
            {"date": "2018-01-06", "metrics": {"ORGANIZATIONS_ACTIVE": 0, "EHR_RECEIVED": 0, "EHR_CONSENTED": 4}},
            results,
        )

    def test_public_metrics_get_sites_count_api(self):
        site = Site(siteName='site', googleGroup='site@googlegroups.com',
                    mayolinkClientNumber=12345, hpoId=PITT_HPO_ID, siteStatus=1, enrollingStatus=1)

        site2 = Site(siteName='site2', googleGroup='site2@googlegroups.com',
                     mayolinkClientNumber=12346, hpoId=PITT_HPO_ID, siteStatus=1, enrollingStatus=1)

        site_dao = SiteDao()
        site_dao.insert(site)
        site_dao.insert(site2)

        qs = '&stratification=SITES_COUNT'
        results = self.send_get('PublicMetrics', query_string=qs)
        self.assertEqual(results, {'sites_count': 2})

    def create_demographics_questionnaire(self):
        """Uses the demographics test data questionnaire.  Returns the questionnaire id"""
        return self.create_questionnaire("questionnaire3.json")

    def post_demographics_questionnaire(
        self, participant_id, questionnaire_id, cabor_signature_string=False, test_time=TIME_1, **kwargs
    ):
        """POSTs answers to the demographics questionnaire for the participant"""
        answers = {
            "code_answers": [],
            "string_answers": [],
            "date_answers": [("dateOfBirth", kwargs.get("dateOfBirth"))],
        }
        if cabor_signature_string:
            answers["string_answers"].append(("CABoRSignature", kwargs.get("CABoRSignature")))
        else:
            answers["uri_answers"] = [("CABoRSignature", kwargs.get("CABoRSignature"))]

        for link_id in self.code_link_ids:
            if link_id in kwargs:
                if link_id == "race":
                    for race_code in kwargs[link_id]:
                        concept = Concept(PPI_SYSTEM, race_code)
                        answers["code_answers"].append((link_id, concept))
                else:
                    concept = Concept(PPI_SYSTEM, kwargs[link_id])
                    answers["code_answers"].append((link_id, concept))

        for link_id in self.string_link_ids:
            code = kwargs.get(link_id)
            answers["string_answers"].append((link_id, code))

        response_data = self.make_questionnaire_response_json(participant_id, questionnaire_id, **answers)

        with FakeClock(test_time):
            url = "Participant/%s/QuestionnaireResponse" % participant_id
            return self.send_post(url, request_data=response_data)

    def init_gender_codes(self):
        code1 = Code(
            codeId=1,
            system=PPI_SYSTEM,
            value="GenderIdentity_Woman",
            display="GenderIdentity_Woman",
            topic="a",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        self.code_dao.insert(code1)
        code2 = Code(
            codeId=2,
            system=PPI_SYSTEM,
            value="GenderIdentity_Transgender",
            display="GenderIdentity_Transgender",
            topic="a",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        self.code_dao.insert(code2)
        code3 = Code(
            codeId=3,
            system=PPI_SYSTEM,
            value="GenderIdentity_Man",
            display="GenderIdentity_Man",
            topic="a",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        self.code_dao.insert(code3)
        code4 = Code(
            codeId=4,
            system=PPI_SYSTEM,
            value="GenderIdentity_AdditionalOptions",
            display="GenderIdentity_AdditionalOptions",
            topic="a",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        self.code_dao.insert(code4)
        code5 = Code(
            codeId=5,
            system=PPI_SYSTEM,
            value="GenderIdentity_NonBinary",
            display="GenderIdentity_NonBinary",
            topic="a",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        self.code_dao.insert(code5)
        code6 = Code(
            codeId=6,
            system=PPI_SYSTEM,
            value="PMI_PreferNotToAnswer",
            display="PMI_PreferNotToAnswer",
            topic="a",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        self.code_dao.insert(code6)
        code7 = Code(
            codeId=7,
            system=PPI_SYSTEM,
            value="PMI_Skip",
            display="PMI_Skip",
            topic="a",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        self.code_dao.insert(code7)
