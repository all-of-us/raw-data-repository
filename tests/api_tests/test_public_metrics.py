import datetime

from rdr_service.clock import FakeClock
from rdr_service.code_constants import (
    PMI_SKIP_CODE,
    PPI_SYSTEM,
    RACE_AIAN_CODE,
    RACE_HISPANIC_CODE,
    RACE_MENA_CODE,
    RACE_NONE_OF_THESE_CODE,
    RACE_WHITE_CODE,
)
from rdr_service.concepts import Concept
from rdr_service.dao.calendar_dao import CalendarDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.offline.participant_counts_over_time import calculate_participant_metrics
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.participant_summary_dao import ParticipantGenderAnswersDao, ParticipantSummaryDao
from rdr_service.model.calendar import Calendar
from rdr_service.model.code import Code, CodeType
from rdr_service.model.hpo import HPO
from rdr_service.model.site import Site
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantGenderAnswers, ParticipantSummary
from rdr_service.participant_enums import (
    EnrollmentStatus,
    OrganizationType,
    TEST_HPO_ID,
    TEST_HPO_NAME,
    make_primary_provider_link_for_name,
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

        self.clear_table_after_test('metrics_enrollment_status_cache')
        self.clear_table_after_test('metrics_gender_cache')
        self.clear_table_after_test('metrics_age_cache')
        self.clear_table_after_test('metrics_race_cache')
        self.clear_table_after_test('metrics_region_cache')
        self.clear_table_after_test('metrics_lifecycle_cache')
        self.clear_table_after_test('metrics_language_cache')

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

        if enrollment_status is None:
            return None

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

    def test_public_metrics_get_enrollment_status_api(self):

        p1 = Participant(participantId=1, biobankId=4)
        self._insert(p1, "Alice", "Aardvark", "UNSET", unconsented=True, time_int=self.time1, time_study=self.time1)

        p2 = Participant(participantId=2, biobankId=5)
        self._insert(
            p2, "Bob", "Builder", "AZ_TUCSON", "AZ_TUCSON_BANNER_HEALTH", time_int=self.time2, time_study=self.time2
        )

        p3 = Participant(participantId=3, biobankId=6)
        self._insert(
            p3,
            "Chad",
            "Caterpillar",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time1,
            time_study=self.time1,
            time_mem=self.time3,
            time_fp_stored=self.time4,
        )

        calculate_participant_metrics()

        qs = "&stratification=ENROLLMENT_STATUS" "&startDate=2018-01-01" "&endDate=2018-01-08"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn({"date": "2018-01-01", "metrics": {"consented": 0, "core": 0, "registered": 3}}, results)
        self.assertIn({"date": "2018-01-02", "metrics": {"consented": 1, "core": 0, "registered": 2}}, results)
        self.assertIn({"date": "2018-01-03", "metrics": {"consented": 0, "core": 1, "registered": 2}}, results)

        qs = "&stratification=ENROLLMENT_STATUS" "&startDate=2018-01-01" "&endDate=2018-01-08" "&awardee=AZ_TUCSON"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn({"date": "2018-01-01", "metrics": {"consented": 0, "core": 0, "registered": 2}}, results)
        self.assertIn({"date": "2018-01-02", "metrics": {"consented": 1, "core": 0, "registered": 1}}, results)
        self.assertIn({"date": "2018-01-03", "metrics": {"consented": 0, "core": 1, "registered": 1}}, results)

    def test_public_metrics_get_gender_api(self):

        self.init_gender_codes()
        gender_code_dict = {
            "GenderIdentity_Woman": 1,
            "GenderIdentity_Transgender": 2,
            "GenderIdentity_Man": 3,
            "GenderIdentity_AdditionalOptions": 4,
            "GenderIdentity_NonBinary": 5,
            "PMI_PreferNotToAnswer": 6,
            "PMI_Skip": 7,
        }

        participant_gender_answer_dao = ParticipantGenderAnswersDao()
        p1 = Participant(participantId=1, biobankId=4)
        self._insert(p1, "Alice", "Aardvark", "UNSET", time_int=self.time1, gender_identity=3)
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=1,
                    created=self.time1,
                    modified=self.time1,
                    codeId=gender_code_dict["GenderIdentity_Woman"],
                )
            )
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
            time_mem=self.time3,
            gender_identity=2
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=2,
                    created=self.time2,
                    modified=self.time2,
                    codeId=gender_code_dict["GenderIdentity_Man"],
                )
            )
        )

        p3 = Participant(participantId=3, biobankId=6)
        self._insert(
            p3,
            "Chad",
            "Caterpillar",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time3,
            time_study=self.time3,
            time_mem=self.time5,
            gender_identity=5
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=3,
                    created=self.time3,
                    modified=self.time3,
                    codeId=gender_code_dict["GenderIdentity_Transgender"],
                )
            )
        )

        p4 = Participant(participantId=4, biobankId=7)
        self._insert(
            p4,
            "Chad2",
            "Caterpillar2",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time4,
            time_study=self.time4,
            time_mem=self.time5,
            gender_identity=5
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=4,
                    created=self.time4,
                    modified=self.time4,
                    codeId=gender_code_dict["GenderIdentity_Transgender"],
                )
            )
        )

        p6 = Participant(participantId=6, biobankId=9)
        self._insert(
            p6,
            "Chad3",
            "Caterpillar3",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time5,
            time_study=self.time5,
            time_mem=self.time5,
            gender_identity=7
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=6,
                    created=self.time5,
                    modified=self.time5,
                    codeId=gender_code_dict["GenderIdentity_Woman"],
                )
            )
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=6,
                    created=self.time5,
                    modified=self.time5,
                    codeId=gender_code_dict["GenderIdentity_Man"],
                )
            )
        )

        # ghost participant should be filtered out
        p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
        self._insert(
            p_ghost, "Ghost", "G", "AZ_TUCSON", "AZ_TUCSON_BANNER_HEALTH", time_int=self.time1, gender_identity=5
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=5,
                    created=self.time1,
                    modified=self.time1,
                    codeId=gender_code_dict["GenderIdentity_Transgender"],
                )
            )
        )

        calculate_participant_metrics()

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

    def test_public_metrics_get_gender_api_v2(self):

        self.init_gender_codes()
        gender_code_dict = {
            "GenderIdentity_Woman": 1,
            "GenderIdentity_Transgender": 2,
            "GenderIdentity_Man": 3,
            "GenderIdentity_AdditionalOptions": 4,
            "GenderIdentity_NonBinary": 5,
            "PMI_PreferNotToAnswer": 6,
            "PMI_Skip": 7,
        }

        participant_gender_answer_dao = ParticipantGenderAnswersDao()
        p1 = Participant(participantId=1, biobankId=4)
        self._insert(p1, "Alice", "Aardvark", "UNSET", time_int=self.time1, gender_identity=3)
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=1,
                    created=self.time1,
                    modified=self.time1,
                    codeId=gender_code_dict["GenderIdentity_Woman"],
                )
            )
        )

        p2 = Participant(participantId=2, biobankId=5)
        self._insert(
            p2,
            "Bob",
            "Builder",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time2,
            time_mem=self.time3,
            gender_identity=2,
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=2,
                    created=self.time2,
                    modified=self.time2,
                    codeId=gender_code_dict["GenderIdentity_Man"],
                )
            )
        )

        p3 = Participant(participantId=3, biobankId=6)
        self._insert(
            p3,
            "Chad",
            "Caterpillar",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time3,
            time_mem=self.time5,
            gender_identity=5,
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=3,
                    created=self.time3,
                    modified=self.time3,
                    codeId=gender_code_dict["GenderIdentity_Transgender"],
                )
            )
        )

        p4 = Participant(participantId=4, biobankId=7)
        self._insert(
            p4,
            "Chad2",
            "Caterpillar2",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time4,
            time_mem=self.time5,
            gender_identity=5,
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=4,
                    created=self.time4,
                    modified=self.time4,
                    codeId=gender_code_dict["GenderIdentity_Transgender"],
                )
            )
        )
        p6 = Participant(participantId=6, biobankId=9)
        self._insert(
            p6,
            "Chad3",
            "Caterpillar3",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time5,
            time_mem=self.time5,
            gender_identity=7,
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=6,
                    created=self.time5,
                    modified=self.time5,
                    codeId=gender_code_dict["GenderIdentity_Woman"],
                )
            )
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=6,
                    created=self.time5,
                    modified=self.time5,
                    codeId=gender_code_dict["GenderIdentity_Man"],
                )
            )
        )

        # ghost participant should be filtered out
        p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
        self._insert(
            p_ghost, "Ghost", "G", "AZ_TUCSON", "AZ_TUCSON_BANNER_HEALTH", time_int=self.time1, gender_identity=5
        )
        participant_gender_answer_dao.insert(
            ParticipantGenderAnswers(
                **dict(
                    participantId=5,
                    created=self.time1,
                    modified=self.time1,
                    codeId=gender_code_dict["GenderIdentity_Transgender"],
                )
            )
        )

        with FakeClock(TIME_2):
            calculate_participant_metrics()

        # test copy historical cache for stage two
        with FakeClock(TIME_3):
            calculate_participant_metrics()

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

    def test_public_metrics_get_age_range_api(self):
        dob1 = datetime.date(1978, 10, 10)
        dob2 = datetime.date(1988, 10, 10)
        dob3 = datetime.date(1988, 10, 10)
        dob4 = datetime.date(1998, 10, 10)
        p1 = Participant(participantId=1, biobankId=4)
        self._insert(p1, "Alice", "Aardvark", "UNSET", time_int=self.time1, dob=dob1)

        p2 = Participant(participantId=2, biobankId=5)
        self._insert(
            p2,
            "Bob",
            "Builder",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time2,
            time_study=self.time2,
            time_mem=self.time3,
            dob=dob2
        )

        p3 = Participant(participantId=3, biobankId=6)
        self._insert(
            p3,
            "Chad",
            "Caterpillar",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_study=self.time3,
            time_int=self.time3,
            time_mem=self.time5,
            dob=dob3
        )

        p4 = Participant(participantId=4, biobankId=7)
        self._insert(
            p4,
            "Chad2",
            "Caterpillar2",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_study=self.time4,
            time_int=self.time4,
            time_mem=self.time5,
            dob=dob4
        )

        # ghost participant should be filtered out
        p_ghost = Participant(participantId=5, biobankId=8, isGhostId=True)
        self._insert(p_ghost, "Ghost", "G", "AZ_TUCSON", "AZ_TUCSON_BANNER_HEALTH", time_int=self.time1, dob=dob3)

        with FakeClock(TIME_2):
            calculate_participant_metrics()

        # test copy historical cache for stage two
        with FakeClock(TIME_3):
            calculate_participant_metrics()

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

    def test_public_metrics_get_total_api(self):

        p1 = Participant(participantId=1, biobankId=4)
        self._insert(p1, "Alice", "Aardvark", "UNSET", unconsented=True, time_int=self.time1, time_study=self.time1)

        p2 = Participant(participantId=2, biobankId=5)
        self._insert(
            p2, "Bob", "Builder", "AZ_TUCSON", "AZ_TUCSON_BANNER_HEALTH", time_int=self.time2, time_study=self.time2
        )

        p3 = Participant(participantId=3, biobankId=6)
        self._insert(
            p3,
            "Chad",
            "Caterpillar",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time3,
            time_study=self.time3,
            time_mem=self.time4,
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
            time_mem=self.time4,
            time_fp_stored=self.time5,
        )

        calculate_participant_metrics()

        qs = "&stratification=TOTAL" "&startDate=2018-01-01" "&endDate=2018-01-08"

        response = self.send_get("PublicMetrics", query_string=qs)

        self.assertIn({"date": "2018-01-01", "metrics": {"TOTAL": 2}}, response)
        self.assertIn({"date": "2018-01-02", "metrics": {"TOTAL": 3}}, response)
        self.assertIn({"date": "2018-01-07", "metrics": {"TOTAL": 3}}, response)
        self.assertIn({"date": "2018-01-08", "metrics": {"TOTAL": 3}}, response)

        qs = "&stratification=TOTAL" "&startDate=2018-01-01" "&endDate=2018-01-08" "&awardee=AZ_TUCSON"

        response = self.send_get("PublicMetrics", query_string=qs)

        self.assertIn({"date": "2018-01-01", "metrics": {"TOTAL": 1}}, response)
        self.assertIn({"date": "2018-01-02", "metrics": {"TOTAL": 2}}, response)
        self.assertIn({"date": "2018-01-07", "metrics": {"TOTAL": 2}}, response)
        self.assertIn({"date": "2018-01-08", "metrics": {"TOTAL": 2}}, response)

    def test_public_metrics_get_race_api(self):

        questionnaire_id = self.create_demographics_questionnaire()

        def setup_participant(when, race_code_list, providerLink=self.provider_link, no_demographic=False):
            # Set up participant, questionnaire, and consent
            with FakeClock(when):
                participant = self.send_post("Participant", {"providerLink": [providerLink]})
                participant_id = participant["participantId"]
                self.send_consent(participant_id)
                if no_demographic:
                    return participant
                # Populate some answers to the questionnaire
                answers = {
                    "race": race_code_list,
                    "genderIdentity": PMI_SKIP_CODE,
                    "firstName": self.fake.first_name(),
                    "middleName": self.fake.first_name(),
                    "lastName": self.fake.last_name(),
                    "zipCode": "78751",
                    "state": PMI_SKIP_CODE,
                    "streetAddress": "1234 Main Street",
                    "city": "Austin",
                    "sex": PMI_SKIP_CODE,
                    "sexualOrientation": PMI_SKIP_CODE,
                    "phoneNumber": "512-555-5555",
                    "recontactMethod": PMI_SKIP_CODE,
                    "language": PMI_SKIP_CODE,
                    "education": PMI_SKIP_CODE,
                    "income": PMI_SKIP_CODE,
                    "dateOfBirth": datetime.date(1978, 10, 9),
                    "CABoRSignature": "signature.pdf",
                }
            self.post_demographics_questionnaire(participant_id, questionnaire_id, time=when, **answers)
            return participant

        p1 = setup_participant(self.time1, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
        self.update_participant_summary(p1["participantId"][1:], time_mem=self.time2)
        p2 = setup_participant(self.time2, [RACE_NONE_OF_THESE_CODE], self.provider_link)
        self.update_participant_summary(p2["participantId"][1:], time_mem=self.time3, time_fp_stored=self.time5)
        p3 = setup_participant(self.time3, [RACE_AIAN_CODE], self.provider_link)
        setup_participant(self.time3, [PMI_SKIP_CODE], self.provider_link, no_demographic=True)
        self.update_participant_summary(p3["participantId"][1:], time_mem=self.time4)
        p4 = setup_participant(self.time4, [PMI_SKIP_CODE], self.provider_link)
        self.update_participant_summary(p4["participantId"][1:], time_mem=self.time5)
        p5 = setup_participant(self.time4, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
        self.update_participant_summary(p5["participantId"][1:], time_mem=self.time4, time_fp_stored=self.time5)
        setup_participant(self.time2, [RACE_AIAN_CODE], self.az_provider_link)
        setup_participant(self.time3, [RACE_AIAN_CODE, RACE_MENA_CODE], self.az_provider_link)

        calculate_participant_metrics()

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

    def test_public_metrics_get_race_api_v2(self):

        questionnaire_id = self.create_demographics_questionnaire()

        def setup_participant(when, race_code_list, providerLink=self.provider_link, no_demographic=False):
            # Set up participant, questionnaire, and consent
            with FakeClock(when):
                participant = self.send_post("Participant", {"providerLink": [providerLink]})
                participant_id = participant["participantId"]
                self.send_consent(participant_id)
                if no_demographic:
                    return participant
                # Populate some answers to the questionnaire
                answers = {
                    "race": race_code_list,
                    "genderIdentity": PMI_SKIP_CODE,
                    "firstName": self.fake.first_name(),
                    "middleName": self.fake.first_name(),
                    "lastName": self.fake.last_name(),
                    "zipCode": "78751",
                    "state": PMI_SKIP_CODE,
                    "streetAddress": "1234 Main Street",
                    "city": "Austin",
                    "sex": PMI_SKIP_CODE,
                    "sexualOrientation": PMI_SKIP_CODE,
                    "phoneNumber": "512-555-5555",
                    "recontactMethod": PMI_SKIP_CODE,
                    "language": PMI_SKIP_CODE,
                    "education": PMI_SKIP_CODE,
                    "income": PMI_SKIP_CODE,
                    "dateOfBirth": datetime.date(1978, 10, 9),
                    "CABoRSignature": "signature.pdf",
                }
            self.post_demographics_questionnaire(participant_id, questionnaire_id, time=when, **answers)
            return participant

        p1 = setup_participant(self.time1, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
        self.update_participant_summary(p1["participantId"][1:], time_mem=self.time2)
        p2 = setup_participant(self.time2, [RACE_NONE_OF_THESE_CODE], self.provider_link)
        self.update_participant_summary(p2["participantId"][1:], time_mem=self.time3, time_fp_stored=self.time5)
        p3 = setup_participant(self.time3, [RACE_AIAN_CODE], self.provider_link)
        self.update_participant_summary(p3["participantId"][1:], time_mem=self.time4)
        # Setup participant with no demographic questionnaire.
        setup_participant(self.time3, [PMI_SKIP_CODE], self.provider_link, no_demographic=True)

        p4 = setup_participant(self.time4, [PMI_SKIP_CODE], self.provider_link)
        self.update_participant_summary(p4["participantId"][1:], time_mem=self.time5)
        p5 = setup_participant(self.time4, [RACE_WHITE_CODE, RACE_HISPANIC_CODE], self.provider_link)
        self.update_participant_summary(p5["participantId"][1:], time_mem=self.time4, time_fp_stored=self.time5)
        setup_participant(self.time2, [RACE_AIAN_CODE], self.az_provider_link)
        setup_participant(self.time3, [RACE_AIAN_CODE, RACE_MENA_CODE], self.az_provider_link)

        with FakeClock(TIME_2):
            calculate_participant_metrics()

        # test copy historical cache for stage two
        with FakeClock(TIME_3):
            calculate_participant_metrics()

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

    def test_public_metrics_get_region_api(self):

        code1 = Code(
            codeId=1,
            system="a",
            value="PIIState_IL",
            display="PIIState_IL",
            topic="a",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        code2 = Code(
            codeId=2,
            system="b",
            value="PIIState_IN",
            display="PIIState_IN",
            topic="b",
            codeType=CodeType.MODULE,
            mapped=True,
        )
        code3 = Code(
            codeId=3,
            system="c",
            value="PIIState_CA",
            display="PIIState_CA",
            topic="c",
            codeType=CodeType.MODULE,
            mapped=True,
        )

        code4 = Code(
            codeId=4,
            system="c",
            value="PIIState_PR",
            display="PIIState_PR",
            topic="c",
            codeType=CodeType.MODULE,
            mapped=True,
        )

        self.code_dao.insert(code1)
        self.code_dao.insert(code2)
        self.code_dao.insert(code3)
        self.code_dao.insert(code4)

        p1 = Participant(participantId=1, biobankId=4)
        self._insert(
            p1,
            "Alice",
            "Aardvark",
            "UNSET",
            time_int=self.time1,
            time_study=self.time1,
            time_mem=self.time1,
            time_fp_stored=self.time1,
            state_id=1,
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
            time_fp_stored=self.time2,
            state_id=2,
        )

        p3 = Participant(participantId=3, biobankId=6)
        self._insert(
            p3,
            "Chad",
            "Caterpillar",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time3,
            time_study=self.time3,
            time_mem=self.time3,
            time_fp_stored=self.time3,
            state_id=3,
        )

        p4 = Participant(participantId=4, biobankId=7)
        self._insert(
            p4,
            "Chad2",
            "Caterpillar2",
            "PITT",
            "PITT_BANNER_HEALTH",
            time_int=self.time3,
            time_study=self.time3,
            time_mem=self.time3,
            time_fp_stored=self.time3,
            state_id=2,
        )

        p5 = Participant(participantId=6, biobankId=9)
        self._insert(
            p5,
            "Chad3",
            "Caterpillar3",
            "PITT",
            "PITT_BANNER_HEALTH",
            time_int=self.time1,
            time_study=self.time1,
            time_mem=self.time2,
            time_fp_stored=self.time3,
            state_id=2,
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
            time_fp_stored=self.time1,
            state_id=1,
        )

        p6 = Participant(participantId=7, biobankId=10)
        self._insert(
            p6,
            "Angela",
            "Alligator",
            "PITT",
            "PITT_BANNER_HEALTH",
            time_int=self.time1,
            time_study=self.time1,
            time_mem=self.time2,
            time_fp_stored=self.time3,
            state_id=4,
        )

        calculate_participant_metrics()

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

    def test_public_metrics_get_lifecycle_api(self):

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

        with FakeClock(TIME_2):
            calculate_participant_metrics()

        # test copy historical cache for stage two
        with FakeClock(TIME_3):
            calculate_participant_metrics()

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

    def test_public_metrics_get_language_api(self):

        p1 = Participant(participantId=1, biobankId=4)
        self._insert(p1, "Alice", "Aardvark", "UNSET", unconsented=True, time_int=self.time1, primary_language="en")

        p2 = Participant(participantId=2, biobankId=5)
        self._insert(
            p2, "Bob", "Builder", "AZ_TUCSON", "AZ_TUCSON_BANNER_HEALTH", time_int=self.time2, primary_language="es"
        )

        p3 = Participant(participantId=3, biobankId=6)
        self._insert(
            p3,
            "Chad",
            "Caterpillar",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time1,
            time_mem=self.time3,
            time_fp_stored=self.time4,
            primary_language="en",
        )

        p4 = Participant(participantId=5, biobankId=7)
        self._insert(
            p4,
            "Chad2",
            "Caterpillar2",
            "AZ_TUCSON",
            "AZ_TUCSON_BANNER_HEALTH",
            time_int=self.time1,
            time_mem=self.time2,
            time_fp_stored=self.time4,
        )

        calculate_participant_metrics()
        qs = "&stratification=LANGUAGE" "&startDate=2017-12-30" "&endDate=2018-01-03"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn({"date": "2017-12-30", "metrics": {"EN": 0, "UNSET": 0, "ES": 0}}, results)
        self.assertIn({"date": "2017-12-31", "metrics": {"EN": 1, "UNSET": 2, "ES": 0}}, results)
        self.assertIn({"date": "2018-01-03", "metrics": {"EN": 1, "UNSET": 2, "ES": 1}}, results)

    def test_public_metrics_get_primary_consent_api(self):

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

        with FakeClock(TIME_2):
            calculate_participant_metrics()

        # test copy historical cache for stage two
        with FakeClock(TIME_3):
            calculate_participant_metrics()

        qs = "&stratification=PRIMARY_CONSENT" "&startDate=2017-12-31" "&endDate=2018-01-08"

        results = self.send_get("PublicMetrics", query_string=qs)
        self.assertIn({"date": "2017-12-31", "metrics": {"Primary_Consent": 1}}, results)
        self.assertIn({"date": "2018-01-02", "metrics": {"Primary_Consent": 2}}, results)
        self.assertIn({"date": "2018-01-06", "metrics": {"Primary_Consent": 5}}, results)

    def test_public_metrics_get_ehr_consent_api(self):

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
        self, participant_id, questionnaire_id, cabor_signature_string=False, time=TIME_1, **kwargs
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

        with FakeClock(time):
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
