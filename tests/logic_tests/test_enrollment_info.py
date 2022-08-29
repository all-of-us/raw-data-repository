from datetime import datetime
from typing import List

from rdr_service.services.system_utils import DateRange


from rdr_service.logic.enrollment_info import (
    EnrollmentCalculation,
    EnrollmentDependencies,
    EnrollmentInfo
)
from rdr_service.participant_enums import (
    EnrollmentStatus,
    EnrollmentStatusV30,
    EnrollmentStatusV31,
    ParticipantCohort
)

from tests.helpers.unittest_base import BaseTestCase


class TestEnrollmentInfo(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(TestEnrollmentInfo, self).__init__(*args, **kwargs)
        self.uses_database = False

    def test_default_state(self):
        """
        Check all versions of the enrollment status calculation for the status given for a participant that
        has only provided primary consent.
        """
        participant_info = self._build_participant_info(
            primary_authored_time=datetime(2022, 1, 9)
        )
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.INTERESTED,
                version_3_0_status=EnrollmentStatusV30.PARTICIPANT,
                version_3_1_status=EnrollmentStatusV31.PARTICIPANT,
                version_legacy_datetime=participant_info.primary_consent_authored_time,
                version_3_0_datetime=participant_info.primary_consent_authored_time,
                version_3_1_datetime=participant_info.primary_consent_authored_time
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

    def test_plus_ehr_status(self):
        """
        Each version of the calculation should upgrade when EHR is provided
        """
        participant_info = self._build_participant_info(
            primary_authored_time=datetime(2019, 8, 1),
            ehr_consent_ranges=[DateRange(start=datetime(2019, 8, 3))]
        )
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.MEMBER,
                version_3_0_status=EnrollmentStatusV30.PARTICIPANT_PLUS_EHR,
                version_3_1_status=EnrollmentStatusV31.PARTICIPANT_PLUS_EHR,
                version_legacy_datetime=participant_info.first_ehr_consent_date,
                version_3_0_datetime=participant_info.first_ehr_consent_date,
                version_3_1_datetime=participant_info.first_ehr_consent_date
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

    def test_basics_and_gror(self):
        """
        3.0 should upgrade to PARTICIPANT_PMB_ELIGIBLE when TheBasics has been submitted.
        3.1 needs TheBasics, but shouldn't upgrade until GROR has been submitted as well.
        The legacy version of the calculation would still just show them as MEMBER.
        """
        participant_info = self._build_participant_info(
            primary_authored_time=datetime(2020, 7, 18),
            ehr_consent_ranges=[DateRange(start=datetime(2020, 7, 18))],
            basics_time=datetime(2020, 7, 27)
        )
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.MEMBER,
                version_3_0_status=EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE,
                version_3_1_status=EnrollmentStatusV31.PARTICIPANT_PLUS_EHR,
                version_legacy_datetime=participant_info.primary_consent_authored_time,
                version_3_0_datetime=participant_info.basics_authored_time,
                version_3_1_datetime=participant_info.first_ehr_consent_date
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

        participant_info.gror_authored_time = datetime(2020, 8, 2)
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.MEMBER,
                version_3_0_status=EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE,
                version_3_1_status=EnrollmentStatusV31.PARTICIPANT_PLUS_BASICS,
                version_legacy_datetime=participant_info.primary_consent_authored_time,
                version_3_0_datetime=participant_info.basics_authored_time,
                version_3_1_datetime=participant_info.gror_authored_time
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

    def test_core_minus_pm(self):
        """
        Check that all versions upgrade to CORE_MINUS_PM when requirements are met.
        """
        participant_info = self._build_participant_info(
            consent_cohort=ParticipantCohort.COHORT_2,
            primary_authored_time=datetime(2022, 3, 4),
            ehr_consent_ranges=[DateRange(start=datetime(2022, 3, 4))],
            basics_time=datetime(2022, 3, 7),
            overall_health_time=datetime(2022, 3, 9),
            lifestyle_time=datetime(2022, 3, 9),
            biobank_received_dna_sample_time=datetime(2022, 3, 12)
        )
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.CORE_MINUS_PM,
                version_3_0_status=EnrollmentStatusV30.CORE_MINUS_PM,
                version_3_1_status=EnrollmentStatusV31.CORE_MINUS_PM,
                version_legacy_datetime=participant_info.earliest_biobank_received_dna_time,
                version_3_0_datetime=participant_info.earliest_biobank_received_dna_time,
                version_3_1_datetime=participant_info.earliest_biobank_received_dna_time
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

        # Check that GROR is needed for cohort 3 participants
        participant_info.consent_cohort = ParticipantCohort.COHORT_3
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.MEMBER,
                version_3_0_status=EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE,
                version_3_1_status=EnrollmentStatusV31.PARTICIPANT_PLUS_EHR,
                version_legacy_datetime=participant_info.primary_consent_authored_time,
                version_3_0_datetime=participant_info.basics_authored_time,
                version_3_1_datetime=participant_info.first_ehr_consent_date
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

        participant_info.gror_authored_time = datetime(2022, 4, 2)
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.CORE_MINUS_PM,
                version_3_0_status=EnrollmentStatusV30.CORE_MINUS_PM,
                version_3_1_status=EnrollmentStatusV31.CORE_MINUS_PM,
                version_legacy_datetime=participant_info.gror_authored_time,
                version_3_0_datetime=participant_info.gror_authored_time,
                version_3_1_datetime=participant_info.gror_authored_time
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

    def test_core(self):
        """
        Check that all versions upgrade to CORE_MINUS_PM when requirements are met.
        And that none of them downgrade when EHR is revoked.
        """
        participant_info = self._build_participant_info(
            consent_cohort=ParticipantCohort.COHORT_2,
            primary_authored_time=datetime(2018, 1, 17),
            ehr_consent_ranges=[
                DateRange(start=datetime(2018, 1, 17), end=datetime(2018, 4, 13))
            ],
            basics_time=datetime(2018, 1, 17),
            overall_health_time=datetime(2018, 1, 17),
            lifestyle_time=datetime(2018, 1, 17),
            biobank_received_dna_sample_time=datetime(2018, 2, 21),
            physical_measurements_time=datetime(2018, 3, 1)
        )
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.FULL_PARTICIPANT,
                version_3_0_status=EnrollmentStatusV30.CORE_PARTICIPANT,
                version_3_1_status=EnrollmentStatusV31.CORE_PARTICIPANT,
                version_legacy_datetime=participant_info.earliest_physical_measurements_time,
                version_3_0_datetime=participant_info.earliest_physical_measurements_time,
                version_3_1_datetime=participant_info.earliest_physical_measurements_time
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

    def test_baseline(self):
        """
        Check that 3.1 upgrades to BASELINE with an EHR file (and that others stay the same).
        """
        participant_info = self._build_participant_info(
            consent_cohort=ParticipantCohort.COHORT_3,
            primary_authored_time=datetime(2018, 1, 17),
            ehr_consent_ranges=[
                DateRange(start=datetime(2018, 1, 17))
            ],
            basics_time=datetime(2018, 1, 17),
            overall_health_time=datetime(2018, 1, 17),
            lifestyle_time=datetime(2018, 1, 17),
            gror_time=datetime(2018, 2, 7),
            biobank_received_dna_sample_time=datetime(2018, 1, 21),
            physical_measurements_time=datetime(2018, 3, 1),
            ehr_file_submitted_time=datetime(2018, 5, 6)
        )
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.FULL_PARTICIPANT,
                version_3_0_status=EnrollmentStatusV30.CORE_PARTICIPANT,
                version_3_1_status=EnrollmentStatusV31.BASELINE_PARTICIPANT,
                version_legacy_datetime=participant_info.earliest_physical_measurements_time,
                version_3_0_datetime=participant_info.earliest_physical_measurements_time,
                version_3_1_datetime=participant_info.earliest_ehr_file_received_time
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

        # Check that BASELINE also needs the DNA update for earlier cohorts.
        participant_info.consent_cohort = ParticipantCohort.COHORT_2
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.FULL_PARTICIPANT,
                version_3_0_status=EnrollmentStatusV30.CORE_PARTICIPANT,
                version_3_1_status=EnrollmentStatusV31.CORE_PARTICIPANT,
                version_legacy_datetime=participant_info.earliest_physical_measurements_time,
                version_3_0_datetime=participant_info.earliest_physical_measurements_time,
                version_3_1_datetime=participant_info.earliest_physical_measurements_time
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

        participant_info.dna_update_time = datetime(2018, 6, 8)
        self.assertEnrollmentInfoEqual(
            EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.FULL_PARTICIPANT,
                version_3_0_status=EnrollmentStatusV30.CORE_PARTICIPANT,
                version_3_1_status=EnrollmentStatusV31.BASELINE_PARTICIPANT,
                version_legacy_datetime=participant_info.earliest_physical_measurements_time,
                version_3_0_datetime=participant_info.earliest_physical_measurements_time,
                version_3_1_datetime=participant_info.dna_update_time
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

    @classmethod
    def _build_participant_info(
        cls,
        primary_authored_time,
        consent_cohort=ParticipantCohort.COHORT_3,
        gror_time=None,
        basics_time=None,
        overall_health_time=None,
        lifestyle_time=None,
        ehr_consent_ranges: List[DateRange] = None,
        biobank_received_dna_sample_time=None,
        physical_measurements_time=None,
        ehr_file_submitted_time=None,
        dna_update_time=None,
        current_enrollment: EnrollmentInfo = None
    ):
        if not ehr_consent_ranges:
            ehr_consent_ranges = []
        if not current_enrollment:
            default_first_status_time = datetime(2017, 1, 1)
            current_enrollment = EnrollmentInfo(
                version_legacy_status=EnrollmentStatus.INTERESTED,
                version_3_0_status=EnrollmentStatusV30.PARTICIPANT,
                version_3_1_status=EnrollmentStatusV31.PARTICIPANT,
                version_legacy_datetime=default_first_status_time,
                version_3_0_datetime=default_first_status_time,
                version_3_1_datetime=default_first_status_time
            )

        return EnrollmentDependencies(
            consent_cohort=consent_cohort,
            primary_consent_authored_time=primary_authored_time,
            gror_authored_time=gror_time,
            basics_authored_time=basics_time,
            overall_health_authored_time=overall_health_time,
            lifestyle_authored_time=lifestyle_time,
            ehr_consent_date_range_list=ehr_consent_ranges,
            earliest_biobank_received_dna_time=biobank_received_dna_sample_time,
            earliest_physical_measurements_time=physical_measurements_time,
            dna_update_time=dna_update_time,
            earliest_ehr_file_received_time=ehr_file_submitted_time,
            current_enrollment=current_enrollment
        )

    @classmethod
    def assertEnrollmentInfoEqual(
        cls,
        expected_info: EnrollmentInfo,
        actual_info: EnrollmentInfo
    ):
        assert (
            expected_info.version_legacy_status == actual_info.version_legacy_status
            and expected_info.version_legacy_datetime == actual_info.version_legacy_datetime
            and expected_info.version_3_0_status == actual_info.version_3_0_status
            and expected_info.version_3_0_datetime == actual_info.version_3_0_datetime
            and expected_info.version_3_1_status == actual_info.version_3_1_status
            and expected_info.version_3_1_datetime == actual_info.version_3_1_datetime
        ), f'\nExpected progress:\n{expected_info}\ndoes not match actual:\n{actual_info}'
