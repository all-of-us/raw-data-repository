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
    EnrollmentStatusV32,
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
            self._build_expected_enrollment_info(
                legacy_data=[(EnrollmentStatus.INTERESTED, participant_info.primary_consent_authored_time)],
                v30_data=[(EnrollmentStatusV30.PARTICIPANT, participant_info.primary_consent_authored_time)],
                v32_data=[(EnrollmentStatusV32.PARTICIPANT, participant_info.primary_consent_authored_time)]
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
            self._build_expected_enrollment_info(
                legacy_data=[
                    (EnrollmentStatus.INTERESTED, participant_info.primary_consent_authored_time),
                    (EnrollmentStatus.MEMBER, participant_info.first_ehr_consent_date)
                ],
                v30_data=[
                    (EnrollmentStatusV30.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV30.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date)
                ],
                v32_data=[
                    (EnrollmentStatusV32.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV32.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date)
                ]
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

    def test_basics_and_gror(self):
        """
        3.0 should upgrade to PARTICIPANT_PMB_ELIGIBLE when TheBasics has been submitted, but also needs EHR
        3.2 should upgrade to needs TheBasics, but doesn't need EHR.
        The legacy version of the calculation would still just show them as MEMBER.
        """
        participant_info = self._build_participant_info(
            primary_authored_time=datetime(2020, 7, 18),
            basics_time=datetime(2020, 7, 27)
        )
        self.assertEnrollmentInfoEqual(
            self._build_expected_enrollment_info(
                legacy_data=[
                    (EnrollmentStatus.INTERESTED, participant_info.primary_consent_authored_time)
                ],
                v30_data=[
                    (EnrollmentStatusV30.PARTICIPANT, participant_info.primary_consent_authored_time)
                ],
                v32_data=[
                    (EnrollmentStatusV32.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV32.ENROLLED_PARTICIPANT, participant_info.basics_authored_time)
                ]
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

        participant_info.ehr_consent_date_range_list = [DateRange(start=datetime(2020, 7, 18))]
        self.assertEnrollmentInfoEqual(
            self._build_expected_enrollment_info(
                legacy_data=[
                    (EnrollmentStatus.INTERESTED, participant_info.primary_consent_authored_time),
                    (EnrollmentStatus.MEMBER, participant_info.first_ehr_consent_date)
                ],
                v30_data=[
                    (EnrollmentStatusV30.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV30.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date),
                    (EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE, participant_info.basics_authored_time)
                ],
                v32_data=[
                    (EnrollmentStatusV32.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV32.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date),
                    (EnrollmentStatusV32.ENROLLED_PARTICIPANT, participant_info.basics_authored_time)
                ]
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
            self._build_expected_enrollment_info(
                legacy_data=[
                    (EnrollmentStatus.INTERESTED, participant_info.primary_consent_authored_time),
                    (EnrollmentStatus.MEMBER, participant_info.first_ehr_consent_date),
                    (EnrollmentStatus.CORE_MINUS_PM, participant_info.earliest_biobank_received_dna_time)
                ],
                v30_data=[
                    (EnrollmentStatusV30.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV30.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date),
                    (EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE, participant_info.basics_authored_time),
                    (EnrollmentStatusV30.CORE_MINUS_PM, participant_info.earliest_biobank_received_dna_time)
                ],
                v32_data=[
                    (EnrollmentStatusV32.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV32.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date),
                    (EnrollmentStatusV32.CORE_MINUS_PM, participant_info.earliest_biobank_received_dna_time)
                ]
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

        # Check that GROR is needed for cohort 3 participants
        participant_info.consent_cohort = ParticipantCohort.COHORT_3
        enrollment_info = EnrollmentCalculation.get_enrollment_info(participant_info)
        self.assertNotEqual(EnrollmentStatus.CORE_MINUS_PM, enrollment_info.version_legacy_status)
        self.assertNotEqual(EnrollmentStatusV30.CORE_MINUS_PM, enrollment_info.version_3_0_status)
        self.assertNotEqual(EnrollmentStatusV32.CORE_MINUS_PM, enrollment_info.version_3_2_status)

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
            self._build_expected_enrollment_info(
                legacy_data=[
                    (EnrollmentStatus.INTERESTED, participant_info.primary_consent_authored_time),
                    (EnrollmentStatus.MEMBER, participant_info.first_ehr_consent_date),
                    (EnrollmentStatus.CORE_MINUS_PM, participant_info.earliest_biobank_received_dna_time),
                    (EnrollmentStatus.FULL_PARTICIPANT, participant_info.earliest_physical_measurements_time)
                ],
                v30_data=[
                    (EnrollmentStatusV30.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV30.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date),
                    (EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE, participant_info.basics_authored_time),
                    (EnrollmentStatusV30.CORE_MINUS_PM, participant_info.earliest_biobank_received_dna_time),
                    (EnrollmentStatusV30.CORE_PARTICIPANT, participant_info.earliest_physical_measurements_time)
                ],
                v32_data=[
                    (EnrollmentStatusV32.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV32.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date),
                    (EnrollmentStatusV32.CORE_MINUS_PM, participant_info.earliest_biobank_received_dna_time),
                    (EnrollmentStatusV32.CORE_PARTICIPANT, participant_info.earliest_physical_measurements_time)
                ]
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

    def test_core_achieved_with_rescinded_ehr(self):
        """
        We only need a Yes response at any time for status upgrades.
        Any further status upgrades should be allowed even if they've since said No to EHR consent.
        """
        participant_info = self._build_participant_info(
            consent_cohort=ParticipantCohort.COHORT_2,
            primary_authored_time=datetime(2018, 1, 17),
            ehr_consent_ranges=[
                DateRange(start=datetime(2018, 1, 17), end=datetime(2018, 1, 20))
            ],
            basics_time=datetime(2018, 1, 17),
            overall_health_time=datetime(2018, 1, 17),
            lifestyle_time=datetime(2018, 1, 17),
            biobank_received_dna_sample_time=datetime(2018, 2, 21),
            physical_measurements_time=datetime(2018, 3, 1)
        )
        self.assertEnrollmentInfoEqual(
            self._build_expected_enrollment_info(
                legacy_data=[
                    (EnrollmentStatus.INTERESTED, participant_info.primary_consent_authored_time),
                    (EnrollmentStatus.MEMBER, participant_info.first_ehr_consent_date),
                    (EnrollmentStatus.CORE_MINUS_PM, participant_info.earliest_biobank_received_dna_time),
                    (EnrollmentStatus.FULL_PARTICIPANT, participant_info.earliest_physical_measurements_time)
                ],
                v30_data=[
                    (EnrollmentStatusV30.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV30.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date),
                    (EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE, participant_info.basics_authored_time),
                    (EnrollmentStatusV30.CORE_MINUS_PM, participant_info.earliest_biobank_received_dna_time),
                    (EnrollmentStatusV30.CORE_PARTICIPANT, participant_info.earliest_physical_measurements_time)
                ],
                v32_data=[
                    (EnrollmentStatusV32.PARTICIPANT, participant_info.primary_consent_authored_time),
                    (EnrollmentStatusV32.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date),
                    (EnrollmentStatusV32.CORE_MINUS_PM, participant_info.earliest_biobank_received_dna_time),
                    (EnrollmentStatusV32.CORE_PARTICIPANT, participant_info.earliest_physical_measurements_time)
                ]
            ),
            EnrollmentCalculation.get_enrollment_info(participant_info)
        )

    def test_core_status_does_not_mean_core_data(self):
        """
        A participant that reaches CORE status doesn't also get the Core Data flag set (yet)
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

        enrollment_status = EnrollmentCalculation.get_enrollment_info(participant_info)
        self.assertEqual(EnrollmentStatusV32.CORE_PARTICIPANT, enrollment_status.version_3_2_status)
        self.assertFalse(enrollment_status.has_core_data)

    def test_achieving_core_data(self):
        """
        A participant that reaches CORE status doesn't also get the Core Data flag set (yet)
        """
        wgs_processed_time = datetime(2018, 5, 10)
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
            physical_measurements_time=datetime(2018, 3, 1),
            earliest_core_pm_time=datetime(2018, 3, 1),
            wgs_sequencing_time=wgs_processed_time,
            ehr_file_submitted_time=datetime(2018, 4, 7)
        )

        enrollment_status = EnrollmentCalculation.get_enrollment_info(participant_info)
        self.assertTrue(enrollment_status.has_core_data)
        self.assertEqual(wgs_processed_time, enrollment_status.core_data_time)

    @classmethod
    def _build_expected_enrollment_info(cls, legacy_data, v30_data, v32_data):
        enrollment = EnrollmentInfo()
        for status, achieved_date in legacy_data:
            enrollment.upgrade_legacy_status(status, achieved_date)
        for status, achieved_date in v30_data:
            enrollment.upgrade_3_0_status(status, achieved_date)
        for status, achieved_date in v32_data:
            enrollment.upgrade_3_2_status(status, achieved_date)
        return enrollment

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
        earliest_mediated_ehr_receipt_time=None,
        dna_update_time=None,
        current_enrollment: EnrollmentInfo = None,
        earliest_core_pm_time: datetime = None,
        wgs_sequencing_time: datetime = None
    ):
        if not ehr_consent_ranges:
            ehr_consent_ranges = []
        if not current_enrollment:
            default_first_status_time = datetime(2017, 1, 1)
            current_enrollment = EnrollmentInfo()
            current_enrollment.upgrade_legacy_status(EnrollmentStatus.INTERESTED, default_first_status_time)
            current_enrollment.upgrade_3_0_status(EnrollmentStatusV30.PARTICIPANT, default_first_status_time)
            current_enrollment.upgrade_3_2_status(EnrollmentStatusV32.PARTICIPANT, default_first_status_time)

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
            earliest_mediated_ehr_receipt_time=earliest_mediated_ehr_receipt_time,
            earliest_height_measurement_time=earliest_core_pm_time,
            earliest_weight_measurement_time=earliest_core_pm_time,
            wgs_sequencing_time=wgs_sequencing_time
        )

    @classmethod
    def assertEnrollmentInfoEqual(
        cls,
        expected_info: EnrollmentInfo,
        actual_info: EnrollmentInfo
    ):
        assert (
            expected_info.version_legacy_status == actual_info.version_legacy_status
            and expected_info.version_legacy_dates == actual_info.version_legacy_dates
            and expected_info.version_3_0_status == actual_info.version_3_0_status
            and expected_info.version_3_0_dates == actual_info.version_3_0_dates
            and expected_info.version_3_2_status == actual_info.version_3_2_status
            and expected_info.version_3_2_dates == actual_info.version_3_2_dates
        ), f'\nExpected progress:\n{expected_info}\ndoes not match actual:\n{actual_info}'
