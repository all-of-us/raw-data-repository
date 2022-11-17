from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from rdr_service.participant_enums import (
    EnrollmentStatus,
    EnrollmentStatusV30,
    EnrollmentStatusV31,
    ParticipantCohort
)
from rdr_service.services.system_utils import DateRange


@dataclass
class EnrollmentInfo:
    """
    Convenience class for communicating enrollment progress for each version
    """
    version_legacy_status: EnrollmentStatus = None
    version_legacy_dates: dict = field(default_factory=dict)

    version_3_0_status: EnrollmentStatusV30 = None
    version_3_0_dates: dict = field(default_factory=dict)

    version_3_1_status: EnrollmentStatusV31 = None
    version_3_1_dates: dict = field(default_factory=dict)

    def upgrade_legacy_status(self, status: EnrollmentStatus, achieved_date: datetime):
        self.version_legacy_status = status
        self.version_legacy_dates[status] = achieved_date

    def upgrade_3_0_status(self, status: EnrollmentStatusV30, achieved_date: datetime):
        self.version_3_0_status = status
        self.version_3_0_dates[status] = achieved_date

    def upgrade_3_1_status(self, status: EnrollmentStatusV31, achieved_date: datetime):
        self.version_3_1_status = status
        self.version_3_1_dates[status] = achieved_date


@dataclass
class EnrollmentDependencies:
    """
    Convenience class for communicating data needed for finding enrollment progress
    """

    consent_cohort: ParticipantCohort
    primary_consent_authored_time: datetime

    dna_update_time: datetime  # Cohorts 1 and 2

    gror_authored_time: datetime
    basics_authored_time: datetime
    overall_health_authored_time: datetime
    lifestyle_authored_time: datetime

    ehr_consent_date_range_list: List[DateRange]
    """DateRanges of when the participant expressed interest in sharing EHR data. Must be in chronological order"""

    earliest_biobank_received_dna_time: datetime
    earliest_ehr_file_received_time: datetime
    earliest_physical_measurements_time: datetime

    @property
    def first_ehr_consent_date(self):
        if len(self.ehr_consent_date_range_list) > 0:
            return self.ehr_consent_date_range_list[0].start

        return None

    @property
    def has_completed_dna_update(self):
        return self.dna_update_time is not None

    @property
    def has_completed_gror_survey(self):
        return self.gror_authored_time is not None

    @property
    def has_completed_the_basics_survey(self):
        return self.basics_authored_time is not None

    @property
    def has_completed_overall_health_survey(self):
        return self.overall_health_authored_time is not None

    @property
    def has_completed_lifestyle_survey(self):
        return self.lifestyle_authored_time is not None

    @property
    def ever_expressed_interest_in_sharing_ehr(self):
        return len(self.ehr_consent_date_range_list) > 0

    @property
    def biobank_received_dna_sample(self):
        return self.earliest_biobank_received_dna_time is not None

    @property
    def has_had_ehr_file_submitted(self):
        return self.earliest_ehr_file_received_time is not None

    @property
    def submitted_physical_measurements(self):
        return self.earliest_physical_measurements_time is not None


class EnrollmentCalculation:
    @classmethod
    def get_enrollment_info(cls, participant_info: EnrollmentDependencies) -> EnrollmentInfo:
        # RDR currently only displays enrollment status for participants that have consented to the Primary consent.
        # So if this is called for any participant, it is assumed they have provided Primary consent.
        enrollment = EnrollmentInfo()
        enrollment.upgrade_legacy_status(EnrollmentStatus.INTERESTED, participant_info.primary_consent_authored_time)
        enrollment.upgrade_3_0_status(EnrollmentStatusV30.PARTICIPANT, participant_info.primary_consent_authored_time)
        enrollment.upgrade_3_1_status(EnrollmentStatusV31.PARTICIPANT, participant_info.primary_consent_authored_time)

        cls._set_legacy_status(enrollment, participant_info)
        cls._set_v30_status(enrollment, participant_info)
        cls._set_v31_status(enrollment, participant_info)

        return enrollment

    @classmethod
    def _set_legacy_status(cls, enrollment: EnrollmentInfo, participant_info: EnrollmentDependencies):
        if participant_info.ever_expressed_interest_in_sharing_ehr:
            enrollment.upgrade_legacy_status(EnrollmentStatus.MEMBER, participant_info.first_ehr_consent_date)

        # Find if CORE_MINUS_PM status is met
        dates_needed_for_upgrade = [
            participant_info.first_ehr_consent_date,
            participant_info.basics_authored_time,
            participant_info.overall_health_authored_time,
            participant_info.lifestyle_authored_time,
            participant_info.earliest_biobank_received_dna_time
        ]
        if participant_info.consent_cohort == ParticipantCohort.COHORT_3:
            dates_needed_for_upgrade.append(participant_info.gror_authored_time)

        core_minus_pm_reqs_met_time = cls._get_requirements_met_date(dates_needed_for_upgrade)
        if core_minus_pm_reqs_met_time:
            enrollment.upgrade_legacy_status(EnrollmentStatus.CORE_MINUS_PM, core_minus_pm_reqs_met_time)

        # Find if CORE status is met
        dates_needed_for_upgrade.append(participant_info.earliest_physical_measurements_time)
        core_reqs_met_time = cls._get_requirements_met_date(dates_needed_for_upgrade)
        if core_reqs_met_time:
            enrollment.upgrade_legacy_status(EnrollmentStatus.FULL_PARTICIPANT, core_reqs_met_time)

    @classmethod
    def _set_v30_status(cls, enrollment: EnrollmentInfo, participant_info: EnrollmentDependencies):
        if not participant_info.ever_expressed_interest_in_sharing_ehr:
            return  # stop here without ehr interest, any more upgrades to the 3.0 enrollment status require it
        enrollment.upgrade_3_0_status(EnrollmentStatusV30.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date)

        if not participant_info.has_completed_the_basics_survey:
            return enrollment  # stop here without TheBasics, any more upgrades to the enrollment status require it

        # continue upgrading since we have TheBasics
        matching_date = cls._get_requirements_met_date([
            participant_info.first_ehr_consent_date,
            participant_info.basics_authored_time
        ])
        if matching_date:
            enrollment.upgrade_3_0_status(EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE, matching_date)

        if cls._meets_requirements_for_core_minus_pm(participant_info):
            enrollment.upgrade_3_0_status(
                EnrollmentStatusV30.CORE_MINUS_PM,
                max(cls._get_dates_needed_for_core_minus_pm(participant_info))
            )

        if cls._meets_requirements_for_core(participant_info):
            enrollment.upgrade_3_0_status(
                EnrollmentStatusV30.CORE_PARTICIPANT,
                max(cls._get_dates_needed_for_core(participant_info))
            )

    @classmethod
    def _set_v31_status(cls, enrollment: EnrollmentInfo, participant_info: EnrollmentDependencies):
        if not participant_info.ever_expressed_interest_in_sharing_ehr:
            return  # stop here without ehr interest, any more upgrades to the enrollment status require it

        enrollment.upgrade_3_1_status(EnrollmentStatusV31.PARTICIPANT_PLUS_EHR, participant_info.first_ehr_consent_date)

        if not participant_info.has_completed_the_basics_survey:
            return  # stop here without TheBasics, any more upgrades to the enrollment status require it

        # Upgrading 3.1 to PLUS_BASICS requires TheBasics and a GROR response
        if participant_info.has_completed_gror_survey:
            matching_date = cls._get_requirements_met_date([
                participant_info.first_ehr_consent_date,
                participant_info.basics_authored_time,
                participant_info.gror_authored_time
            ])
            if matching_date:
                enrollment.upgrade_3_1_status(EnrollmentStatusV31.PARTICIPANT_PLUS_BASICS, matching_date)

        if cls._meets_requirements_for_core_minus_pm(participant_info):
            enrollment.upgrade_3_1_status(
                EnrollmentStatusV31.CORE_MINUS_PM,
                max(cls._get_dates_needed_for_core_minus_pm(participant_info))
            )

        if cls._meets_requirements_for_core(participant_info):
            enrollment.upgrade_3_1_status(
                EnrollmentStatusV31.CORE_PARTICIPANT,
                max(cls._get_dates_needed_for_core(participant_info))
            )

            # Check to see if the participant also meets BASELINE requirements
            if (
                participant_info.has_had_ehr_file_submitted
                and (
                    participant_info.consent_cohort not in (ParticipantCohort.COHORT_1, ParticipantCohort.COHORT_2)
                    or participant_info.has_completed_dna_update
                )
            ):
                # Track the extra dates needed
                # (definitely need the date of an ehr file, but also possibly the dna update time)
                extra_dates_needed = [participant_info.earliest_ehr_file_received_time]
                if participant_info.consent_cohort in [ParticipantCohort.COHORT_1, ParticipantCohort.COHORT_2]:
                    extra_dates_needed.append(participant_info.dna_update_time)

                enrollment.upgrade_3_1_status(
                    EnrollmentStatusV31.BASELINE_PARTICIPANT,
                    max([
                        *cls._get_dates_needed_for_core(participant_info),
                        *extra_dates_needed
                    ])
                )

        return enrollment

    @classmethod
    def _meets_requirements_for_core_minus_pm(cls, participant_info: EnrollmentDependencies):
        return not any(
            required_date is None
            for required_date in cls._get_dates_needed_for_core_minus_pm(participant_info)
        )

    @classmethod
    def _get_dates_needed_for_core_minus_pm(cls, participant_info: EnrollmentDependencies):
        dates_needed = [
            participant_info.first_ehr_consent_date,
            participant_info.basics_authored_time,
            participant_info.overall_health_authored_time,
            participant_info.lifestyle_authored_time,
            participant_info.earliest_biobank_received_dna_time
        ]
        if participant_info.consent_cohort == ParticipantCohort.COHORT_3:
            dates_needed.append(participant_info.gror_authored_time)

        return dates_needed

    @classmethod
    def _meets_requirements_for_core(cls, participant_info: EnrollmentDependencies):
        return not any(
            required_date is None
            for required_date in cls._get_dates_needed_for_core(participant_info)
        )

    @classmethod
    def _get_dates_needed_for_core(cls, participant_info: EnrollmentDependencies):
        return [
            participant_info.earliest_physical_measurements_time,
            *cls._get_dates_needed_for_core_minus_pm(participant_info)
        ]

    @classmethod
    def _get_requirements_met_date(cls, required_date_list: List[datetime]) -> Optional[datetime]:
        if any([required_date is None for required_date in required_date_list]):
            return None
        else:
            return max(required_date_list)
