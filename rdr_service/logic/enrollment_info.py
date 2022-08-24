from dataclasses import dataclass
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
    version_legacy_status: EnrollmentStatus
    version_legacy_datetime: datetime

    version_3_0_status: EnrollmentStatusV30
    version_3_0_datetime: datetime

    version_3_1_status: EnrollmentStatusV31
    version_3_1_datetime: datetime


@dataclass
class EnrollmentDependencies:
    """
    Convenience class for communicating data needed for finding enrollment progress
    """

    consent_cohort: ParticipantCohort
    primary_consent_authored_time: datetime
    current_enrollment: EnrollmentInfo

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
        enrollment = EnrollmentInfo(
            version_legacy_status=EnrollmentStatus.INTERESTED,
            version_legacy_datetime=participant_info.primary_consent_authored_time,

            version_3_0_status=EnrollmentStatusV30.PARTICIPANT,
            version_3_0_datetime=participant_info.primary_consent_authored_time,

            version_3_1_status=EnrollmentStatusV31.PARTICIPANT,
            version_3_1_datetime=participant_info.primary_consent_authored_time
        )

        cls._set_legacy_status(enrollment, participant_info)
        cls._set_v30_status(enrollment, participant_info)
        cls._set_v31_status(enrollment, participant_info)

        return enrollment

    @classmethod
    def _set_legacy_status(cls, enrollment: EnrollmentInfo, participant_info: EnrollmentDependencies):
        if participant_info.ever_expressed_interest_in_sharing_ehr:
            latest_ehr_range = participant_info.ehr_consent_date_range_list[-1]
            # Check that EHR consent hasn't been revoked`
            if latest_ehr_range.end is None:
                enrollment.version_legacy_status = EnrollmentStatus.MEMBER
                enrollment.version_legacy_datetime = latest_ehr_range.start

        # Find if CORE_MINUS_PM status is met
        dates_needed_for_upgrade = [
            participant_info.basics_authored_time,
            participant_info.overall_health_authored_time,
            participant_info.lifestyle_authored_time,
            participant_info.earliest_biobank_received_dna_time
        ]
        if participant_info.consent_cohort == ParticipantCohort.COHORT_3:
            dates_needed_for_upgrade.append(participant_info.gror_authored_time)

        core_minus_pm_reqs_met_time = cls._get_requirements_met_date(
            participant_info.ehr_consent_date_range_list,
            other_required_date_list=dates_needed_for_upgrade
        )
        if core_minus_pm_reqs_met_time:
            enrollment.version_legacy_status = EnrollmentStatus.CORE_MINUS_PM
            enrollment.version_legacy_datetime = core_minus_pm_reqs_met_time

        # Find if CORE status is met
        dates_needed_for_upgrade.append(participant_info.earliest_physical_measurements_time)
        core_reqs_met_time = cls._get_requirements_met_date(
            participant_info.ehr_consent_date_range_list,
            other_required_date_list=dates_needed_for_upgrade
        )
        if core_reqs_met_time:
            enrollment.version_legacy_status = EnrollmentStatus.FULL_PARTICIPANT
            enrollment.version_legacy_datetime = core_reqs_met_time

        current_status = participant_info.current_enrollment.version_legacy_status
        if current_status == EnrollmentStatus.FULL_PARTICIPANT:
            enrollment.version_legacy_status = EnrollmentStatus.FULL_PARTICIPANT
        if (
            current_status == EnrollmentStatus.CORE_MINUS_PM
            and enrollment.version_legacy_status in (EnrollmentStatus.INTERESTED, EnrollmentStatus.MEMBER)
        ):
            enrollment.version_legacy_status = EnrollmentStatus.CORE_MINUS_PM

    @classmethod
    def _set_v30_status(cls, enrollment: EnrollmentInfo, participant_info: EnrollmentDependencies):
        if not participant_info.ever_expressed_interest_in_sharing_ehr:
            return  # stop here without ehr interest, any more upgrades to the 3.0 enrollment status require it

        enrollment.version_3_0_status = EnrollmentStatusV30.PARTICIPANT_PLUS_EHR
        enrollment.version_3_0_datetime = participant_info.first_ehr_consent_date

        if not participant_info.has_completed_the_basics_survey:
            return enrollment  # stop here without TheBasics, any more upgrades to the enrollment status require it

        # continue upgrading since we have TheBasics
        enrollment.version_3_0_status = EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE
        enrollment.version_3_0_datetime = max(
            participant_info.first_ehr_consent_date,
            participant_info.basics_authored_time
        )

        if cls._meets_requirements_for_core_minus_pm(participant_info):
            enrollment.version_3_0_status = EnrollmentStatusV30.CORE_MINUS_PM
            enrollment.version_3_0_datetime = max(cls._get_dates_needed_for_core_minus_pm(participant_info))

        if cls._meets_requirements_for_core(participant_info):
            enrollment.version_3_0_status = EnrollmentStatusV30.CORE_PARTICIPANT
            enrollment.version_3_0_datetime = max(cls._get_dates_needed_for_core(participant_info))

    @classmethod
    def _set_v31_status(cls, enrollment: EnrollmentInfo, participant_info: EnrollmentDependencies):
        if not participant_info.ever_expressed_interest_in_sharing_ehr:
            return  # stop here without ehr interest, any more upgrades to the enrollment status require it

        enrollment.version_3_1_status = EnrollmentStatusV31.PARTICIPANT_PLUS_EHR
        enrollment.version_3_1_datetime = participant_info.first_ehr_consent_date

        if not participant_info.has_completed_the_basics_survey:
            return  # stop here without TheBasics, any more upgrades to the enrollment status require it

        # Upgrading 3.1 to PLUS_BASICS requires TheBasics and a GROR response
        if participant_info.has_completed_gror_survey:
            enrollment.version_3_1_status = EnrollmentStatusV31.PARTICIPANT_PLUS_BASICS
            enrollment.version_3_1_datetime = max(
                participant_info.first_ehr_consent_date,
                participant_info.basics_authored_time,
                participant_info.gror_authored_time
            )

        if cls._meets_requirements_for_core_minus_pm(participant_info):
            enrollment.version_3_1_status = EnrollmentStatusV31.CORE_MINUS_PM
            enrollment.version_3_1_datetime = max(cls._get_dates_needed_for_core_minus_pm(participant_info))

        if cls._meets_requirements_for_core(participant_info):
            enrollment.version_3_1_status = EnrollmentStatusV31.CORE_PARTICIPANT
            enrollment.version_3_1_datetime = max(cls._get_dates_needed_for_core(participant_info))

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

                enrollment.version_3_1_status = EnrollmentStatusV31.BASELINE_PARTICIPANT
                enrollment.version_3_1_datetime = max([
                    *cls._get_dates_needed_for_core(participant_info),
                    *extra_dates_needed
                ])

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
    def _get_requirements_met_date(
        cls, ehr_range_list: List[DateRange], other_required_date_list: List[datetime]
    ) -> Optional[datetime]:
        if any([required_date is None for required_date in other_required_date_list]):
            return None

        for ehr_yes_range in ehr_range_list:
            matching_date = ehr_yes_range.find_first_overlap_list(other_required_date_list)
            if matching_date:
                return matching_date

        return None
