import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from rdr_service.model.enrollment_dependencies import EnrollmentDependencies as DbEnrollmentDependencies
from rdr_service.participant_enums import (
    EnrollmentStatus,
    EnrollmentStatusV30,
    EnrollmentStatusV32,
    ParticipantCohortEnum as ParticipantCohort
)


@dataclass
class EnrollmentInfo:
    """
    Convenience class for communicating enrollment progress for each version
    """
    version_legacy_status: EnrollmentStatus = None
    version_legacy_dates: dict = field(default_factory=dict)

    version_3_0_status: EnrollmentStatusV30 = None
    version_3_0_dates: dict = field(default_factory=dict)

    version_3_2_status: EnrollmentStatusV32 = None
    version_3_2_dates: dict = field(default_factory=dict)

    has_core_data: bool = False
    core_data_time: datetime = None

    def upgrade_legacy_status(self, status: EnrollmentStatus, achieved_date: datetime):
        self.version_legacy_status = status
        self.version_legacy_dates[status] = achieved_date

    def upgrade_3_0_status(self, status: EnrollmentStatusV30, achieved_date: datetime):
        self.version_3_0_status = status
        self.version_3_0_dates[status] = achieved_date

    def upgrade_3_2_status(self, status: EnrollmentStatusV32, achieved_date: datetime):
        self.version_3_2_status = status
        self.version_3_2_dates[status] = achieved_date


@dataclass
class EnrollmentDependencies:
    """
    Convenience class for communicating data needed for finding enrollment progress
    """

    def __init__(self, db_obj: DbEnrollmentDependencies):
        self._db_obj = db_obj

    @property
    def consent_cohort(self) -> ParticipantCohort:
        return self._db_obj.consent_cohort

    @property
    def primary_consent_authored_time(self) -> datetime:
        return self._db_obj.primary_consent_authored_time

    @property
    def first_full_ehr_consent_authored_time(self) -> datetime:
        return self._db_obj.full_ehr_consent_authored_time

    @property
    def dna_update_time(self) -> datetime:
        # Cohorts 1 and 2
        return self._db_obj.dna_consent_update_time

    @property
    def gror_authored_time(self) -> datetime:
        return self._db_obj.gror_consent_authored_time

    @property
    def basics_authored_time(self) -> datetime:
        return self._db_obj.basics_survey_authored_time

    @property
    def overall_health_authored_time(self) -> datetime:
        return self._db_obj.overall_health_survey_authored_time

    @property
    def lifestyle_authored_time(self) -> datetime:
        return self._db_obj.lifestyle_survey_authored_time

    @property
    def exposures_authored_time(self) -> datetime:
        return self._db_obj.exposures_survey_authored_time

    @property
    def earliest_biobank_received_dna_time(self) -> datetime:
        return self._db_obj.biobank_received_dna_time

    @property
    def earliest_ehr_file_received_time(self) -> datetime:
        return self._db_obj.first_ehr_file_received_time

    @property
    def earliest_mediated_ehr_receipt_time(self) -> datetime:
        return self._db_obj.first_mediated_ehr_received_time

    @property
    def earliest_physical_measurements_time(self) -> datetime:
        return self._db_obj.physical_measurements_time

    @property
    def earliest_weight_measurement_time(self) -> datetime:
        # Earliest physical measurement that meets core data reqs
        return self._db_obj.weight_physical_measurements_time

    @property
    def earliest_height_measurement_time(self) -> datetime:
        # Earliest physical measurement that meets core data reqs
        return self._db_obj.height_physical_measurements_time

    @property
    def wgs_sequencing_time(self) -> datetime:
        return self._db_obj.wgs_sequencing_time

    @property
    def is_pediatric_participant(self) -> bool:
        return self._db_obj.is_pediatric_participant

    @property
    def has_linked_guardian_accounts(self) -> bool:
        return self._db_obj.has_linked_guardian_account

    @property
    def first_ehr_consent_date(self):
        return self._db_obj.intent_to_share_ehr_time

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
        return self.first_ehr_consent_date is not None

    @property
    def biobank_received_dna_sample(self):
        return self.earliest_biobank_received_dna_time is not None

    @property
    def has_had_ehr_file_submitted(self):
        return self.earliest_ehr_file_received_time is not None

    @property
    def has_had_mediated_ehr_submitted(self):
        return self.earliest_mediated_ehr_receipt_time is not None

    @property
    def submitted_physical_measurements(self):
        return self.earliest_physical_measurements_time is not None

    def to_json_dict(self):
        return {
            "consent_cohort": self._db_obj.consent_cohort.name,
            "dna_update_time": self._db_obj.dna_consent_update_time,
            "gror_authored_time": self._db_obj.gror_consent_authored_time,
            "wgs_sequencing_time": self._db_obj.wgs_sequencing_time,
            "basics_authored_time": self._db_obj.basics_survey_authored_time,
            "exposures_authored_time": self._db_obj.exposures_survey_authored_time,
            "lifestyle_authored_time": self._db_obj.lifestyle_survey_authored_time,
            "is_pediatric_participant": self._db_obj.is_pediatric_participant,
            "first_ehr_consent_date": self._db_obj.intent_to_share_ehr_time,
            "has_linked_guardian_accounts": self._db_obj.has_linked_guardian_account,
            "overall_health_authored_time": self._db_obj.overall_health_survey_authored_time,
            "primary_consent_authored_time": self._db_obj.primary_consent_authored_time,
            "earliest_ehr_file_received_time": self._db_obj.first_ehr_file_received_time,
            "earliest_height_measurement_time": self._db_obj.height_physical_measurements_time,
            "earliest_weight_measurement_time": self._db_obj.weight_physical_measurements_time,
            "earliest_biobank_received_dna_time": self._db_obj.biobank_received_dna_time,
            "earliest_mediated_ehr_receipt_time": self._db_obj.first_mediated_ehr_received_time,
            "earliest_physical_measurements_time": self._db_obj.physical_measurements_time,
            "first_full_ehr_consent_authored_time": self._db_obj.full_ehr_consent_authored_time
        }


class EnrollmentCalculation:
    @classmethod
    def get_enrollment_info(cls, participant_info: EnrollmentDependencies) -> EnrollmentInfo:
        # RDR currently only displays enrollment status for participants that have consented to the Primary consent.
        # So if this is called for any participant, it is assumed they have provided Primary consent.
        enrollment = EnrollmentInfo()

        if participant_info.is_pediatric_participant and not participant_info.has_linked_guardian_accounts:
            # Pediatric participants must have a linked guardian account
            logging.error('Pediatric participant found without guardian account')
            return enrollment

        enrollment.upgrade_legacy_status(EnrollmentStatus.INTERESTED, participant_info.primary_consent_authored_time)
        enrollment.upgrade_3_0_status(EnrollmentStatusV30.PARTICIPANT, participant_info.primary_consent_authored_time)
        enrollment.upgrade_3_2_status(EnrollmentStatusV32.PARTICIPANT, participant_info.primary_consent_authored_time)

        cls._set_legacy_status(enrollment, participant_info)
        cls._set_v30_status(enrollment, participant_info)
        cls._set_v32_status(enrollment, participant_info)

        cls._set_core_data_fields(enrollment, participant_info)

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
        if (
            participant_info.consent_cohort == ParticipantCohort.COHORT_3
            and not participant_info.is_pediatric_participant
        ):
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
    def _set_v32_status(cls, enrollment: EnrollmentInfo, participant_info: EnrollmentDependencies):
        if participant_info.ever_expressed_interest_in_sharing_ehr:
            enrollment.upgrade_3_2_status(
                EnrollmentStatusV32.PARTICIPANT_PLUS_EHR,
                participant_info.first_ehr_consent_date
            )

        if participant_info.basics_authored_time:
            enrollment.upgrade_3_2_status(
                EnrollmentStatusV32.ENROLLED_PARTICIPANT,
                participant_info.basics_authored_time
            )

        dates_needed_for_pmb_eligible = [
            participant_info.first_full_ehr_consent_authored_time,
            participant_info.basics_authored_time
        ]
        if not participant_info.is_pediatric_participant:
            dates_needed_for_pmb_eligible.append(participant_info.gror_authored_time)
        met_pmb_eligible_reqs_time = cls._get_requirements_met_date(dates_needed_for_pmb_eligible)
        if met_pmb_eligible_reqs_time:
            enrollment.upgrade_3_2_status(EnrollmentStatusV32.PMB_ELIGIBLE, met_pmb_eligible_reqs_time)

        if cls._meets_requirements_for_core_minus_pm(participant_info):
            enrollment.upgrade_3_2_status(
                EnrollmentStatusV32.CORE_MINUS_PM,
                max(cls._get_dates_needed_for_core_minus_pm(participant_info))
            )

        if cls._meets_requirements_for_core(participant_info):
            enrollment.upgrade_3_2_status(
                EnrollmentStatusV32.CORE_PARTICIPANT,
                max(cls._get_dates_needed_for_core(participant_info))
            )

        return enrollment

    @classmethod
    def _set_core_data_fields(cls, enrollment: EnrollmentInfo, participant_info: EnrollmentDependencies):
        required_timestamp_list = [
            participant_info.first_ehr_consent_date,
            participant_info.basics_authored_time,
            participant_info.overall_health_authored_time,
            participant_info.earliest_height_measurement_time,
            participant_info.earliest_weight_measurement_time,
            participant_info.wgs_sequencing_time,
            participant_info.earliest_ehr_file_received_time
        ]
        if participant_info.consent_cohort == ParticipantCohort.COHORT_1:
            required_timestamp_list.append(participant_info.dna_update_time)

        if participant_info.is_pediatric_participant:
            required_timestamp_list.append(participant_info.exposures_authored_time)
        else:
            required_timestamp_list.append(participant_info.lifestyle_authored_time)

        if any(required_time is None for required_time in required_timestamp_list):
            return  # If any required timestamps are missing, leave Core Data flag as False

        enrollment.has_core_data = True  # All timestamps are present, so Core Data requirements are met
        enrollment.core_data_time = max(required_timestamp_list)

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
            participant_info.earliest_biobank_received_dna_time
        ]
        if (
            participant_info.consent_cohort == ParticipantCohort.COHORT_3
            and not participant_info.is_pediatric_participant
        ):
            dates_needed.append(participant_info.gror_authored_time)

        if participant_info.is_pediatric_participant:
            dates_needed.append(participant_info.exposures_authored_time)
        else:
            dates_needed.append(participant_info.lifestyle_authored_time)

        return dates_needed

    @classmethod
    def _meets_requirements_for_core(cls, participant_info: EnrollmentDependencies):
        return not any(
            required_date is None
            for required_date in cls._get_dates_needed_for_core(participant_info)
        )

    @classmethod
    def _get_dates_needed_for_core(cls, participant_info: EnrollmentDependencies):
        required_dates = cls._get_dates_needed_for_core_minus_pm(participant_info)
        required_dates.append(participant_info.earliest_physical_measurements_time)

        if participant_info.is_pediatric_participant:
            required_dates.extend([
                participant_info.earliest_weight_measurement_time,
                participant_info.earliest_height_measurement_time
            ])
        return required_dates

    @classmethod
    def _get_requirements_met_date(cls, required_date_list: List[datetime]) -> Optional[datetime]:
        if any([required_date is None for required_date in required_date_list]):
            return None
        else:
            return max(required_date_list)
