from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from rdr_service.participant_enums import ParticipantCohort


@dataclass
class Consent:
    is_consent_provided:    bool        # Indicates that we got a response that gives a "Yes" answer
    authored_timestamp:     datetime


@dataclass
class RetentionEligibilityDependencies:
    primary_consent:                        Consent
    first_ehr_consent:                      Optional[Consent]   # First consent, or intent, to share EHR
    is_deceased:                            bool                # Has an APPROVED deceased report
    is_withdrawn:                           bool
    dna_samples_timestamp:                  Optional[datetime]
    consent_cohort:                         ParticipantCohort
    has_uploaded_ehr_file:                  bool
    latest_ehr_upload_timestamp:            Optional[datetime]  # Date RDR has for most recent upload of EHR file

    basics_response_timestamp:              Optional[datetime]
    overallhealth_response_timestamp:       Optional[datetime]
    lifestyle_response_timestamp:           Optional[datetime]

    healthcare_access_response_timestamp:   Optional[datetime]
    family_health_response_timestamp:       Optional[datetime]
    medical_history_response_timestamp:     Optional[datetime]
    fam_med_history_response_timestamp:     Optional[datetime]
    sdoh_response_timestamp:                Optional[datetime]
    latest_cope_response_timestamp:         Optional[datetime]  # Most recent response to any of the cope/vax surveys
    remote_pm_response_timestamp:           Optional[datetime]
    life_func_response_timestamp:           Optional[datetime]
    reconsent_response_timestamp:           Optional[datetime]  # Cohort 1 reconsent to primary consent
    gror_response_timestamp:                Optional[datetime]


class RetentionEligibility:
    def __init__(self, participant_data: RetentionEligibilityDependencies):
        self._participant = participant_data

    @property
    def is_eligible(self) -> bool:
        return (
            not self._participant.is_deceased
            and not self._participant.is_withdrawn
            and self._did_provide_consent(self._participant.primary_consent)
            and self._did_provide_consent(self._participant.first_ehr_consent)
            and self._participant.basics_response_timestamp is not None
            and self._participant.overallhealth_response_timestamp is not None
            and self._participant.lifestyle_response_timestamp is not None
            and self._participant.dna_samples_timestamp is not None
        )

    @property
    def retention_eligible_date(self) -> Optional[datetime]:
        if not self.is_eligible:
            return None

        return max(
            self._participant.primary_consent.authored_timestamp,
            self._participant.first_ehr_consent.authored_timestamp,
            self._participant.basics_response_timestamp,
            self._participant.overallhealth_response_timestamp,
            self._participant.lifestyle_response_timestamp,
            self._participant.dna_samples_timestamp
        )

    @property
    def is_actively_retained(self) -> bool:
        if not self.is_eligible:
            return False

        return (
            self._is_less_than_18_months_ago(self._participant.healthcare_access_response_timestamp)
            or self._is_less_than_18_months_ago(self._participant.family_health_response_timestamp)
            or self._is_less_than_18_months_ago(self._participant.medical_history_response_timestamp)
            or self._is_less_than_18_months_ago(self._participant.fam_med_history_response_timestamp)
            or self._is_less_than_18_months_ago(self._participant.sdoh_response_timestamp)
            or self._is_less_than_18_months_ago(self._participant.latest_cope_response_timestamp)

            or self._is_less_than_18_months_ago(self._participant.remote_pm_response_timestamp)
            or self._is_less_than_18_months_ago(self._participant.life_func_response_timestamp)
            or (
                self._participant.consent_cohort in [ParticipantCohort.COHORT_1, ParticipantCohort.COHORT_2]
                and self._is_less_than_18_months_ago(self._participant.gror_response_timestamp)
            ) or (
                self._participant.consent_cohort == ParticipantCohort.COHORT_1
                and self._is_less_than_18_months_ago(self._participant.reconsent_response_timestamp)
            )
        )

    @property
    def last_active_retention_date(self) -> Optional[datetime]:
        if not self.is_eligible:
            return None

        possible_dates = [
            self._participant.healthcare_access_response_timestamp,
            self._participant.family_health_response_timestamp,
            self._participant.medical_history_response_timestamp,
            self._participant.fam_med_history_response_timestamp,
            self._participant.sdoh_response_timestamp,
            self._participant.latest_cope_response_timestamp,
            self._participant.remote_pm_response_timestamp,
            self._participant.life_func_response_timestamp
        ]

        if self._participant.consent_cohort in [ParticipantCohort.COHORT_1, ParticipantCohort.COHORT_2]:
            possible_dates.append(self._participant.gror_response_timestamp)

        if self._participant.consent_cohort == ParticipantCohort.COHORT_1:
            possible_dates.append(self._participant.reconsent_response_timestamp)

        # Create a list of any gathered timestamps that are not None
        none_null_timestamps = [timestamp for timestamp in possible_dates if timestamp]

        # If all timestamps were None, then return None
        if not none_null_timestamps:
            return None

        # Otherwise return the latest timestamp
        return max(none_null_timestamps)

    @property
    def is_passively_retained(self):
        return (
            self.is_eligible
            and self._is_less_than_18_months_ago(self._participant.latest_ehr_upload_timestamp)
            and self._participant.has_uploaded_ehr_file
        )

    @classmethod
    def _did_provide_consent(cls, consent: Consent) -> bool:
        return False if consent is None else consent.is_consent_provided

    @classmethod
    def _is_less_than_18_months_ago(cls, timestamp: datetime) -> bool:
        return None if timestamp is None else timestamp >= cls._get_datetime_18_months_ago()

    @classmethod
    def _get_datetime_18_months_ago(cls) -> datetime:
        return datetime.today() - timedelta(days=547)
