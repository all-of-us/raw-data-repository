from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from rdr_service.model.enrollment_dependencies import EnrollmentDependencies
from rdr_service.participant_enums import ParticipantCohortEnum

cache = dict()


class EnrollmentDependenciesDao:

    @classmethod
    def get_enrollment_dependencies(cls, participant_id: int, session: Session) -> Optional[EnrollmentDependencies]:
        if participant_id in cache:
            return cache[participant_id]

        result = session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == participant_id
        ).first()
        if result:
            cache[participant_id] = result
        return result

    @classmethod
    def _set_field(cls, field_name: str, value, participant_id: int, session: Session):
        obj = cls.get_enrollment_dependencies(participant_id=participant_id, session=session)
        if not obj:
            obj = EnrollmentDependencies(participant_id=participant_id)
            cache[participant_id] = obj
            session.add(obj)

        if getattr(obj, field_name) is None:
            setattr(obj, field_name, value)

    @classmethod
    def set_consent_cohort(cls, value: ParticipantCohortEnum, participant_id: int, session: Session):
        cls._set_field(
            'consent_cohort', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_primary_consent_authored_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'primary_consent_authored_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_intent_to_share_ehr_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'intent_to_share_ehr_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_full_ehr_consent_authored_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'full_ehr_consent_authored_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_gror_consent_authored_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'gror_consent_authored_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_dna_consent_update_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'dna_consent_update_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_basics_survey_authored_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'basics_survey_authored_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_overall_health_survey_authored_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'overall_health_survey_authored_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_lifestyle_survey_authored_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'lifestyle_survey_authored_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_exposures_survey_authored_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'exposures_survey_authored_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_biobank_received_dna_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'biobank_received_dna_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_wgs_sequencing_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'wgs_sequencing_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_first_ehr_file_received_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'first_ehr_file_received_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_first_mediated_ehr_received_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'first_mediated_ehr_received_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_physical_measurements_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'physical_measurements_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_weight_physical_measurements_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'weight_physical_measurements_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_height_physical_measurements_time(cls, value: datetime, participant_id: int, session: Session):
        cls._set_field(
            'height_physical_measurements_time', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_is_pediatric_participant(cls, value: bool, participant_id: int, session: Session):
        cls._set_field(
            'is_pediatric_participant', value, session=session, participant_id=participant_id
        )

    @classmethod
    def set_has_linked_guardian_account(cls, value: bool, participant_id: int, session: Session):
        cls._set_field(
            'has_linked_guardian_account', value, session=session, participant_id=participant_id
        )
