import sqlalchemy as sa

from rdr_service.clock import CLOCK
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime
from rdr_service.participant_enums import ParticipantCohortEnum as ParticipantCohort


class EnrollmentDependencies(Base):
    """
    Copy of operational data from other tables. Used to have a clear and collected place for checking what data is
    present or may still be needed for achieving different enrollment statuses.
    """

    __tablename__ = 'enrollment_dependencies'
    id = sa.Column(sa.BIGINT, primary_key=True, autoincrement=True, nullable=False)
    created = sa.Column(UTCDateTime, nullable=False, default=CLOCK.now())
    modified = sa.Column(UTCDateTime, nullable=False, default=CLOCK.now())
    participant_id = sa.Column(sa.Integer, sa.ForeignKey('participant.participant_id'))

    consent_cohort = sa.Column(sa.Enum(ParticipantCohort))
    primary_consent_authored_time = sa.Column(UTCDateTime)
    intent_to_share_ehr_time = sa.Column(UTCDateTime)
    full_ehr_consent_authored_time = sa.Column(UTCDateTime)
    gror_consent_authored_time = sa.Column(UTCDateTime)
    dna_consent_update_time = sa.Column(UTCDateTime)

    basics_survey_authored_time = sa.Column(UTCDateTime)
    overall_health_survey_authored_time = sa.Column(UTCDateTime)
    lifestyle_survey_authored_time = sa.Column(UTCDateTime)
    exposures_survey_authored_time = sa.Column(UTCDateTime)

    biobank_received_dna_time = sa.Column(UTCDateTime)
    wgs_sequencing_time = sa.Column(UTCDateTime)

    first_ehr_file_received_time = sa.Column(UTCDateTime)
    first_mediated_ehr_received_time = sa.Column(UTCDateTime)

    physical_measurements_time = sa.Column(UTCDateTime)
    weight_physical_measurements_time = sa.Column(UTCDateTime)
    height_physical_measurements_time = sa.Column(UTCDateTime)

    is_pediatric_participant = sa.Column(sa.Boolean)
    has_linked_guardian_account = sa.Column(sa.Boolean)


sa.event.listen(EnrollmentDependencies, 'before_insert', model_insert_listener)
sa.event.listen(EnrollmentDependencies, 'before_update', model_update_listener)
