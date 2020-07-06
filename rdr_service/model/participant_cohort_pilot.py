from sqlalchemy import (
    Column, Date, DateTime, ForeignKey, Integer, SmallInteger, event, Index
)

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum
from rdr_service.participant_enums import (ParticipantCohort, ParticipantCohortPilotFlag)

# DA-1622:  To retain participant cohort designation details provided by PTSC
# Originally implemented to determine the Genomics cohort 2 pilot participants
class ParticipantCohortPilot(Base):
    """ Participant cohort and pilot designations  """

    __tablename__ = "participant_cohort_pilot"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", DateTime, nullable=True, index=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", DateTime, nullable=True)
    # endpoint

    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"))
    consentDate = Column("consent_date", Date)
    enrollmentStatusCoreStoredSampleDate = Column("enrollment_status_core_stored_sample_date", Date)
    cluster = Column("cluster", SmallInteger)
    participantCohort = Column(
        "participant_cohort", Enum(ParticipantCohort), default=ParticipantCohort.UNSET
    )
    participantCohortPilot = Column(
        "participant_cohort_pilot", Enum(ParticipantCohortPilotFlag), default=ParticipantCohortPilotFlag.UNSET
    )


Index("participant_cohort_participantId", ParticipantCohortPilot.participantId, unique=False)

event.listen(ParticipantCohortPilot, "before_insert", model_insert_listener)
event.listen(ParticipantCohortPilot, "before_update", model_update_listener)
