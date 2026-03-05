from sqlalchemy import Column, Integer, ForeignKey, BigInteger, event, Computed
from sqlalchemy.sql import func
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime


class ParticipantResearchIds(Base):
    """ Table holding participant research ids """
    __tablename__ = 'participant_research_ids'
    participant_id = Column(
        "participant_id", Integer, ForeignKey("participant.participant_id"), primary_key=True, autoincrement=False
    )
    created = Column(UTCDateTime, nullable=False, server_default=func.now())
    modified = Column(UTCDateTime, nullable=False, server_default=func.now())
    research_id = Column("research_id", Integer, unique=True)
    external_id = Column("external_id", BigInteger)
    registered_tier_id = Column('registered_tier_id', BigInteger, unique=True)

event.listen(ParticipantResearchIds, 'before_insert', model_insert_listener)
event.listen(ParticipantResearchIds, 'before_update', model_update_listener)
