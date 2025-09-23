from sqlalchemy import Column, Integer, ForeignKey, String
from rdr_service.model.base import Base


class ParticipantResearchIds(Base):
    """ Table holding participant research ids """
    __tablename__ = 'participant_research_ids'
    participant_id = Column(
        "participant_id", Integer, ForeignKey("participant.participant_id"), primary_key=True, autoincrement=False
    )
    research_id = Column("research_id", Integer, unique=True)
    external_id = Column("external_id", String)
    controlled_tier_id = Column('controlled_tier_id', Integer, unique=True)
    registered_tier_id = Column('registered_tier_id', Integer, unique=True)
    controlled_tier_plus_id = Column('controlled_tier_plus_id', Integer, unique=True)
