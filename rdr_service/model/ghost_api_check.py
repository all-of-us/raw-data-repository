
from protorpc import messages
from sqlalchemy import Column, ForeignKey, Integer

from rdr_service.model.base import Base
from rdr_service.model.participant import Participant
from rdr_service.model.utils import Enum, UTCDateTime


class GhostFlagModification(messages.Enum):
    GHOST_FLAG_SET = 1
    GHOST_FLAG_REMOVED = 2


class GhostApiCheck(Base):
    __tablename__ = 'ghost_api_check'
    id = Column(Integer, primary_key=True, autoincrement=True)
    participant_id = Column(Integer, ForeignKey(Participant.participantId), nullable=False)
    timestamp = Column(UTCDateTime, nullable=False)
    modification_performed = Column(Enum(GhostFlagModification))
