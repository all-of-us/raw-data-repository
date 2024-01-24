from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    func,
    Integer,
    String
)
from rdr_service.model.base import Base

class ParticipantDataToTactis(Base):
    __tablename__ = "participant_data_to_tactis"

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=False)
    action = Column(String(255), nullable=True)
    created = Column(DateTime, nullable=False, default=func.now())
