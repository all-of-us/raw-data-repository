from sqlalchemy import (
    Column, DateTime, ForeignKey, Integer,
    String, SmallInteger, event,
)

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.participant import Participant
from rdr_service.model.site import Site


class ParticipantIncentives(Base):
    """
    Participant Incentives model
    """

    __tablename__ = 'participant_incentives'

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", DateTime)
    modified = Column("modified", DateTime)
    participant_id = Column(Integer, ForeignKey(Participant.participantId), index=True)
    created_by = Column(String(255))
    site = Column(Integer, ForeignKey(Site.siteId))
    date_given = Column(String(255))
    incentive_type = Column(String(255))
    amount = Column(SmallInteger)
    occurrence = Column(String(255))
    giftcard_type = Column(String(255), nullable=True)
    notes = Column(String(512), nullable=True)
    cancelled = Column(SmallInteger, nullable=False, default=0)
    cancelled_by = Column(String(255), nullable=True)
    cancelled_date = Column(String(255), nullable=True)


event.listen(ParticipantIncentives, "before_insert", model_insert_listener)
event.listen(ParticipantIncentives, "before_update", model_update_listener)
