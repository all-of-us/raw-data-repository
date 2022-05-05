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

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False)
    participantId = Column('participant_id', Integer, ForeignKey(Participant.participantId), index=True, nullable=False)
    createdBy = Column('created_by', String(255), nullable=True)
    incentiveType = Column('incentive_type', String(255), nullable=True)
    site = Column(Integer, ForeignKey(Site.siteId), nullable=False)
    dateGiven = Column('date_given', String(255), nullable=False)
    amount = Column(SmallInteger, nullable=True)
    occurrence = Column(String(255), nullable=True)
    giftcardType = Column('giftcard_type', String(255), nullable=True)
    notes = Column(String(512), nullable=True)
    cancelled = Column(SmallInteger, nullable=False, default=0)
    cancelledBy = Column('cancelled_by', String(255), nullable=True)
    cancelledDate = Column('cancelled_date', String(255), nullable=True)


event.listen(ParticipantIncentives, "before_insert", model_insert_listener)
event.listen(ParticipantIncentives, "before_update", model_update_listener)
