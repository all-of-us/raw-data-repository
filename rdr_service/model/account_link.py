import sqlalchemy as sa
from sqlalchemy.orm import relationship

from rdr_service.clock import CLOCK
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime


class AccountLink(Base):
    """
    Many-to-many relationship between participant accounts. Put in place to store relationships between
    pediatric participants and their guardians, but may have other uses.
    """

    __tablename__ = 'account_link'
    id = sa.Column(sa.BIGINT, primary_key=True, autoincrement=True, nullable=False)
    created = sa.Column(UTCDateTime, nullable=False)
    modified = sa.Column(UTCDateTime, nullable=False)
    start = sa.Column(UTCDateTime)
    """
    Timestamp of when the relationship starts being applicable. May be null to denote that it
    always was (or that we simply don't know the 'start' time for it).
    """
    end = sa.Column(UTCDateTime)
    """
    Timestamp of when the relationship stops being applicable. May be null to denote that it
    always will be (or that we simply don't know the 'end' time for it).
    """
    participant_id = sa.Column(sa.Integer, sa.ForeignKey('participant.participant_id'))
    """
    The first participant this relationship definition focuses on.
    In a child-guardian relationship, this would be the child's participant id.
    """
    related_id = sa.Column(sa.Integer, sa.ForeignKey('participant.participant_id'))
    """
    The second participant this relationship definition focuses on.
    In a child-guardian relationship, this would be the guardian's participant id.
    """

    participant = relationship('Participant', foreign_keys=participant_id)
    related = relationship('Participant', foreign_keys=related_id)

    @classmethod
    def get_active_filter(cls):
        now_datetime = CLOCK.now()
        return sa.and_(
            sa.or_(AccountLink.start.is_(None), AccountLink.start < now_datetime),
            sa.or_(AccountLink.end.is_(None), AccountLink.end > now_datetime)
        )


sa.event.listen(AccountLink, 'before_insert', model_insert_listener)
sa.event.listen(AccountLink, 'before_update', model_update_listener)
