from enum import Enum

import sqlalchemy as sa

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime


class PrimaryParticipantIndication(Enum):
    PARTICIPANT_A = 1
    PARTICIPANT_B = 2


class DuplicationStatus(Enum):
    POTENTIAL = 1
    APPROVED = 2
    REJECTED = 3


class DuplicationSource(Enum):
    RDR = 1
    SUPPORT_TICKET = 2
    VIBRENT = 3


class DuplicateAccount(Base):
    __tablename__ = 'duplicate_accounts'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True, nullable=False)
    created = sa.Column(UTCDateTime, nullable=False)
    modified = sa.Column(UTCDateTime, nullable=False)
    participant_a_id = sa.Column(
        sa.Integer, sa.ForeignKey('participant.participant_id'), nullable=False, index=True
    )
    participant_b_id = sa.Column(
        sa.Integer, sa.ForeignKey('participant.participant_id'), nullable=False, index=True
    )
    primary_participant = sa.Column(sa.Enum(PrimaryParticipantIndication))
    authored = sa.Column(UTCDateTime, nullable=False)
    status = sa.Column(sa.Enum(DuplicationStatus), nullable=False)
    source = sa.Column(sa.Enum(DuplicationSource), nullable=False)

    def get_other_participant_id(self, participant_id: int):
        """Convenience method for getting the other participant id from the pair."""
        return self.participant_a_id if self.participant_a_id != participant_id else self.participant_b_id

    def get_primary_id(self):
        """Convenience method for getting the participant id of the primary account"""
        if self.primary_participant is None:
            return None
        elif self.primary_participant == PrimaryParticipantIndication.PARTICIPANT_A:
            return self.participant_a_id
        elif self.primary_participant == PrimaryParticipantIndication.PARTICIPANT_B:
            return self.participant_b_id


sa.event.listen(DuplicateAccount, 'before_insert', model_insert_listener)
sa.event.listen(DuplicateAccount, 'before_update', model_update_listener)
