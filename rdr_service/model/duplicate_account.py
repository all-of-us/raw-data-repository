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


sa.event.listen(DuplicateAccount, 'before_insert', model_insert_listener)
sa.event.listen(DuplicateAccount, 'before_update', model_update_listener)


"""
cron job algorithm
    - runs nightly, gets all the participant summaries last modified in the last 28 hours
    - for each of those summaries:
        - compare data to every other participant (#1 below)
        - when done comparing, will have a list of potential duplications, for each duplication:
            - check to see if the duplication (participant_a/participant_b combo) already exists in the db table
            - ignore any that are already there, store any that are new




#1 comparing each newly modified participant to every other participant is going to scale badly
(maybe about 1-3k participants a day, each being compared to about 700k other summaries)
    - before any comparisons, prepare a cache of data needed for comparison
        - load participant ids, names, addresses, contact info (store in dict as key to participant id map)
    - compare new participant info to cached data


DAO
    define ALREADY_EXISTS error
    define method to store duplication
        - check if combo already exists, if it does throw ALREADY_EXISTS error (ticket-tool will handle carefully,
          nightly cron job will ignore)










"""


