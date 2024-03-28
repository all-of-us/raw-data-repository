from datetime import datetime
from typing import Iterable

import sqlalchemy as sa
from sqlalchemy.orm import Session

from rdr_service.model.duplicate_account import (
    DuplicateAccount, DuplicationSource, DuplicationStatus, PrimaryParticipantIndication
)
from rdr_service.model.participant_summary import ParticipantSummary


class DuplicateExistsException(Exception):
    def __init__(self, existing_record: DuplicateAccount):
        self.existing_record = existing_record


class DuplicateAccountDao:
    @classmethod
    def store_duplication(
        cls, participant_a_id: int, participant_b_id: int, session: Session, authored: datetime,
        source: DuplicationSource, status: DuplicationStatus = DuplicationStatus.POTENTIAL,
        primary_account: PrimaryParticipantIndication = None
    ):
        existing_record = session.query(DuplicateAccount).filter(
            sa.or_(
                sa.and_(
                    DuplicateAccount.participant_a_id == participant_a_id,
                    DuplicateAccount.participant_b_id == participant_b_id
                ),
                sa.and_(
                    DuplicateAccount.participant_a_id == participant_b_id,
                    DuplicateAccount.participant_b_id == participant_a_id
                )
            )
        ).one_or_none()
        if existing_record:
            raise DuplicateExistsException(existing_record)

        new_record = DuplicateAccount(
            participant_a_id=participant_a_id,
            participant_b_id=participant_b_id,
            authored=authored,
            status=status,
            source=source
        )
        if primary_account is not None:
            new_record.primary_participant = primary_account
        session.add(new_record)

    @classmethod
    def query_participant_duplication_data(cls, session) -> Iterable[ParticipantSummary]:
        """Load participant summary data used for finding duplicate accounts"""
        return session.query(
            ParticipantSummary.participantId,
            ParticipantSummary.firstName,
            ParticipantSummary.lastName,
            ParticipantSummary.dateOfBirth,
            ParticipantSummary.email,
            ParticipantSummary.loginPhoneNumber
        ).yield_per(1000)

    @classmethod
    def query_participants_to_check(cls, since: datetime, session: Session) -> Iterable[ParticipantSummary]:
        """Load participant summary data for accounts that should be checked for duplication"""
        return session.query(
            ParticipantSummary.participantId,
            ParticipantSummary.firstName,
            ParticipantSummary.lastName,
            ParticipantSummary.dateOfBirth,
            ParticipantSummary.email,
            ParticipantSummary.loginPhoneNumber
        ).filter(
            ParticipantSummary.lastModified > since
        ).all()
