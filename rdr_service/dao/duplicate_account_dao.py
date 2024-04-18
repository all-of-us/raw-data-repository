from datetime import datetime
from typing import Iterable, Optional

import sqlalchemy as sa
from sqlalchemy.orm import Session

from rdr_service.model.duplicate_account import (
    DuplicateAccount, DuplicationSource, DuplicationStatus, PrimaryParticipantIndication
)
from rdr_service.model.participant_summary import ParticipantSummary


class DuplicateExistsException(Exception):
    def __init__(self, existing_record: DuplicateAccount):
        self.existing_record = existing_record


class DuplicateAccountChainError(Exception):
    ...

class RecordNotFound(Exception):
    ...


class DuplicateAccountDao:
    @classmethod
    def store_duplication(
        cls, participant_a_id: int, participant_b_id: int, session: Session, authored: datetime,
        source: DuplicationSource, status: DuplicationStatus = DuplicationStatus.POTENTIAL,
        primary_account: PrimaryParticipantIndication = None
    ):
        existing_record = cls._get_existing_record(participant_a_id, participant_b_id, session)
        if existing_record:
            raise DuplicateExistsException(existing_record)

        primary_id = None
        secondary_id = None
        if primary_account == PrimaryParticipantIndication.PARTICIPANT_A:
            primary_id = participant_a_id
            secondary_id = participant_b_id
        elif primary_account == PrimaryParticipantIndication.PARTICIPANT_B:
            primary_id = participant_b_id
            secondary_id = participant_a_id
        if primary_id:
            conflicting_duplication = cls._any_pairs_with_participant_as_primary(secondary_id, session)
            if conflicting_duplication:
                raise DuplicateAccountChainError(
                    f'P{secondary_id} can\'t be a secondary account because it is listed as a primary account in '
                    f'duplicate-pair with id "{conflicting_duplication.id}"'
                )
            conflicting_duplication = cls._any_pairs_with_participant_as_secondary(primary_id, session)
            if conflicting_duplication:
                raise DuplicateAccountChainError(
                    f'P{primary_id} can\'t be a primary account because it is listed as a secondary account in '
                    f'duplicate-pair with id "{conflicting_duplication.id}"'
                )

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
    def update_duplication(
        cls, participant_a_id: int, participant_b_id: int, session: Session, **kwargs
    ):
        existing_record = cls._get_existing_record(participant_a_id, participant_b_id, session)
        if not existing_record:
            raise RecordNotFound()

        if 'authored' in kwargs:
            existing_record.authored = kwargs['authored']
        if 'source' in kwargs:
            existing_record.source = kwargs['source']
        if 'status' in kwargs:
            existing_record.status = kwargs['status']
        if 'primary_account' in kwargs:
            existing_record.primary_participant = kwargs['primary_account']

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

    @classmethod
    def _any_pairs_with_participant_as_primary(cls, participant_id, session) -> DuplicateAccount:
        result = session.query(DuplicateAccount).filter(
            sa.or_(
                sa.and_(
                    DuplicateAccount.participant_a_id == participant_id,
                    DuplicateAccount.primary_participant == PrimaryParticipantIndication.PARTICIPANT_A
                ),
                sa.and_(
                    DuplicateAccount.participant_b_id == participant_id,
                    DuplicateAccount.primary_participant == PrimaryParticipantIndication.PARTICIPANT_B
                )
            )
        ).first()
        return result

    @classmethod
    def _any_pairs_with_participant_as_secondary(cls, participant_id, session) -> DuplicateAccount:
        result = session.query(DuplicateAccount).filter(
            sa.or_(
                sa.and_(
                    DuplicateAccount.participant_a_id == participant_id,
                    DuplicateAccount.primary_participant == PrimaryParticipantIndication.PARTICIPANT_B
                ),
                sa.and_(
                    DuplicateAccount.participant_b_id == participant_id,
                    DuplicateAccount.primary_participant == PrimaryParticipantIndication.PARTICIPANT_A
                )
            )
        ).first()
        return result

    @classmethod
    def _get_existing_record(
        cls, participant_a_id: int, participant_b_id: int, session: Session
    ) -> Optional[DuplicateAccount]:
        return session.query(DuplicateAccount).filter(
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
