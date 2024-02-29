from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.orm import Session

from rdr_service.model.duplicate_account import DuplicateAccount, DuplicationSource, DuplicationStatus


class DuplicateExistsException(Exception):
    def __init__(self, existing_record: DuplicateAccount):
        self.existing_record = existing_record


class DuplicateAccountDao:
    @classmethod
    def store_duplication(
        cls, participant_a_id: int, participant_b_id: int, session: Session, authored: datetime,
        source: DuplicationSource, status: DuplicationStatus = DuplicationStatus.POTENTIAL
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

        session.add(
            DuplicateAccount(
                participant_a_id=participant_a_id,
                participant_b_id=participant_b_id,
                authored=authored,
                status=status,
                source=source
            )
        )
