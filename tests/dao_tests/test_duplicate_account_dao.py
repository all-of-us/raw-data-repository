from datetime import datetime

from rdr_service.clock import FakeClock
from rdr_service.dao.duplicate_account_dao import (
    DuplicateAccountDao, DuplicateExistsException, DuplicateAccountChainError, RecordNotFound
)
from rdr_service.model.duplicate_account import (
    DuplicateAccount, DuplicationSource, DuplicationStatus, PrimaryParticipantIndication
)
from tests.helpers.unittest_base import BaseTestCase


class DuplicateAccountDaoTest(BaseTestCase):
    def setUp(self, *args, **kwargs) -> None:
        super().setUp(*args, **kwargs)
        self.first_participant = self.data_generator.create_database_participant()
        self.second_participant = self.data_generator.create_database_participant()

    def test_storing_duplicate_account(self):
        authored_timestamp = datetime(2018, 2, 8)
        created_timestamp = datetime(2020, 9, 20)

        with FakeClock(created_timestamp):
            DuplicateAccountDao.store_duplication(
                participant_a_id=self.first_participant.participantId,
                participant_b_id=self.second_participant.participantId,
                session=self.session,
                authored=authored_timestamp,
                source=DuplicationSource.SUPPORT_TICKET,
                status=DuplicationStatus.APPROVED
            )
            self.session.commit()

        duplicate_record: DuplicateAccount = self.session.query(DuplicateAccount).one()
        self.assertEqual(created_timestamp, duplicate_record.created)
        self.assertEqual(created_timestamp, duplicate_record.modified)
        self.assertEqual(self.first_participant.participantId, duplicate_record.participant_a_id)
        self.assertEqual(self.second_participant.participantId, duplicate_record.participant_b_id)
        self.assertIsNone(duplicate_record.primary_participant)
        self.assertEqual(authored_timestamp, duplicate_record.authored)
        self.assertEqual(DuplicationStatus.APPROVED, duplicate_record.status)

    def test_error_when_already_exists(self):
        """If we try to save an existing record, nothing should be stored and an error should be raised"""
        existing_duplicate = DuplicateAccount(
            participant_a_id=self.first_participant.participantId,
            participant_b_id=self.second_participant.participantId,
            authored=datetime(2020, 1, 1),
            status=DuplicationStatus.POTENTIAL,
            source=DuplicationSource.RDR
        )
        self.session.add(existing_duplicate)
        self.session.commit()

        with self.assertRaises(DuplicateExistsException) as context:
            DuplicateAccountDao.store_duplication(
                participant_a_id=self.second_participant.participantId,
                participant_b_id=self.first_participant.participantId,
                session=self.session,
                authored=datetime.now(),
                source=DuplicationSource.SUPPORT_TICKET,
                status=DuplicationStatus.APPROVED
            )

        self.assertEqual(existing_duplicate, context.exception.existing_record)

    def test_chaining_errors(self):
        """
        An account can't be the primary account in a new pair if it is the secondary account in an existing pair.
        And the reverse is also not allowed: an account can't be the secondary account in a new pair if it is the
        primary account in an existing pair.
        """

        existing_duplicate = DuplicateAccount(
            participant_a_id=self.first_participant.participantId,
            participant_b_id=self.second_participant.participantId,
            authored=datetime(2020, 1, 1),
            status=DuplicationStatus.POTENTIAL,
            source=DuplicationSource.RDR,
            primary_participant=PrimaryParticipantIndication.PARTICIPANT_A
        )
        self.session.add(existing_duplicate)
        self.session.commit()

        with self.assertRaises(DuplicateAccountChainError) as error_content:
            DuplicateAccountDao.store_duplication(
                participant_a_id=self.data_generator.create_database_participant_summary().participantId,
                participant_b_id=self.first_participant.participantId,
                session=self.session,
                authored=datetime.now(),
                source=DuplicationSource.SUPPORT_TICKET,
                primary_account=PrimaryParticipantIndication.PARTICIPANT_A
            )
        self.assertEqual(
            f'P{self.first_participant.participantId} can\'t be a secondary account because it is listed as '
            f'a primary account in duplicate-pair with id "{existing_duplicate.id}"',
            str(error_content.exception)
        )

        with self.assertRaises(DuplicateAccountChainError) as error_content:
            DuplicateAccountDao.store_duplication(
                participant_a_id=self.data_generator.create_database_participant_summary().participantId,
                participant_b_id=self.second_participant.participantId,
                session=self.session,
                authored=datetime.now(),
                source=DuplicationSource.SUPPORT_TICKET,
                primary_account=PrimaryParticipantIndication.PARTICIPANT_B
            )
        self.assertEqual(
            f'P{self.second_participant.participantId} can\'t be a primary account because it is listed as '
            f'a secondary account in duplicate-pair with id "{existing_duplicate.id}"',
            str(error_content.exception)
        )

    def test_updating_duplicate_account(self):
        authored_timestamp = datetime(2020, 1, 1)
        existing_duplicate = DuplicateAccount(
            participant_a_id=self.first_participant.participantId,
            participant_b_id=self.second_participant.participantId,
            authored=authored_timestamp,
            status=DuplicationStatus.POTENTIAL,
            source=DuplicationSource.RDR
        )
        self.session.add(existing_duplicate)
        self.session.commit()

        DuplicateAccountDao.update_duplication(
            participant_a_id=self.second_participant.participantId,
            participant_b_id=self.first_participant.participantId,
            session=self.session,
            status=DuplicationStatus.APPROVED
        )
        self.session.commit()

        self.session.refresh(existing_duplicate)
        self.assertEqual(self.first_participant.participantId, existing_duplicate.participant_a_id)
        self.assertEqual(self.second_participant.participantId, existing_duplicate.participant_b_id)
        self.assertEqual(authored_timestamp, existing_duplicate.authored)
        self.assertEqual(DuplicationStatus.APPROVED, existing_duplicate.status)
        self.assertEqual(DuplicationSource.RDR, existing_duplicate.source)

    def test_update_not_found(self):
        with self.assertRaises(RecordNotFound):
            DuplicateAccountDao.update_duplication(
                participant_a_id=self.second_participant.participantId,
                participant_b_id=self.first_participant.participantId,
                session=self.session,
                status=DuplicationStatus.APPROVED
            )
