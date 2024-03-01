from datetime import datetime

from rdr_service.clock import FakeClock
from rdr_service.dao.duplicate_account_dao import DuplicateAccountDao, DuplicateExistsException
from rdr_service.model.duplicate_account import DuplicateAccount, DuplicationSource, DuplicationStatus
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
