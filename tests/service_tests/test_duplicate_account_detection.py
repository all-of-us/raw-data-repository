from datetime import date, datetime
import mock

from rdr_service.dao.duplicate_account_dao import DuplicateExistsException
from rdr_service.model.duplicate_account import DuplicationSource
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.duplicate_detection import DuplicateDetection
from tests.helpers.unittest_base import BaseTestCase


class DuplicateDetectionTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super().setUp(*args, **kwargs)
        self.all_participants_mock = self.mock(
            'rdr_service.dao.duplicate_account_dao.DuplicateAccountDao.query_participant_duplication_data'
        )
        self.participants_to_check_mock = self.mock(
            'rdr_service.dao.duplicate_account_dao.DuplicateAccountDao.query_participants_to_check'
        )
        self.save_duplicate_mock = self.mock(
            'rdr_service.dao.duplicate_account_dao.DuplicateAccountDao.store_duplication'
        )

    def test_detecting_name_and_dob(self):
        first_sam = ParticipantSummary(
            firstName='Sam', lastName='Smith', dateOfBirth=date(2020, 3, 2),
            email="firstsam@msn.com", participantId=1
        )
        second_sam = ParticipantSummary(
            firstName='Sam', lastName='Smith', dateOfBirth=date(2020, 3, 2),
            email="secondsam@gmail.com", participantId=2
        )

        self.all_participants_mock.return_value = [
            first_sam,
            second_sam,
            # more that shouldn't match
            ParticipantSummary(
                firstName='Sam', lastName='Smith', dateOfBirth=date(1991, 3, 2), participantId=3
            ),
            ParticipantSummary(
                firstName='Alice', lastName='Smith', dateOfBirth=date(2020, 3, 2), participantId=4
            ),
            ParticipantSummary(
                firstName='Sam', lastName='Smyth', dateOfBirth=date(1991, 3, 2), participantId=5
            )
        ]
        self.participants_to_check_mock.return_value = [second_sam]

        DuplicateDetection.find_duplicates(datetime.now(), session=mock.MagicMock())
        self.assertEqual(1, self.save_duplicate_mock.call_count)

        saved_record = self.save_duplicate_mock.call_args_list[0].kwargs
        self.assertEqual(second_sam.participantId, saved_record['participant_a_id'])
        self.assertEqual(first_sam.participantId, saved_record['participant_b_id'])
        self.assertEqual(DuplicationSource.RDR, saved_record['source'])

    def test_detecting_email(self):
        johnson_account = ParticipantSummary(
            firstName='Fred', lastName='Johnson', dateOfBirth=date(2020, 3, 2),
            email="tycho@belt.net", participantId=1
        )
        drummer_account = ParticipantSummary(
            firstName='Karina', lastName='Drummer', dateOfBirth=date(2020, 3, 2),
            email="tycho@belt.net", participantId=2
        )
        sam_account = ParticipantSummary(
            firstName='Sam', lastName='Test', dateOfBirth=date(2020, 3, 2),
            email="another@email.org", participantId=3
        )
        foo_account = ParticipantSummary(
            firstName='Foo', lastName='Bar', dateOfBirth=date(2020, 3, 2),
            email="another@email.org", participantId=4
        )

        self.all_participants_mock.return_value = [
            johnson_account, drummer_account, sam_account, foo_account
        ]
        self.participants_to_check_mock.return_value = [johnson_account, foo_account]

        DuplicateDetection.find_duplicates(datetime.now(), session=mock.MagicMock())
        self.assertEqual(2, self.save_duplicate_mock.call_count)

        saved_record = self.save_duplicate_mock.call_args_list[0].kwargs
        self.assertEqual(johnson_account.participantId, saved_record['participant_a_id'])
        self.assertEqual(drummer_account.participantId, saved_record['participant_b_id'])

        saved_record = self.save_duplicate_mock.call_args_list[1].kwargs
        self.assertEqual(foo_account.participantId, saved_record['participant_a_id'])
        self.assertEqual(sam_account.participantId, saved_record['participant_b_id'])

    def test_detecting_login_phone(self):
        johnson_account = ParticipantSummary(
            firstName='Fred', lastName='Johnson', dateOfBirth=date(2020, 3, 2),
            loginPhoneNumber='(123) 456-7890', participantId=1
        )
        drummer_account = ParticipantSummary(
            firstName='Karina', lastName='Drummer', dateOfBirth=date(2020, 3, 2),
            loginPhoneNumber='(123) 456-7890', participantId=2
        )
        sam_account = ParticipantSummary(
            firstName='Sam', lastName='Test', dateOfBirth=date(2020, 3, 2),
            loginPhoneNumber='0987654321', participantId=3
        )
        foo_account = ParticipantSummary(
            firstName='Foo', lastName='Bar', dateOfBirth=date(2020, 3, 2),
            loginPhoneNumber='0987654321', participantId=4
        )

        self.all_participants_mock.return_value = [
            johnson_account, drummer_account, sam_account, foo_account
        ]
        self.participants_to_check_mock.return_value = [johnson_account, foo_account]

        DuplicateDetection.find_duplicates(datetime.now(), session=mock.MagicMock())
        self.assertEqual(2, self.save_duplicate_mock.call_count)

        saved_record = self.save_duplicate_mock.call_args_list[0].kwargs
        self.assertEqual(johnson_account.participantId, saved_record['participant_a_id'])
        self.assertEqual(drummer_account.participantId, saved_record['participant_b_id'])

        saved_record = self.save_duplicate_mock.call_args_list[1].kwargs
        self.assertEqual(foo_account.participantId, saved_record['participant_a_id'])
        self.assertEqual(sam_account.participantId, saved_record['participant_b_id'])

    def test_existing_duplicate_is_ignored(self):
        """DAO throws exception to notify a duplicate is already known, making sure it doesn't crash anything"""
        first_sam = ParticipantSummary(
            firstName='Sam', lastName='Smith', dateOfBirth=date(2020, 3, 2),
            email="firstsam@msn.com", participantId=1
        )
        second_sam = ParticipantSummary(
            firstName='Sam', lastName='Smith', dateOfBirth=date(2020, 3, 2),
            email="secondsam@gmail.com", participantId=2
        )

        self.all_participants_mock.return_value = [
            first_sam, second_sam
        ]
        self.participants_to_check_mock.return_value = [second_sam]

        self.save_duplicate_mock.side_effect = mock.Mock(side_effect=DuplicateExistsException(None))
        DuplicateDetection.find_duplicates(datetime.now(), session=mock.MagicMock())
