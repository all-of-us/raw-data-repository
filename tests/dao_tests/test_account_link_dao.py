from datetime import datetime

from rdr_service.clock import FakeClock
from rdr_service.dao.account_link_dao import AccountLinkDao
from rdr_service.model.account_link import AccountLink
from tests.helpers.unittest_base import BaseTestCase


class AccountLinkDaoTest(BaseTestCase):
    def test_getting_links_for_participant(self):
        first_parent_id = self.data_generator.create_database_participant().participantId
        second_parent_id = self.data_generator.create_database_participant().participantId
        child_id = self.data_generator.create_database_participant().participantId

        # Link the parents to the child
        AccountLinkDao.save_account_link(
            account_link=AccountLink(participant_id=child_id, related_id=first_parent_id),
            session=self.session
        )
        AccountLinkDao.save_account_link(
            account_link=AccountLink(participant_id=child_id, related_id=second_parent_id),
            session=self.session
        )

        results = AccountLinkDao.get_linked_ids(child_id, session=self.session)
        self.assertEqual({first_parent_id, second_parent_id}, results)

    def test_start_and_end_dates(self):
        first_parent_id = self.data_generator.create_database_participant().participantId
        second_parent_id = self.data_generator.create_database_participant().participantId
        child_id = self.data_generator.create_database_participant().participantId

        # Link the parents to the child
        AccountLinkDao.save_account_link(
            account_link=AccountLink(
                participant_id=child_id, related_id=first_parent_id,
                start=datetime(2020, 10, 1)
            ),
            session=self.session
        )
        AccountLinkDao.save_account_link(
            account_link=AccountLink(
                participant_id=child_id, related_id=second_parent_id,
                end=datetime(2020, 11, 1)
            ),
            session=self.session
        )

        # check when the start date hasn't started yet
        with FakeClock(datetime(2020, 9, 1)):
            results = AccountLinkDao.get_linked_ids(child_id, session=self.session)
            self.assertEqual({second_parent_id}, results)

        # check when they're both active
        with FakeClock(datetime(2020, 10, 17)):
            results = AccountLinkDao.get_linked_ids(child_id, session=self.session)
            self.assertEqual({first_parent_id, second_parent_id}, results)

        # check when the end date has ended
        with FakeClock(datetime(2020, 12, 1)):
            results = AccountLinkDao.get_linked_ids(child_id, session=self.session)
            self.assertEqual({first_parent_id}, results)

    def test_linking_sets_modified_time(self):
        pediatric_summary = self.data_generator.create_database_participant_summary()
        guardian_id = self.data_generator.create_database_participant().participantId

        timestamp = datetime(2023, 4, 8)
        with FakeClock(timestamp):
            AccountLinkDao.save_account_link(
                account_link=AccountLink(participant_id=pediatric_summary.participantId, related_id=guardian_id),
                session=self.session
            )

        self.assertEqual(timestamp, pediatric_summary.lastModified)

    def test_upsert(self):
        """Saving a link should update the current data if one already exists"""
        parent_id = self.data_generator.create_database_participant().participantId
        child_id = self.data_generator.create_database_participant().participantId

        # Create the initial link
        AccountLinkDao.save_account_link(
            account_link=AccountLink(
                participant_id=child_id, related_id=parent_id,
                start=datetime(2020, 10, 1)
            )
        )

        # Make an update
        saved_end_date = datetime(2021, 12, 1)
        AccountLinkDao.save_account_link(
            account_link=AccountLink(
                participant_id=child_id, related_id=parent_id,
                start=datetime(2020, 10, 1),
                end=saved_end_date
            )
        )

        account_link = self.session.query(AccountLink).filter(
            AccountLink.participant_id == child_id,
            AccountLink.related_id == parent_id
        ).one()
        self.assertEqual(saved_end_date, account_link.end)
