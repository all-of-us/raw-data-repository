from datetime import datetime, timedelta

from rdr_service.dao.ghost_check_dao import GhostCheckDao

from tests.helpers.unittest_base import BaseTestCase

class GhostCheckDaoTest(BaseTestCase):
    def test_loads_only_vibrent(self):
        """We might accidentally start flagging CE participants as ghosts if they're returned"""
        vibrent_participant = self.data_generator.create_database_participant(participantOrigin='vibrent')
        self.data_generator.create_database_participant(participantOrigin='careevolution')
        self.data_generator.create_database_participant(participantOrigin='anotherplatform')

        participants = GhostCheckDao.get_participants_needing_checked(
            session=self.data_generator.session,
            earliest_signup_time=datetime.now() - timedelta(weeks=1)
        )
        self.assertEqual(1, len(participants), 'Should only be the Vibrent participant')
        self.assertEqual(vibrent_participant.participantId, participants[0].participantId)

    def test_ghost_flag_returned(self):
        """Ensure we get back the ghost data field"""
        ghost_participant = self.data_generator.create_database_participant(
            participantOrigin='vibrent',
            isGhostId=True
        )
        self.data_generator.create_database_participant(
            participantOrigin='vibrent',
            isGhostId=None
        )
        self.data_generator.create_database_participant(
            participantOrigin='vibrent',
            isGhostId=False
        )

        results = GhostCheckDao.get_participants_needing_checked(
            session=self.data_generator.session,
            earliest_signup_time=datetime.now() - timedelta(weeks=1)
        )
        for participant in results:
            if participant.participantId == ghost_participant.participantId:
                self.assertTrue(participant.isGhostId)
            else:
                self.assertFalse(participant.isGhostId)

