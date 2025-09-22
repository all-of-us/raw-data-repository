from tests.helpers.unittest_base import BaseTestCase
from rdr_service.dao.participant_research_ids_dao import ParticipantResearchIdsDao
from rdr_service.model.participant_research_ids import ParticipantResearchIds


class ParticipantResearchIdsDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()

    def test_get_new_participants(self):
        pri_dao = ParticipantResearchIdsDao()
        new_participants = pri_dao.get_new_participants()
        self.assertIsNotNone(new_participants)

    def test_insert_new_participants(self):
        pri_dao = ParticipantResearchIdsDao()
        new_participants = pri_dao.get_new_participants()
        pri_dao.insert_new_participants(new_participants)

        re_check_participants = pri_dao.get_new_participants()
        self.assertListEqual(re_check_participants, [])
