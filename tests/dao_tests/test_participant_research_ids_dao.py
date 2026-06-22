import datetime

from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.mysql_helper_data import random_ids

from rdr_service.clock import FakeClock
from rdr_service.dao.participant_research_ids_dao import ParticipantResearchIdsDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.participant import Participant


class ParticipantResearchIdsDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao = ParticipantResearchIdsDao()
        self.p_dao = ParticipantDao()
        p = Participant(externalId=99)
        time = datetime.datetime(2025, 4, 10)
        with random_ids([1, 2, 3]):
            with FakeClock(time):
                self.p_dao.insert(p)

    def test_get_new_participants(self):
        new_participants = self.dao.get_new_participants()
        self.assertEqual(len(new_participants), 1)
        self.assertEqual(new_participants[0].participantId, 1)

    def test_insert_new_participants(self):
        new_participants = self.dao.get_new_participants()
        self.dao.insert_new_participants(new_participants)

        re_check_participants = self.dao.get_new_participants()
        self.assertEqual(len(re_check_participants), 0)
        research_ids = self.dao.get(1)
        self.assertEqual(new_participants[0].researchId, research_ids.research_id)
        self.assertEqual(new_participants[0].externalId, research_ids.external_id)
        self.assertIsNotNone(research_ids.registered_tier_id)
        self.assertIsNotNone(research_ids.controlled_tier_plus_id)

    def test_research_id_range(self):
        _MIN_REGISTERED_TIER_RESEARCH_ID = 200001000000
        _MAX_REGISTERED_TIER_RESEARCH_ID = 200009999999
        _MIN_CONTROLLED_TIER_PLUS_RESEARCH_ID = 300001000000
        _MAX_CONTROLLED_TIER_PLUS_RESEARCH_ID = 300009999999
        participant = Participant(participantId=10, biobankId=20)
        self.p_dao.insert(participant)
        self.dao.insert_new_participants([participant])
        research_ids = self.dao.get(10)
        self.assertGreaterEqual(research_ids.registered_tier_id, _MIN_REGISTERED_TIER_RESEARCH_ID)
        self.assertLessEqual(research_ids.registered_tier_id, _MAX_REGISTERED_TIER_RESEARCH_ID)
        self.assertGreaterEqual(research_ids.controlled_tier_plus_id, _MIN_CONTROLLED_TIER_PLUS_RESEARCH_ID)
        self.assertLessEqual(research_ids.controlled_tier_plus_id, _MAX_CONTROLLED_TIER_PLUS_RESEARCH_ID)
