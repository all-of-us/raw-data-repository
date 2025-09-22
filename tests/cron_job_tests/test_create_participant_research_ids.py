from tests.helpers.unittest_base import BaseTestCase

class TestCreateParticipantResearchIds(BaseTestCase):
    def setUp(self):
        super().setUp()

    def test_create_participant_research_ids(self):
        from rdr_service.offline.create_research_ids import create_participant_research_ids
        self.assertIsNotNone(create_participant_research_ids(), True)
