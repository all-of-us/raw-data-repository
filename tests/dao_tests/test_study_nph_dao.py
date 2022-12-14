from rdr_service.dao.study_nph_dao import NphParticipantDao
# from rdr_service.model.study_nph import Participant
from tests.helpers.unittest_base import BaseTestCase


class NphParticipantDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.nph_participant_dao = NphParticipantDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.nph_participant_dao.get(1))
