from rdr_service.dao.rex_dao import RexStudyDao
# from rdr_service.model.rex import Study
from tests.helpers.unittest_base import BaseTestCase


class NphParticipantDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.rex_study_dao = RexStudyDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.rex_study_dao.get(1))
