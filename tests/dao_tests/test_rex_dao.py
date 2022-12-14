from datetime import datetime

from rdr_service.dao.rex_dao import RexStudyDao
from rdr_service.clock import FakeClock
from rdr_service.model.rex import Study
from tests.helpers.unittest_base import BaseTestCase


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)


class RexStudyDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.rex_study_dao = RexStudyDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.rex_study_dao.get(1))

    def test_insert_study(self):
        rex_study_params = {
            "ignore_flag": 0,
            "name": "Study 1",
            "prefix": 1E2+5E4
        }
        rex_study = Study(**rex_study_params)
        with FakeClock(TIME):
            self.rex_study_dao.insert(rex_study)

        expected_rex_study = {
            "id": 1,
            "created": TIME,
            "modified": TIME,
            "ignore_flag": 0,
            "name": "Study 1",
            "prefix": int(1E2+5E4)
        }
        expected_rex_study_ = Study(**expected_rex_study)
        self.assertEqual(expected_rex_study_.asdict(), self.rex_study_dao.get(1).asdict())
