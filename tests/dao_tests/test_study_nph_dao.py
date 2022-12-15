from datetime import datetime

from rdr_service.dao.study_nph_dao import NphParticipantDao
from rdr_service.clock import FakeClock
from rdr_service.model.study_nph import Participant
from tests.helpers.unittest_base import BaseTestCase


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)


class NphParticipantDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.nph_participant_dao = NphParticipantDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.nph_participant_dao.get(1))

    def test_insert_participant(self):
        nph_participant_params = {
            "id": 1,
            "ignore_flag": 0,
            "disable_flag": 0,
            "disable_reason": "N/A",
            "biobank_id": 1E7,
            "research_id": 1E7
        }
        nph_participant = Participant(**nph_participant_params)
        with FakeClock(TIME):
            self.nph_participant_dao.insert(nph_participant)

        expected_nph_participant = {
            "id": 1,
            "created": TIME,
            "modified": TIME,
            "ignore_flag": 0,
            "disable_flag": 0,
            "disable_reason": "N/A",
            "biobank_id": int(1E7),
            "research_id": int(1E7),
        }
        expected_nph_participant_ = Participant(**expected_nph_participant)
        participant_obj = self.nph_participant_dao.get(1)
        self.assertEqual(self.nph_participant_dao.get_id(participant_obj), 1)
        self.assertEqual(expected_nph_participant_.asdict(), participant_obj.asdict())
