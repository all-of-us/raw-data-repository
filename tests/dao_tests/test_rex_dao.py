from datetime import datetime

from rdr_service.dao.rex_dao import RexStudyDao
from rdr_service.clock import FakeClock
from rdr_service.model.rex import Study, ParticipantMapping
from rdr_service.model.participant import Participant as RdrParticipant
from rdr_service.dao.participant_dao import ParticipantDao as RdrParticipantDao
from rdr_service.dao.study_nph_dao import NphParticipantDao
from rdr_service.dao.rex_dao import RexStudyDao, RexParticipantMappingDao
from rdr_service.model.study_nph import Participant as NphParticipant
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
            "schema_name": "ancilliary_study_1",
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
            "schema_name": "ancilliary_study_1",
            "prefix": int(1E2+5E4)
        }
        expected_rex_study_ = Study(**expected_rex_study)
        self.assertEqual(expected_rex_study_.asdict(), self.rex_study_dao.get(1).asdict())

    def tearDown(self):
        self.clear_table_after_test("rex.study")


class RexParticipantMappingDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.rdr_participant_dao = RdrParticipantDao()
        self.nph_participant_dao = NphParticipantDao()
        self.rex_study_dao = RexStudyDao()
        self.rex_participant_mapping_dao = RexParticipantMappingDao()

    def test_get_before_insert(self):
        self.assertIsNone(self.rex_participant_mapping_dao.get(1))

    def _create_nph_participant(self, participant_id: int) -> NphParticipant:
        nph_participant_params = {
            "id": participant_id,
            "ignore_flag": 0,
            "disable_flag": 0,
            "disable_reason": "N/A",
            "biobank_id": 1E7,
            "research_id": 1E7
        }
        nph_participant = NphParticipant(**nph_participant_params)
        with FakeClock(TIME):
            return self.nph_participant_dao.insert(nph_participant)

    def _create_rex_study(self, schema_name: str) -> Study:
        rex_study_params = {
            "ignore_flag": 0,
            "schema_name": schema_name,
            "prefix": 1E2+5E4
        }
        rex_study = Study(**rex_study_params)
        with FakeClock(TIME):
            return self.rex_study_dao.insert(rex_study)

    def _create_rdr_participant(self, participantId: int, biobankId: int) -> RdrParticipant:
        participant = RdrParticipant(participantId=participantId, biobankId=biobankId)
        return self.rdr_participant_dao.insert(participant)

    def test_insert_participantmapping(self):
        _time = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)

        primary_rex_study: Study = self._create_rex_study(schema_name="primary_rex_study")
        ancillary_rex_study: Study = self._create_rex_study(schema_name="ancillary_rex_study")

        primary_participant: RdrParticipant = (
            self._create_rdr_participant(participantId=123, biobankId=555)
        )
        ancillary_participant: NphParticipant = self._create_nph_participant(participant_id=1)

        rex_participant_mapping_params = {
            "created": _time,
            "modified": _time,
            "ignore_flag": 0,
            "primary_study_id": primary_rex_study.id,
            "ancillary_study_id": ancillary_rex_study.id,
            "primary_participant_id": primary_participant.participantId,
            "ancillary_participant_id": ancillary_participant.id,
        }
        rex_participant_mapping = ParticipantMapping(**rex_participant_mapping_params)
        with FakeClock(_time):
            self.rex_participant_mapping_dao.insert(rex_participant_mapping)

        expected_rex_participant_mapping = {
            "id": 1,
            "created": _time,
            "modified": _time,
            "ignore_flag": 0,
            "primary_study_id": primary_rex_study.id,
            "ancillary_study_id": ancillary_rex_study.id,
            "primary_participant_id": primary_participant.participantId,
            "ancillary_participant_id": ancillary_participant.id,
        }
        expected_rex_participant_mapping_ = ParticipantMapping(**expected_rex_participant_mapping)
        self.assertEqual(expected_rex_participant_mapping_.asdict(), self.rex_participant_mapping_dao.get(1).asdict())

    def test_get_from_ancillary_id(self):
        primary_rex_study: Study = self._create_rex_study(schema_name="primary_rex_study")
        ancillary_rex_study: Study = self._create_rex_study(schema_name="ancillary_rex_study")
        participant_mapping_data = ParticipantMapping(
            primary_study_id=primary_rex_study.id,
            ancillary_study_id=ancillary_rex_study.id,
            primary_participant_id=101,
            ancillary_participant_id=10032
        )
        self.rex_participant_mapping_dao.insert(participant_mapping_data)

        participant_mapping = self.rex_participant_mapping_dao.get_from_ancillary_id(primary_rex_study.id,
                                                                            ancillary_rex_study.id,
                                                                            10032)

        self.assertEqual(101, participant_mapping.primary_participant_id)

        participant_mapping = self.rex_participant_mapping_dao.get_from_ancillary_id(primary_rex_study.id,
                                                                            ancillary_rex_study.id,
                                                                            32110)
        self.assertIsNone(participant_mapping)

    def tearDown(self):
        self.clear_table_after_test("rex.participant_mapping")
        self.clear_table_after_test("rex.study")
        self.clear_table_after_test("nph.participant")
