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

    def _create_rex_study(self, name: str) -> Study:
        rex_study_params = {
            "ignore_flag": 0,
            "name": name,
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

        primary_rex_study: Study = self._create_rex_study(name="Primary Study")
        ancillary_rex_study: Study = self._create_rex_study(name="Ancillary Study")

        primary_participant: RdrParticipant = (
            self._create_rdr_participant(participantId=123, biobankId=555)
        )
        ancillary_participant: NphParticipant = self._create_nph_participant(participant_id=1)

        # breakpoint()
        rex_participant_mapping_params = {
            "created": _time,
            "modified": _time,
            "ignore_flag": 0,
            "primary_study_id": primary_rex_study.id,
            "ancillary_study_id": ancillary_rex_study.id,
            "primary_participant_id": primary_participant.participantId,
            "ancillary_participant_id": ancillary_participant.id,
        }
        with FakeClock(_time):
            self.rex_participant_mapping_dao.insert(rex_participant_mapping_params)

        expected_rex_participant_mapping = {
            "id": 1,
            "created": _time,
            "modified": _time,
            "ignore_flag": 0,
            "primary_study_id": primary_rex_study,
            "ancillary_study_id": ancillary_rex_study,
            "primary_participant_id": primary_participant,
            "ancillary_participant_id": ancillary_participant
        }
        expected_rex_participant_mapping_ = ParticipantMapping(**expected_rex_participant_mapping)
        self.assertEqual(expected_rex_participant_mapping_.asdict(), self.rex_participant_mapping_dao.get(1).asdict())

    def tearDown(self):
        with self.rex_study_dao.session() as session:
            session.query(Study).filter(Study.name == "Primary Study").delete()
            session.query(Study).filter(Study.name == "Ancillary Study").delete()

        with self.rdr_participant_dao.session() as session:
            session.query(RdrParticipant).filter(
                RdrParticipant.participantId == 123,
                RdrParticipant.biobankId == 555
            ).delete()

        with self.nph_participant_dao.session() as session:
            session.query(NphParticipant).filter(NphParticipant.id == 1).delete()
