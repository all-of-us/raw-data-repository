from rdr_service.dao.rex_dao import RexParticipantMappingDao, RexStudyDao
from rdr_service.dao.study_nph_dao import NphParticipantDao
from rdr_service.services.ancillary_studies.study_enrollment import EnrollmentInterface
from tests.helpers.unittest_base import BaseTestCase


class StudyEnrollmentTest(BaseTestCase):

    def __init__(self, *args, **kwargs):
        super(StudyEnrollmentTest, self).__init__(*args, **kwargs)

    def setUp(self, *args, **kwargs) -> None:
        super(StudyEnrollmentTest, self).setUp(*args, **kwargs)
        self._create_initial_study_data()

    def _create_initial_study_data(self):
        study_dao = RexStudyDao()
        aou = study_dao.model_type(schema_name='rdr')
        nph = study_dao.model_type(schema_name='nph', prefix=1000)
        study_dao.insert(aou)
        study_dao.insert(nph)

    def test_create_study_participant(self):
        aou_pid = 123123123
        self.data_generator.create_database_participant(
            participantId=aou_pid,
            researchId=12345
        )

        interface = EnrollmentInterface('NPH-1000')
        interface.create_study_participant(
            aou_pid=aou_pid,
            ancillary_pid='1000578448930'
        )
        # Test Rex inserted correctly
        rex_dao = RexParticipantMappingDao()
        pid_mapping = rex_dao.get(1)
        self.assertEqual(pid_mapping.primary_participant_id, 123123123)
        self.assertEqual(pid_mapping.ancillary_participant_id, 578448930)
        self.assertEqual(pid_mapping.primary_study_id, 1)
        self.assertEqual(pid_mapping.ancillary_study_id, 2)

        # Test NPH Inserted Correctly
        nph_dao = NphParticipantDao()
        nph_participant = nph_dao.get(578448930)
        self.assertIsNotNone(nph_participant)
        self.assertEqual(nph_participant.research_id, 12345)
        self.assertIsNotNone(nph_participant.biobank_id)
        self.assertGreater(nph_participant.biobank_id, 10000000000)
        self.assertLess(nph_participant.biobank_id, 99999999999)

    def test_call_to_enrollment_cloud_task(self):
        pass
