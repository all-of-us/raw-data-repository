from rdr_service import config
from rdr_service.api_util import open_cloud_file
from tests import test_data
from rdr_service.model.participant import Participant
from rdr_service.model.config_utils import get_biobank_id_prefix
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.offline import antibody_study_pipeline
from rdr_service.dao.antibody_study_dao import BiobankCovidAntibodySampleDao, QuestCovidAntibodyTestDao, \
    QuestCovidAntibodyTestResultDao
from rdr_service.dao.participant_dao import ParticipantDao

_FAKE_BIOBANK_SAMPLE_BUCKET = 'rdr_fake_biobank_sample_bucket'
_FAKE_QUEST_ANTIBODY_STUDY_BUCKET = 'fake-antibody-prevalence-upload'
_FAKE_BIOBANK_SAMPLE_BUCKET_FOLDER = "antibody_manifests"
_BIOBANK_ID_PREFIX = get_biobank_id_prefix()


class AntibodyStudyPipelineTest(BaseTestCase):

    def setUp(self):
        super(AntibodyStudyPipelineTest, self).setUp()
        config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BIOBANK_SAMPLE_BUCKET])
        config.override_setting(config.QUEST_ANTIBODY_STUDY_BUCKET_NAME, [_FAKE_QUEST_ANTIBODY_STUDY_BUCKET])

    def test_import_biobank_antibody_study_manifest_file(self):
        bucket_name = _FAKE_BIOBANK_SAMPLE_BUCKET
        self._create_ingestion_test_file('Quest_AoU_Serology_test_1.csv', bucket_name,
                                         folder=_FAKE_BIOBANK_SAMPLE_BUCKET_FOLDER)
        self._create_ingestion_test_file('Quest_AoU_Serology_test_2.csv', bucket_name,
                                         folder=_FAKE_BIOBANK_SAMPLE_BUCKET_FOLDER)
        # create fake participants
        participant_dao = ParticipantDao()
        p1 = Participant(participantId=1, biobankId='646545564')
        participant_dao.insert(p1)
        p2 = Participant(participantId=2, biobankId='272524862')
        participant_dao.insert(p2)
        # import manifest file
        antibody_study_pipeline.import_biobank_covid_manifest_files()

        biobank_antibody_sample_dao = BiobankCovidAntibodySampleDao()
        record_1 = biobank_antibody_sample_dao.get(1)
        self.assertEqual(record_1.aouBiobankId, 646545564)
        self.assertEqual(record_1.noAouBiobankId, None)
        self.assertEqual(record_1.sampleId, '20191000938')
        self.assertEqual(record_1.matrixTubeId, 357251991)
        self.assertEqual(record_1.sampleType, 'Serum')
        self.assertEqual(record_1.quantityUl, 350)
        self.assertEqual(record_1.storageLocation, 'BX-00219787/A1')
        self.assertEqual(record_1.ingestFileName, 'Quest_AoU_Serology_test_1.csv')
        record_2 = biobank_antibody_sample_dao.get(2)
        self.assertEqual(record_2.aouBiobankId, 272524862)
        self.assertEqual(record_2.noAouBiobankId, None)
        record_3 = biobank_antibody_sample_dao.get(3)
        self.assertEqual(record_3.aouBiobankId, None)
        self.assertEqual(record_3.noAouBiobankId, 'PIO 1106')

        # import duplicate records will not add new records
        antibody_study_pipeline.import_biobank_covid_manifest_files()
        records = biobank_antibody_sample_dao.get_all()
        self.assertEqual(len(records), 3)

    def test_import_quest_antibody_study_files(self):
        bucket_name = _FAKE_QUEST_ANTIBODY_STUDY_BUCKET
        self._create_ingestion_test_file('Quest_AoU_Tests_test_1.csv', bucket_name)
        self._create_ingestion_test_file('Quest_AoU_Tests_test_2.csv', bucket_name)
        self._create_ingestion_test_file('Quest_AoU_Results_test_1.csv', bucket_name)
        self._create_ingestion_test_file('Quest_AoU_Results_test_2.csv', bucket_name)

        # import manifest file
        antibody_study_pipeline.import_quest_antibody_files()

        quest_covid_antibody_test_dao = QuestCovidAntibodyTestDao()
        records = quest_covid_antibody_test_dao.get_all()
        self.assertEqual(len(records), 3)
        quest_covid_antibody_test_result_dao = QuestCovidAntibodyTestResultDao()
        records = quest_covid_antibody_test_result_dao.get_all()
        self.assertEqual(len(records), 4)

    def _create_ingestion_test_file(self, test_data_filename, bucket_name, folder=None):
        test_data_file = self._open_test_file(test_data_filename, _BIOBANK_ID_PREFIX)
        self._write_cloud_csv(test_data_filename, test_data_file, folder=folder, bucket=bucket_name)

    def _open_test_file(self, test_filename, biobank_id_prefix=None):
        with open(test_data.data_path(test_filename)) as f:
            lines = f.readlines()
            csv_str = ""
            for line in lines:
                if biobank_id_prefix:
                    line = line.replace('{prefix}', biobank_id_prefix)
                csv_str += line

            return csv_str

    def _write_cloud_csv(self, file_name, contents_str, bucket=None, folder=None):
        if folder is None:
            path = "/%s/%s" % (bucket, file_name)
        else:
            path = "/%s/%s/%s" % (bucket, folder, file_name)
        with open_cloud_file(path, mode='wb') as cloud_file:
            cloud_file.write(contents_str.encode("utf-8"))
