import csv
import os

from rdr_service.api_util import open_cloud_file
from rdr_service.dao.exposomics_dao import ExposomicsM0Dao, ExposomicsSamplesDao, ExposomicsM1Dao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.exposomics.exposomics_generate import ExposomicsGenerate
from rdr_service.exposomics.exposomics_manifests import ExposomicsM1Workflow
from tests.genomics_tests.test_genomic_utils import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class ExposomicsWorkflowTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.participant_summary_dao = ParticipantSummaryDao()
        self.m0_dao = ExposomicsM0Dao()
        self.m1_dao = ExposomicsM1Dao()
        self.samples_dao = ExposomicsSamplesDao()

    def generate_m0_data(self):
        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for _ in range(2):
            participant_summary = self.data_generator.create_database_participant_summary(
                consentForStudyEnrollment=1
            )
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId=participant_summary.biobankId,
                genomeType="aou_array",
            )

    @classmethod
    def execute_base_exposomics_ingestion(cls, **kwargs):
        create_ingestion_test_file(
            test_data_filename=kwargs.get('test_data_filename'),
            bucket_name=kwargs.get('bucket_name'),
            folder=kwargs.get('subfolder'),
            include_timestamp=False,
            include_sub_num=False
        )

    def test_form_data_to_M0_manifest(self):

        self.generate_m0_data()

        current_biobank_ids = [obj.biobankId for obj in self.participant_summary_dao.get_all()]

        form_data = {'sample_type': 'Plasma', 'treatment_type': 'EDTA', 'unique_study_identifier': '8579309',
                     'study_name': 'Fake research study', 'study_pi_first_name': 'Jimmy', 'study_pi_last_name': 'Johns',
                     'quantity_ul': '22', 'total_concentration_ng_ul': '22', 'freeze_thaw_count': '22'}

        sample_list = [
            {
                'biobank_id': f'A{current_biobank_ids[0]}',
                'sample_id': '11111',
                'collection_tube_id': '11111111'
            },
            {
                'biobank_id': f'A{current_biobank_ids[1]}',
                'sample_id': '22222',
                'collection_tube_id': '22222222'
            },
            {
                'biobank_id': 'A300000',
                'sample_id': '33333',
                'collection_tube_id': '33333333'
            }
        ]

        # run generation
        ExposomicsGenerate.create_exposomics_generate_workflow(
            sample_list=sample_list,
            form_data=form_data,
        ).run_generation()

        # check stored samples
        current_samples = self.samples_dao.get_all()

        self.assertEqual(len(sample_list), len(current_samples))
        self.assertTrue(all(obj.biobank_id is not None for obj in current_samples))
        self.assertTrue(all(obj.sample_id is not None for obj in current_samples))
        self.assertTrue(all(obj.collection_tube_id is not None for obj in current_samples))
        self.assertTrue(all(obj.exposomics_set is not None for obj in current_samples))
        self.assertTrue(all(obj.exposomics_set is not None for obj in current_samples))
        self.assertTrue(all(obj.exposomics_set == 1 for obj in current_samples))

        # check file was generated
        current_m0 = self.m0_dao.get_all()
        self.assertEqual(len(current_m0), 2)

        # check row data records stored
        self.assertTrue(all(obj.created is not None for obj in current_m0))
        self.assertTrue(all(obj.modified is not None for obj in current_m0))
        self.assertTrue(all(obj.biobank_id is not None for obj in current_m0))
        self.assertTrue(all(obj.file_path is not None for obj in current_m0))
        self.assertTrue(all(obj.row_data is not None for obj in current_m0))
        self.assertTrue(all(obj.file_name is not None for obj in current_m0))
        self.assertTrue(all(obj.bucket_name is not None for obj in current_m0))
        self.assertTrue(all(obj.exposomics_set is not None for obj in current_m0))

        # check csv that was generated
        with open_cloud_file(
            os.path.normpath(f'{current_m0[0].file_path}')
        ) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            csv_rows = list(csv_reader)
            self.assertEqual(len(csv_rows), 2)

    def test_exposomics_m1_ingestion_and_send_copy_manifest(self):
        bucket_name = 'test_expo_bucket'
        subfolder = 'expo_subfolder'
        file_name = 'AoU_m1_Plasma_865485_2022-07-07-00-30-10.csv'

        self.execute_base_exposomics_ingestion(
            test_data_filename=file_name,
            bucket_name=bucket_name,
            subfolder=subfolder
        )
        original_file_path = f'{bucket_name}/{subfolder}/{file_name}'

        ExposomicsM1Workflow(
            file_path=original_file_path
        ).ingest_manifest()

        # check file was ingested
        current_m1 = self.m1_dao.get_all()
        self.assertEqual(len(current_m1), 3)

        # check row data records stored
        self.assertTrue(all(obj.created is not None for obj in current_m1))
        self.assertTrue(all(obj.modified is not None for obj in current_m1))
        self.assertTrue(all(obj.biobank_id is not None for obj in current_m1))
        self.assertTrue(all(obj.file_path is not None for obj in current_m1))
        self.assertTrue(all(obj.row_data is not None for obj in current_m1))
        self.assertTrue(all(obj.file_name is not None for obj in current_m1))
        self.assertTrue(all(obj.bucket_name is not None for obj in current_m1))
        self.assertTrue(all(obj.copied_path is not None for obj in current_m1))

        # check file paths differ
        self.assertTrue(all(obj.file_path != obj.copied_path for obj in current_m1))

        copied_path = current_m1[0].copied_path
        # check copied csv that was generated
        with open_cloud_file(
            os.path.normpath(copied_path)
        ) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            csv_rows = list(csv_reader)
            self.assertEqual(len(csv_rows), 3)


