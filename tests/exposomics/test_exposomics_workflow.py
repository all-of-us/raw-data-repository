import csv
import os

from rdr_service.api_util import open_cloud_file
from rdr_service.dao.exposomics_dao import ExposomicsM0Dao, ExposomicsSamplesDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.exposomics.exposomics_generate import ExposomicsGenerate
from tests.helpers.unittest_base import BaseTestCase


class ExposomicsWorkflowTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.participant_summary_dao = ParticipantSummaryDao()
        self.m0_dao = ExposomicsM0Dao()
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
        self.assertEqual(len(current_m0), 1)

        # check file data was stored
        current_m0 = current_m0[0]
        self.assertTrue(current_m0.file_data is not None)
        self.assertTrue(current_m0.file_name is not None)
        self.assertTrue(current_m0.file_path is not None)
        self.assertTrue(current_m0.bucket_name is not None)

        # check csv that was generated
        with open_cloud_file(
            os.path.normpath(f'{current_m0.file_path}')
        ) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            csv_rows = list(csv_reader)
            self.assertEqual(len(csv_rows), 2)

