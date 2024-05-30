from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.exposomics.exposomics_generate import ExposomicsGenerate
from tests.helpers.unittest_base import BaseTestCase


class ExposomicsWorkflowTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.participant_summary_dao = ParticipantSummaryDao()

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
            self.data_generator.create_database_biobank_stored_sample(
                biobankId=participant_summary.biobankId,
                test='test'
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
                'biobank_id': current_biobank_ids[0],
                'sample_id': '11111'
            },
            {
                'biobank_id': current_biobank_ids[1],
                'sample_id': '22222'
            },
            {
                'biobank_id': 3000000,
                'sample_id': '33333'
            }
        ]

        ExposomicsGenerate.create_exposomics_generate_workflow(
            sample_list=sample_list,
            form_data=form_data,
        ).run_generation()

