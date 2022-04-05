from rdr_service.dao.genomic_datagen_dao import GenomicDateGenCaseTemplateDao
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicGCValidationMetricsDao, GenomicCVLSecondSampleDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.genomic_datagen_dao import GenomicDataGenRunDao, GenomicDataGenMemberRunDao
from rdr_service.services.genomic_datagen import ParticipantGenerator
from tests.helpers.unittest_base import BaseTestCase


class GenomicDataGenParticipantGeneratorTest(BaseTestCase):
    def setUp(self):
        super(GenomicDataGenParticipantGeneratorTest, self).setUp()

        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()

        self.participant_dao = ParticipantDao()
        self.participant_summary_dao = ParticipantSummaryDao()

        self.datagen_template_dao = GenomicDateGenCaseTemplateDao()
        self.datagen_run_dao = GenomicDataGenRunDao()
        self.datagen_member_run_dao = GenomicDataGenMemberRunDao()

        self.num_participants = 4

        self.data_file_base = 'gs://stable-rdr-genomics/Wgs_sample_raw_data/SS_VCF_clinical/'
        self.defaults_map = {
            'participant': {
                'participant_id': 'system',
                'biobank_id': 'system',
                'research_id': 'system',
            },
            'participant_summary': {
                'consent_for_genomics_ror': 'external',
                'consent_for_study_enrollment': 1,
                'withdrawal_status': 'external',
                'suspension_status': 1,
                'deceased_status': 0,
                'date_of_birth': '1980-01-01',
                'participant_origin': 'vibrent'
            },
            'genomic_set_member': {
                'genomic_set_id': 'system',
                'participant_id': '%participant.participant_id%',
                'collection_tube_id': '%participant.participant_id%%participant.biobank_id%',
                'genome_type': 'aou_wgs',
                'biobank_id': '%participant.biobank_id%',
                'sample_id': '22%participant.participant_id%',
                'sex_at_birth': 'F',
                'ny_flag': 0,
                'gc_site_id': 'external',
                'qc_status': 1,
                'qc_status_str': 'PASS',
                'genomic_workflow_state': 22,
                'genomic_workflow_state_str': 'CVL_READY'
            },
            'genomic_gc_validation_metrics': {
                'genomic_set_member_id': '%genomic_set_member.id%',
                'sex_concordance': True,
                'processing_status': 'PASS',
                'drc_fp_concordance': 'PASS',
                'aou_hdr_coverage': 99.999,
                'contamination': 0.001,
                'hf_vcf_path': f'{self.data_file_base}%genomic_set_member'
                               f'.gc_site_id%_T%participant.biobank_id%_'
                               f'%genomic_set_member.sample_id%_v1.vcf.gz',
                'hf_vcf_tbi_received': 1
            }
        }

        # build default datagen template data
        self.build_cvl_template_based_data('default', self.defaults_map)

    def build_cvl_template_based_data(self, template_name, _dict):
        for table, attribute_list in _dict.items():
            for key, val in attribute_list.items():
                source = None
                if val == 'system' or val == 'external':
                    value = ''
                    if val == 'system':
                        source = 'system'
                    elif val == 'external':
                        source = 'external'
                elif type(val) is str and '%' in val:
                    source = 'calculated'
                    value = val
                else:
                    source = 'literal'
                    value = val

                self.data_generator.create_database_datagen_template(
                    project_name='cvl',
                    template_name=template_name,
                    rdr_field=f'{table}.{key}',
                    field_source=source,
                    field_value=value
                )

    def test_build_cvl_template_based_data_method_inserts_correctly(self):
        current_template_data = self.datagen_template_dao.get_all()

        for table, table_attributes in self.defaults_map.items():
            table_template_items = list(filter(lambda x: table == x.rdr_field.split('.')[0],
                                               current_template_data))
            self.assertTrue(len(table_template_items), len(table_attributes))

    def test_inserted_objs_from_base_template_data(self):
        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=1,
                template_type='default',
                external_values={
                    'gc_site_id': 'uw',
                    'withdrawal_status': 1,
                    'consent_for_genomics_ror': 1
                }
            )

        self.assertEqual(len(self.participant_dao.get_all()), 1)
        self.assertEqual(len(self.participant_summary_dao.get_all()), 1)
        self.assertEqual(len(self.member_dao.get_all()), 1)
        self.assertEqual(len(self.metrics_dao.get_all()), 1)

        self.clear_table_after_test('genomic_datagen_member_run')

    def test_default_calculation_field_returns_is_correct(self):

        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=1,
                template_type='default',
                external_values={
                    'gc_site_id': 'uw',
                    'withdrawal_status': 1,
                    'consent_for_genomics_ror': 1
                }
            )

        current_participants = self.participant_dao.get_all()
        self.assertEqual(len(current_participants), 1)

        current_participant = current_participants[0]
        calc_participant_id = participant_generator._calc_algo_value('%participant.participant_id%')
        self.assertEqual(str(current_participant.participantId), calc_participant_id)

        calc_biobank_id = participant_generator._calc_algo_value('%participant.biobank_id%')
        self.assertEqual(str(current_participant.biobankId), calc_biobank_id)

        calc_research_id = participant_generator._calc_algo_value('%participant.research_id%')
        self.assertEqual(str(current_participant.researchId), calc_research_id)

        current_genomic_set_members = self.member_dao.get_all()
        self.assertEqual(len(current_participants), 1)

        current_member = current_genomic_set_members[0]
        member_defaults = self.defaults_map['genomic_set_member']

        calc_collection_tube_id = participant_generator._calc_algo_value(member_defaults['collection_tube_id'])
        self.assertEqual(current_member.collectionTubeId, calc_collection_tube_id)

        calc_sample_id = participant_generator._calc_algo_value(member_defaults['sample_id'])
        self.assertEqual(current_member.sampleId, calc_sample_id)

        current_genomic_metrics = self.metrics_dao.get_all()
        self.assertEqual(len(current_genomic_metrics), 1)

        current_metric = current_genomic_metrics[0]
        metric_defaults = self.defaults_map['genomic_gc_validation_metrics']
        calc_hf_vcf_path = participant_generator._calc_algo_value(metric_defaults['hf_vcf_path'])
        self.assertEqual(current_metric.hfVcfPath, calc_hf_vcf_path)

        self.clear_table_after_test('genomic_datagen_member_run')

    def test_run_records_inserted_correctly(self):
        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=self.num_participants,
                template_type='default',
                external_values={
                    'gc_site_id': 'uw',
                    'withdrawal_status': 1,
                    'consent_for_genomics_ror': 1
                }
            )

        datagen_run = self.datagen_run_dao.get_all()
        self.assertEqual(len(datagen_run), 1)
        datagen_run = datagen_run[0]
        self.assertEqual(datagen_run.project_name, 'cvl')

        datagen_run_members = self.datagen_member_run_dao.get_all()
        self.assertEqual(len(datagen_run_members), self.num_participants)
        self.assertTrue(all(obj.created_run_id == datagen_run.id for obj in datagen_run_members))

        current_members = self.member_dao.get_all()
        self.assertEqual(len(current_members), self.num_participants)
        member_ids = [obj.id for obj in current_members]

        self.assertTrue(all(obj.template_name == 'default' for obj in datagen_run_members))
        self.assertTrue(all(obj.genomic_set_member_id in member_ids for obj in datagen_run_members))

        self.clear_table_after_test('genomic_datagen_member_run')

    def test_template_records_get_inserted(self):
        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=self.num_participants,
                template_type='default',
                external_values={
                    'gc_site_id': 'uw',
                    'withdrawal_status': 1,
                    'consent_for_genomics_ror': 1
                }
            )

        datagen_run = self.datagen_run_dao.get_all()
        self.assertEqual(len(datagen_run), 1)
        datagen_run = datagen_run[0]
        self.assertEqual(datagen_run.project_name, 'cvl')

        datagen_run_members = self.datagen_member_run_dao.get_all()
        self.assertEqual(len(datagen_run_members), self.num_participants)
        self.assertTrue(all(obj.created_run_id == datagen_run.id for obj in datagen_run_members))

        current_members = self.member_dao.get_all()
        self.assertEqual(len(current_members), self.num_participants)
        member_ids = [obj.id for obj in current_members]

        self.assertTrue(all(obj.template_name == 'default' for obj in datagen_run_members))
        self.assertTrue(all(obj.genomic_set_member_id in member_ids for obj in datagen_run_members))

        self.clear_table_after_test('genomic_datagen_member_run')

    def templates_other_default_inserts_records(self):
        w3ss_template = {
            'genomic_cvl_second_sample': {
                'genomic_set_member_id': '%genomic_set_member.id%',
                'biobank_id': '%genomic_set_member.biobank_id%',
                'sample_id': '%genomic_set_member.sample_id%',
                'version': 'v1',
                'box_storageunit_id': '1111111',
                'box_id_plate_id': '222222',
            }
        }
        template_name = 'w3ss'
        cvl_second_sample_dao = GenomicCVLSecondSampleDao()
        self.assertEqual(cvl_second_sample_dao.model_type.__tablename__, list(w3ss_template.keys())[0])

        # build template datagen w3ss template data
        self.build_cvl_template_based_data('w3ss', w3ss_template)

        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=self.num_participants,
                template_type=template_name,
                external_values={
                    'gc_site_id': 'uw',
                    'withdrawal_status': 1,
                    'consent_for_genomics_ror': 1
                }
            )

        current_members = self.member_dao.get_all()
        self.assertEqual(len(current_members), self.num_participants)
        member_ids = [obj.id for obj in current_members]

        current_second_sample_records = cvl_second_sample_dao.get_all()
        self.assertEqual(len(current_second_sample_records), self.num_participants)
        self.assertTrue(all(obj.genomic_set_member_id in member_ids for obj in current_second_sample_records))

        datagen_run_members = self.datagen_member_run_dao.get_all()
        self.assertEqual(len(datagen_run_members), self.num_participants)
        self.assertTrue(all(obj.template_name == template_name for obj in datagen_run_members))

        self.clear_table_after_test('genomic_datagen_member_run')
