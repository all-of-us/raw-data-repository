import random

from rdr_service.dao.genomic_datagen_dao import GenomicDateGenCaseTemplateDao
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicGCValidationMetricsDao, \
    GenomicCVLSecondSampleDao, GenomicInformingLoopDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.genomic_datagen_dao import GenomicDataGenRunDao, GenomicDataGenMemberRunDao
from rdr_service.participant_enums import QuestionnaireStatus, WithdrawalStatus
from rdr_service.services.genomic_datagen import ParticipantGenerator, GeneratorOutputTemplate
from tests.helpers.unittest_base import BaseTestCase


class GenomicDataGenMixin(BaseTestCase):

    data_file_base = 'gs://stable-rdr-genomics/Wgs_sample_raw_data/SS_VCF_clinical/'
    defaults_map = {
        'participant': {
            'participant_id': 'system',
            'biobank_id': 'system',
            'research_id': 'system',
        },
        'participant_summary': {
            'consent_for_genomics_ror': 'external_gror_status',
            'consent_for_study_enrollment': 1,
            'withdrawal_status': 'external_withdrawal_status',
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
            'gc_site_id': 'external_requesting_site',
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
            'hf_vcf_path': f'{data_file_base}%genomic_set_member'
                           f'.gc_site_id%_T%participant.biobank_id%_'
                           f'%genomic_set_member.sample_id%_v1.vcf.gz',
            'hf_vcf_tbi_received': 1
        }
    }

    def build_cvl_template_based_data(self, template_name, _dict):
        for table, attribute_list in _dict.items():
            for key, val in attribute_list.items():
                if val == 'system':
                    value = ''
                    source = 'system'
                elif type(val) is str and 'external' in val:
                    source = 'external'
                    value = val.split('_', 1)[-1]
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


class GenomicDataGenParticipantGeneratorTest(GenomicDataGenMixin):
    def setUp(self):
        super(GenomicDataGenParticipantGeneratorTest, self).setUp()
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.informing_loop_dao = GenomicInformingLoopDao()
        self.participant_dao = ParticipantDao()
        self.participant_summary_dao = ParticipantSummaryDao()

        self.datagen_run_dao = GenomicDataGenRunDao()
        self.datagen_template_dao = GenomicDateGenCaseTemplateDao()
        self.datagen_member_run_dao = GenomicDataGenMemberRunDao()
        self.num_participants = 4

        # build default datagen template data
        self.build_cvl_template_based_data('default', self.defaults_map)

    def test_build_cvl_template_based_data_method_inserts_correctly(self):
        current_template_data = self.datagen_template_dao.get_all()

        for table, table_attributes in self.defaults_map.items():
            table_template_items = list(filter(lambda x: table == x.rdr_field.split('.')[0],
                                               current_template_data))
            self.assertTrue(len(table_template_items), len(table_attributes))

        system_items = list(filter(lambda x: x.field_source == 'system', current_template_data))
        self.assertTrue(all(obj.field_value == '' for obj in system_items))

        all_other_items = list(filter(lambda x: x.field_source != 'system', current_template_data))
        self.assertTrue(all(obj.field_value is not None for obj in all_other_items))

    def test_inserted_objs_from_base_template_data(self):
        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=1,
                template_type='default',
                external_values={
                    'requesting_site': 'uw',
                    'withdrawal_status': 1,
                    'gror_status': 1
                }
            )

        self.assertEqual(len(self.participant_dao.get_all()), 1)
        self.assertEqual(len(self.participant_summary_dao.get_all()), 1)
        self.assertEqual(len(self.member_dao.get_all()), 1)
        self.assertEqual(len(self.metrics_dao.get_all()), 1)

        self.clear_table_after_test('genomic_datagen_member_run')

    def test_external_values_inserted_correctly(self):
        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=1,
                template_type='default',
                external_values={
                    'requesting_site': 'uw',
                    'withdrawal_status': 1,
                    'gror_status': 1
                }
            )

        genomic_set_members = self.member_dao.get_all()
        self.assertEqual(len(genomic_set_members), 1)
        member = genomic_set_members[0]
        self.assertEqual(member.gcSiteId, 'uw')

        participant_summaries = self.participant_summary_dao.get_all()
        self.assertEqual(len(participant_summaries), 1)
        summary = participant_summaries[0]
        self.assertEqual(summary.withdrawalStatus, WithdrawalStatus.NOT_WITHDRAWN)
        self.assertEqual(summary.consentForGenomicsROR, QuestionnaireStatus.SUBMITTED)

        self.clear_table_after_test('genomic_datagen_member_run')

    def test_default_calculation_field_returns_is_correct(self):
        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=1,
                template_type='default',
                external_values={
                    'requesting_site': 'uw',
                    'withdrawal_status': 1,
                    'gror_status': 1
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
                    'requesting_site': 'uw',
                    'withdrawal_status': 1,
                    'gror_status': 1
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
                    'requesting_site': 'uw',
                    'withdrawal_status': 1,
                    'gror_status': 1
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

    def test_template_handles_external_informing_loop_inserts(self):
        w1il_template = {
            'genomic_informing_loop': {
                'participant_id': '%genomic_set_member.participant_id%',
                'event_type': 'informing_loop_decision',
                'module_type': 'external_informing_loop_hdr',
                'decision_value': 'external_informing_loop_hdr'
            }
        }
        template_name = 'w1il'

        # build template datagen w1il template data
        self.build_cvl_template_based_data(template_name, w1il_template)

        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=1,
                template_type=template_name,
                external_values={
                    'requesting_site': 'uw',
                    'withdrawal_status': 1,
                    'gror_status': 1,
                    'informing_loop_hdr': 'yes',
                    'informing_loop_pgx': 'yes'
                }
            )

        participant_summary = self.participant_summary_dao.get_all()
        self.assertEqual(len(participant_summary), 1)

        participant_summary_id = participant_summary[0].participantId

        informing_loops = self.informing_loop_dao.get_all()
        self.assertEqual(len(informing_loops), 2)

        self.assertTrue(all(obj.participant_id == participant_summary_id for obj in informing_loops))
        self.assertTrue(all(obj.event_type == 'informing_loop_decision' for obj in informing_loops))
        self.assertTrue(all(obj.module_type in ['hdr', 'pgx'] for obj in informing_loops))
        self.assertTrue(all(obj.decision_value == 'yes' for obj in informing_loops))

        self.clear_table_after_test('genomic_datagen_member_run')

    def test_templates_other_default_inserts_records(self):
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
        self.build_cvl_template_based_data(template_name, w3ss_template)

        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=self.num_participants,
                template_type=template_name,
                external_values={
                    'requesting_site': 'uw',
                    'withdrawal_status': 1,
                    'gror_status': 1
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


class GenomicDataGeneratorOutputTemplateTest(GenomicDataGenMixin):
    def setUp(self):
        super(GenomicDataGeneratorOutputTemplateTest, self).setUp()
        self.datagen_run_dao = GenomicDataGenRunDao()
        self.datagen_member_run_dao = GenomicDataGenMemberRunDao()

        self.default_output_template_field_map = {
            'template_name': 'default',
            'withdrawal_status': 'ParticipantSummary.withdrawalStatus',
            'gror_status': 'ParticipantSummary.consentForGenomicsROR',
            'biobank_id': 'GenomicSetMember.biobankId',
            'sample_id': 'GenomicSetMember.sampleId',
        }

        # build default datagen template data
        self.build_cvl_template_based_data('default', self.defaults_map)

    def build_default_output_template_records(self, _dict):
        for index, (fieldname, value) in enumerate(_dict.items()):
            s_type = 'literal'
            if type(value) is str and '.' in value:
                s_type = 'model'
            s_value = value

            self.data_generator.create_database_genomic_datagen_output_template(
                project_name='cvl',
                template_name='default',
                field_index=index,
                field_name=fieldname,
                source_type=s_type,
                source_value=s_value
            )

    @staticmethod
    def run_base_participant_generator():
        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=4,
                template_type='default',
                external_values={
                    'requesting_site': 'uw',
                    'withdrawal_status': 1,
                    'gror_status': 1
                }
            )

    def test_get_output_data_by_run_id_base_models(self):

        # use generator to insert participants
        self.run_base_participant_generator()

        self.build_default_output_template_records(self.default_output_template_field_map)

        # get datagen run id
        datagen_run = self.datagen_run_dao.get_all()[0]
        datagen_run_id = datagen_run.id
        members_in_run = self.datagen_member_run_dao.get_set_members_from_run_id(datagen_run_id)

        member_sample_ids = [obj.sampleId for obj in members_in_run]
        member_biobank_ids = [obj.biobankId for obj in members_in_run]

        template_output = GeneratorOutputTemplate(
            output_template_name='default',
            output_run_id=datagen_run_id
        )
        generator_output = template_output.run_output_creation()

        for item in generator_output:
            should_have_keys = self.default_output_template_field_map.keys()
            self.assertTrue(item.keys() == should_have_keys)

        self.assertTrue(all(obj['template_name'] == 'default' for obj in generator_output))
        self.assertTrue(all(obj['biobank_id'] in member_biobank_ids for obj in generator_output))
        self.assertTrue(all(obj['sample_id'] in member_sample_ids for obj in generator_output))
        self.assertTrue(all(obj['withdrawal_status'] == WithdrawalStatus.NOT_WITHDRAWN.name.lower() for obj in
                            generator_output))
        self.assertTrue(all(obj['gror_status'] == QuestionnaireStatus.SUBMITTED.name.lower() for obj in
                            generator_output))

    def test_get_output_data_by_sample_ids(self):

        # use generator to insert participants
        self.run_base_participant_generator()

        self.build_default_output_template_records(self.default_output_template_field_map)

        # get datagen run id
        datagen_run = self.datagen_run_dao.get_all()[0]
        datagen_run_id = datagen_run.id
        members_in_run = self.datagen_member_run_dao.get_set_members_from_run_id(datagen_run_id)

        member_sample_ids = [obj.sampleId for obj in members_in_run]
        random_sample_id = [random.choice(member_sample_ids)]

        template_output = GeneratorOutputTemplate(
            output_template_name='default',
            output_sample_ids=random_sample_id
        )
        generator_output = template_output.run_output_creation()

        self.assertEqual(len(generator_output), len(random_sample_id))
        self.assertTrue(generator_output[0]['sample_id'] == random_sample_id[0])

    def test_get_output_data_by_run_id_with_loop_models(self):

        # use generator to insert participants
        w1il_template = {
            'genomic_informing_loop': {
                'participant_id': '%genomic_set_member.participant_id%',
                'event_type': 'informing_loop_decision',
                'module_type': 'external_informing_loop_hdr',
                'decision_value': 'external_informing_loop_hdr'
            }
        }
        template_name = 'w1il'

        # build template datagen w1il template data
        self.build_cvl_template_based_data(template_name, w1il_template)

        with ParticipantGenerator() as participant_generator:
            participant_generator.run_participant_creation(
                num_participants=1,
                template_type=template_name,
                external_values={
                    'requesting_site': 'uw',
                    'withdrawal_status': 1,
                    'gror_status': 1,
                    'informing_loop_hdr': 'yes',
                    'informing_loop_pgx': 'no'
                }
            )

        self.default_output_template_field_map.update({
            'informing_loop_pgx': 'GenomicInformingLoop.pgx_decision_value',
            'informing_loop_hdr': 'GenomicInformingLoop.hdr_decision_value'
        })

        self.build_default_output_template_records(
            self.default_output_template_field_map
        )

        # get datagen run id
        datagen_run = self.datagen_run_dao.get_all()[0]
        datagen_run_id = datagen_run.id

        template_output = GeneratorOutputTemplate(
            output_template_name='default',
            output_run_id=datagen_run_id
        )
        generator_output = template_output.run_output_creation()

        for item in generator_output:
            should_have_keys = self.default_output_template_field_map.keys()
            self.assertTrue(item.keys() == should_have_keys)

