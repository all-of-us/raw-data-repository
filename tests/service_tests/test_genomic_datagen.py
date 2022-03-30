
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicGCValidationMetricsDao
from rdr_service.services.genomic_datagen import ParticipantGenerator
from tests.helpers.unittest_base import BaseTestCase


class GenomicDataGenParticipantTest(BaseTestCase):
    def setUp(self):
        super(GenomicDataGenParticipantTest, self).setUp()
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()

    def build_defaults_based_template_data(self):
        data_file_base = 'gs://stable-rdr-genomics/Wgs_sample_raw_data/SS_VCF_clinical/'
        defaults_map = {
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
                'genomic_set_member_id': 'system',
                'sex_concordance': True,
                'processing_status': 'PASS',
                'drc_fp_concordance': 'PASS',
                'aou_hdr_coverage': 99.999,
                'contamination': 0.001,
                'hf_vcf_path': f'{data_file_base}%genomic_set_member'
                               f'.gc_site_id%_T%genomic_set_member.biobank_id%_%genomic_set_member.sample_id%_v1.vcf.gz'
                ,
                'hf_vcf_tbi_received': 1
            }
        }
        count = 0
        for table, attribute_list in defaults_map.items():
            for key, val in attribute_list.items():
                count += 1
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
                    template_name='default',
                    rdr_field=f'{table}.{key}',
                    field_source=source,
                    field_value=value
                )

    def test_insert_base_template_data(self):
        # build default data template
        self.build_defaults_based_template_data()
        datagen = ParticipantGenerator(
            num_participants=1,
            external_values={
                'gc_site_id': 'uw',
                'withdrawal_status': 1,
                'consent_for_genomics_ror': 1
            }
        )
        datagen.build_participant_default()
        print('Darryl')
