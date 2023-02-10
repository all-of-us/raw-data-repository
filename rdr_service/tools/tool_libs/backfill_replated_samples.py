from rdr_service import clock
from rdr_service.model.genomics import GenomicSetMember
from rdr_service.dao.genomics_dao import GenomicSetMemberDao
from rdr_service.genomic_enums import GenomicWorkflowState
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'backfill-reset-members'
tool_desc = 'Backfill replated samples to original state'


class ResetMembersBackfillTool(ToolBase):

    def __init__(self, args, gcp_env=None, tool_name=None):
        super().__init__(args, gcp_env, tool_name)
        self.member_dao = GenomicSetMemberDao()

    def run(self):
        super(ResetMembersBackfillTool, self).run()
        # Get list of paths
        id_list = self.get_ids_from_file()

        members = self.member_dao.get_members_from_member_ids(id_list)
        member_updates = []
        for member in members:
            member_to_update = {
                'id': member.id,
                'modified': clock.CLOCK.now(),
                'package_id': None,
                'validated_time': None,
                'validation_flags': None,
                'sample_id': None,
                'sample_type': None,
                'reconcile_cvl_job_run_id': None,
                'sequencing_file_name': None,
                'reconcile_gc_manifest_job_run_id': None,
                'reconcile_metrics_sequencing_job_run_id': None,
                'gc_manifest_box_plate_id': None,
                'gc_manifest_box_storage_unit_id': None,
                'gc_manifest_contact': None,
                'gc_manifest_email': None,
                'gc_manifest_failure_description': None,
                'gc_manifest_failure_mode': None,
                'gc_manifest_matrix_id': None,
                'gc_manifest_parent_sample_id': None,
                'gc_manifest_quantity_ul': None,
                'gc_manifest_sample_source': None,
                'gc_manifest_study': None,
                'gc_manifest_study_pi': None,
                'gc_manifest_test_name': None,
                'gc_manifest_total_concentration_ng_per_ul': None,
                'gc_manifest_total_dna_ng': None,
                'gc_manifest_tracking_number': None,
                'gc_manifest_treatments': None,
                'gc_manifest_visit_description': None,
                'gc_manifest_well_position': None,
                'collection_tube_id': f'replated_{member.replated_member}',
                'gc_site_id': None,
                'aw3_manifest_job_run_id': None,
                'dev_note': 'Reset after incorrect AW1 ingestion DA-3238',
                'aw1_file_processed_id': None,
                'aw2_file_processed_id': None,
                'aw3_manifest_file_id': None,
                'qc_status': 0,
                'qc_status_string': 'UNSET'
            }
            if member.genomicWorkflowState != GenomicWorkflowState.EXTRACT_REQUESTED:
                member_to_update['genomic_workflow_state'] = int(GenomicWorkflowState.EXTRACT_REQUESTED)
                member_to_update['genomic_workflow_state_str'] = str(GenomicWorkflowState.EXTRACT_REQUESTED)
                member_to_update['genomic_workflow_state_modified_time'] = clock.CLOCK.now()
            member_updates.append(member_to_update)
        self.member_dao.bulk_update(member_updates)

    def get_ids_from_file(self):
        id_set = set()

        with open(self.args.input_file, encoding='utf-8-sig') as f:
            lines = f.readlines()
            for line in lines:
                id_set.add(int(line.strip()))

        return list(id_set)

    def reset_member(self, member: GenomicSetMember):
        member.genomicWorkflowState = GenomicWorkflowState.EXTRACT_REQUESTED



def add_additional_arguments(parser):
    parser.add_argument('--input-file', required=True, help='path of text file with list of genomic set member ids')


def run():
    return cli_run(tool_cmd, tool_desc, ResetMembersBackfillTool, add_additional_arguments)
