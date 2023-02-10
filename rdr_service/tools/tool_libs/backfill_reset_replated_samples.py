from rdr_service import clock
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
                'packageId': None,
                'validatedTime': None,
                'validationFlags': None,
                'sampleId': None,
                'sampleType': None,
                'collectionTubeId': f'replated_{member.replatedMemberId}',
                'reconcileCvlJobRunId': None,
                'sequencingFileName': None,
                'reconcileGcManifestJobRunId': None,
                'reconcileMetricsSequencingJobRunId': None,
                'gcManifestBoxPlateId': None,
                'gcManifestBoxStorageUnitId': None,
                'gcManifestContact': None,
                'gcManifestEmail': None,
                'gcManifestFailureDescription': None,
                'gcManifestFailureMode': None,
                'gcManifestMatrixId': None,
                'gcManifestParentSampleId': None,
                'gcManifestQuantity_ul': None,
                'gcManifestSampleSource': None,
                'gcManifestStudy': None,
                'gcManifestStudyPI': None,
                'gcManifestTestName': None,
                'gcManifestTotalConcentration_ng_per_ul': None,
                'gcManifestTotalDNA_ng': None,
                'gcManifestTrackingNumber': None,
                'gcManifestTreatments': None,
                'gcManifestVisitDescription': None,
                'gcManifestWellPosition': None,
                'gcSiteId': None,
                'devNote': 'Reset after incorrect AW1 ingestion DA-3238',
                'aw1FileProcessedId': None,
                'aw2FileProcessedId': None,
                'gemA1ManifestJobRunId': None,
                'qcStatus': 0,
                'qcStatusStr': 'UNSET'
            }
            if member.genomicWorkflowState != GenomicWorkflowState.EXTRACT_REQUESTED:
                member_to_update['genomicWorkflowState'] = int(GenomicWorkflowState.EXTRACT_REQUESTED)
                member_to_update['genomicWorkflowStateStr'] = str(GenomicWorkflowState.EXTRACT_REQUESTED)
                member_to_update['genomicWorkflowStateModifiedTime'] = clock.CLOCK.now()
            member_updates.append(member_to_update)
        self.member_dao.bulk_update(member_updates)

    def get_ids_from_file(self):
        id_set = set()

        with open(self.args.input_file, encoding='utf-8-sig') as f:
            lines = f.readlines()
            for line in lines:
                id_set.add(int(line.strip()))

        return list(id_set)




def add_additional_arguments(parser):
    parser.add_argument('--input-file', required=True, help='path of text file with list of genomic set member ids')


def run():
    return cli_run(tool_cmd, tool_desc, ResetMembersBackfillTool, add_additional_arguments)
