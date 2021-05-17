from rdr_service.model.genomics import GenomicGCValidationMetrics, GenomicSetMember
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'backfill-gvcf'
tool_desc = 'Backfill the gVCF paths in genomic_gc_validation_metrics'


class GVcfBackfillTool(ToolBase):
    def run(self):
        super(GVcfBackfillTool, self).run()

        # Get list of paths
        path_list = self.get_paths_from_file()

        for path in path_list:
            sample_id = self.get_sample_id_from_gvcf_path(path)
            metric = self.get_metric_from_sample_id(sample_id)
            self.update_metric_gvcf_path(metric, path)


    def get_paths_from_file(self):
        path_set = set()

        with open(self.args.input_file, encoding='utf-8-sig') as f:
            lines = f.readlines()
            for line in lines:
                path_set.add(line.strip())

        return list(path_set)

    @staticmethod
    def get_sample_id_from_gvcf_path(path):
        # Based on naming convention:
        # gs://prod-genomics-data-northwest/Wgs_sample_raw_data/
        #   SS_VCF_research/UW_A100329930_21055000718_702252_v1.hard-filtered.gvcf.gz
        return path.split("_")[7]

    def get_metric_from_sample_id(self, sample_id):
        with self.get_session() as session:
            return session.query(GenomicGCValidationMetrics).join(
                GenomicSetMember,
                GenomicSetMember.id == GenomicGCValidationMetrics.genomicSetMemberId
            ).filter(
                GenomicSetMember.sampleId == sample_id
            ).one_or_none()

    def update_metric_gvcf_path(self, metric, path):
        if self.args.md5:
            metric.gvcfMd5Received = 1
            metric.gvcfMd5Path = path

        else:
            metric.gvcfReceived = 1
            metric.gvcfPath = path

        with self.get_session() as session:
            session.merge(metric)


def add_additional_arguments(parser):
    parser.add_argument('--input-file', required=True, help='path of text file with list of gVCF paths')
    parser.add_argument('--md5', required=False, action="store_true", help='backfilling md5 files')


def run():
    return cli_run(tool_cmd, tool_desc, GVcfBackfillTool, add_additional_arguments)
