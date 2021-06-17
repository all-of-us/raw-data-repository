from unittest import mock

from rdr_service import clock
from tests import test_data
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicGCValidationMetricsDao
from rdr_service.genomic_enums import GenomicJob, GenomicWorkflowState, GenomicContaminationCategory
from rdr_service.tools.tool_libs.backfill_gvcf_paths import GVcfBackfillTool
from rdr_service.tools.tool_libs.genomic_utils import GenomicProcessRunner, LoadRawManifest, IngestionClass
from tests.helpers.tool_test_mixin import ToolTestMixin
from tests.helpers.unittest_base import BaseTestCase


class GenomicUtilsTestBase(ToolTestMixin, BaseTestCase):
    def setUp(self):
        super(GenomicUtilsTestBase, self).setUp()


class GenomicProcessRunnerTest(GenomicUtilsTestBase):
    def setUp(self):
        super(GenomicProcessRunnerTest, self).setUp()

    @staticmethod
    def run_genomic_process_runner_tool(genomic_job,
                                        _id=None,
                                        _csv=None,
                                        _file=None,
                                        _cloud_task=None):

        GenomicProcessRunnerTest.run_tool(GenomicProcessRunner, tool_args={
            'command': 'process-runner',
            'job': genomic_job,
            'id': _id,
            'csv': _csv,
            'manifest_file': _file,
            'cloud_task': _cloud_task
        })

    @mock.patch('rdr_service.offline.genomic_pipeline.dispatch_genomic_job_from_task')
    def test_calculate_aw1_record_count(self, dispatch_job_mock):
        manifest = self.data_generator.create_database_genomic_manifest_file()

        self.run_genomic_process_runner_tool(
            genomic_job="CALCULATE_RECORD_COUNT_AW1",
            _id='1'
        )

        called_json_obj = dispatch_job_mock.call_args[0][0]

        self.assertEqual(manifest.id, called_json_obj.manifest_file.id)
        self.assertEqual(GenomicJob.CALCULATE_RECORD_COUNT_AW1, called_json_obj.job)


class GenomicUtilsGeneralTest(GenomicUtilsTestBase):
    def setUp(self):
        super(GenomicUtilsGeneralTest, self).setUp()

    def setup_raw_test_data(self, test_aw1, test_aw2):
        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="1",
            genomeType="aou_array",
            genomicWorkflowState=GenomicWorkflowState.AW0
        )

        self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
        )

        self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_INGESTION,
            startTime=clock.CLOCK.now(),
        )

        # AW1 file_processed
        self.data_generator.create_database_genomic_file_processed(
            runId=1,
            startTime=clock.CLOCK.now(),
            filePath=f'{test_aw1}',
            bucketName="test-bucket",
            fileName="test_GEN_sample_manifest.csv"
        )

        # AW2 file_processed
        self.data_generator.create_database_genomic_file_processed(
            runId=2,
            startTime=clock.CLOCK.now(),
            filePath=f'{test_aw2}',
            bucketName="test-bucket",
            fileName="test_GEN_data_manifest.csv"
        )

    @mock.patch('rdr_service.offline.genomic_pipeline.load_awn_manifest_into_raw_table')
    def test_load_manifest_into_raw_table(self, load_job_mock):

        test_file = "test-bucket/test_folder/test_manifest_file.csv"

        GenomicUtilsGeneralTest.run_tool(LoadRawManifest, tool_args={
            'command': 'load-raw-manifest',
            'manifest_file': test_file,
            'manifest_type': 'aw1',
        })

        for call in load_job_mock.call_args_list:
            _, kwargs = call

            self.assertEqual(test_file, kwargs['file_path'])
            self.assertEqual("aw1", kwargs['manifest_type'])

    def test_ingest_awn_from_raw_table(self):

        test_aw1 = "test-bucket/test_folder/test_GEN_sample_manifest.csv"
        test_aw2 = "test-bucket/test_folder/test_GEN_data_manifest.csv"

        self.setup_raw_test_data(test_aw1=test_aw1, test_aw2=test_aw2)

        self.data_generator.create_database_genomic_aw1_raw(
            file_path=test_aw1,
            package_id="pkg-1",
            well_position="A01",
            sample_id="1001",
            collection_tube_id="111000",
            biobank_id="A1",
            test_name="aou_array",
        )

        self.data_generator.create_database_genomic_aw2_raw(
            file_path=test_aw2,
            biobank_id="A1",
            sample_id="1001",
            contamination="0.005",
            call_rate="",
            processing_status="Pass",
            chipwellbarcode="10001_R01C01",
        )

        # Test AW1
        GenomicUtilsGeneralTest.run_tool(IngestionClass, tool_args={
            'command': 'sample-ingestion',
            'job': "AW1_MANIFEST",
            'manifest_file': test_aw1,
            'data_type': 'aw1',
            'use_raw': True,
            'member_ids': "1",
            'csv': False,
            'cloud_task': False,
        })

        mdao = GenomicSetMemberDao()
        m = mdao.get(1)

        self.assertEqual(m.gcManifestWellPosition, "A01")
        self.assertEqual(m.collectionTubeId, "111000")
        self.assertEqual(m.packageId, "pkg-1")
        self.assertEqual(m.gcManifestWellPosition, "A01")
        self.assertEqual(m.sampleId, "1001")
        self.assertEqual(GenomicWorkflowState.AW1, m.genomicWorkflowState)
        self.assertEqual(1, m.aw1FileProcessedId)

        # Test AW2
        GenomicUtilsGeneralTest.run_tool(IngestionClass, tool_args={
            'command': 'sample-ingestion',
            'job': "METRICS_INGESTION",
            'manifest_file': test_aw2,
            'data_type': 'aw2',
            'use_raw': True,
            'member_ids': "1",
            'csv': False,
            'cloud_task': False,
        })

        vdao = GenomicGCValidationMetricsDao()
        v = vdao.get(1)

        self.assertEqual(GenomicContaminationCategory.NO_EXTRACT, v.contaminationCategory)
        self.assertEqual('0.005', v.contamination)
        self.assertEqual("Pass", v.processingStatus)
        self.assertEqual("10001_R01C01", v.chipwellbarcode)
        self.assertEqual(1, v.genomicSetMemberId)

        m = mdao.get(1)
        self.assertEqual(GenomicWorkflowState.AW2, m.genomicWorkflowState)
        self.assertEqual(2, m.aw2FileProcessedId)

    def test_backfill_gvcf(self):
        test_file = test_data.data_path("test_gvcf_path.txt")

        # create test data
        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="1",
            sampleId="10001",
            genomeType="aou_array",
            genomicWorkflowState=GenomicWorkflowState.AW0
        )

        self.data_generator.create_database_genomic_gc_validation_metrics(genomicSetMemberId=1)

        # Run tool
        GenomicUtilsGeneralTest.run_tool(GVcfBackfillTool, tool_args={
            'command': 'backfill-gvcf',
            'input_file': test_file,
            'md5': False
        })

        # Test data updated correctly
        expected_path = "gs://test-genomics-data-rdr/Wgs_sample_raw_data/"
        expected_path += "SS_VCF_research/RDR_A1_10001_00000_v1.hard-filtered.gvcf.gz"

        metric_dao = GenomicGCValidationMetricsDao()
        metric_obj = metric_dao.get(1)

        self.assertEqual(metric_obj.gvcfReceived, 1)
        self.assertEqual(metric_obj.gvcfPath, expected_path)
