from unittest import mock

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicSetMemberDao
from rdr_service.participant_enums import GenomicJob
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
                                        _file=None):

        GenomicProcessRunnerTest.run_tool(GenomicProcessRunner, tool_args={
            'command': 'process-runner',
            'job': genomic_job,
            'id': _id,
            'csv': _csv,
            'file': _file,
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

    def test_ingest_aw1_from_raw_table(self):
        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="1",
            genomeType="aou_array"
        )

        test_file = "test-bucket/test_folder/test_GEN_manifest_file.csv"

        self.data_generator.create_database_genomic_aw1_raw(
            file_path=test_file,
            package_id="pkg-1",
            well_position="A01",
            sample_id="1001",
            collection_tube_id="111000",
            biobank_id="A1",
            test_name="aou_array",
        )

        self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
        )

        self.data_generator.create_database_genomic_file_processed(
            runId=1,
            startTime=clock.CLOCK.now(),
            filePath=f'/{test_file}',
            bucketName="test-bucket",
            fileName="test_GEN_manifest_file.csv"
        )

        GenomicUtilsGeneralTest.run_tool(IngestionClass, tool_args={
            'command': 'sample-ingestion',
            'manifest_file': test_file,
            'data_type': 'aw1',
            'use_raw': True,
            'member_ids': "1",
            'csv': False,
        })

        dao = GenomicSetMemberDao()
        m = dao.get(1)

        self.assertEqual(m.gcManifestWellPosition, "A01")
        self.assertEqual(m.collectionTubeId, "111000")
        self.assertEqual(m.packageId, "pkg-1")
        self.assertEqual(m.gcManifestWellPosition, "A01")
        self.assertEqual(m.sampleId, "1001")
