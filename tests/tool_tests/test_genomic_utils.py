from unittest import mock

from rdr_service.participant_enums import GenomicJob
from rdr_service.tools.tool_libs.genomic_utils import GenomicProcessRunner, LoadRawManifest
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
    def test_load_aw1_manifest_into_raw_table(self, load_job_mock):

        test_file = "test-bucket/test_folder/test_manifest_file.csv"

        GenomicUtilsGeneralTest.run_tool(LoadRawManifest, tool_args={
            'command': 'load-raw-manifest',
            'manifest_file': test_file,
        })

        for call in load_job_mock.call_args_list:
            _, kwargs = call

            self.assertEqual(test_file, kwargs['file_path'])

