import mock

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicLongReadDao
from rdr_service.genomic.genomic_sub_workflow import GenomicSubWorkflow
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessResult
from tests.helpers.unittest_base import BaseTestCase


class GenomicSubWorkflowTest(BaseTestCase):
    def setUp(self):
        super().setUp()

    @mock.patch('rdr_service.genomic.genomic_sub_workflow.GenomicBaseSubWorkflow.execute_cloud_task')
    def test_get_diff_alert_from_request(self, mock_cloud_task):

        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.LR_LR_WORKFLOW,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )
        request_biobank_ids = ['1', '2', '3']
        returned_biobank_ids = ['1', '2', '3']

        genomic_sub_workflow = GenomicSubWorkflow(
                dao=GenomicLongReadDao,
                job_id=gen_job_run.jobId,
                job_run_id=gen_job_run.id,
                manifest_file_name='test_lr_manifest.csv'
            )

        genomic_sub_workflow.handle_request_differences(
            request_biobank_ids=request_biobank_ids,
            returned_biobank_ids=returned_biobank_ids
        )

        # SHOULD NOT call if lists are equal
        self.assertEqual(mock_cloud_task.call_count, 0)
        self.assertEqual(mock_cloud_task.call_args_list, [])

        request_biobank_ids = ['1', '2', '3']
        returned_biobank_ids = ['1', '2']

        genomic_sub_workflow.handle_request_differences(
            request_biobank_ids=request_biobank_ids,
            returned_biobank_ids=returned_biobank_ids
        )

        # SHOULD call since lists are not equal
        self.assertEqual(mock_cloud_task.call_count, 1)
        cloud_task_data = mock_cloud_task.call_args_list[0][1]

        self.assertEqual(cloud_task_data.get('endpoint'), 'genomic_incident')
        self.assertEqual(len(cloud_task_data.get('payload')), 5)

        payload_data = cloud_task_data.get('payload')
        self.assertEqual(payload_data.get('slack'), True)
        self.assertEqual(payload_data.get('code'), 'REQUEST_MANIFEST_VALIDATION_FAIL')
        self.assertTrue(payload_data.get('message'))
        self.assertTrue(GenomicJob.LR_LR_WORKFLOW.name in payload_data.get('message'))



