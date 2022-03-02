import datetime

from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicFileProcessedDao, GenomicJobRunDao
from rdr_service.genomic_enums import GenomicManifestTypes, GenomicJob, GenomicWorkflowState, GenomicSubProcessStatus, \
    GenomicSubProcessResult
from rdr_service.offline import genomic_pipeline
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicCVLPipelineTest(BaseTestCase):
    def setUp(self):
        super(GenomicCVLPipelineTest, self).setUp()
        self.job_run_dao = GenomicJobRunDao()
        self.member_dao = GenomicSetMemberDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

    def test_w2sc_manifest_ingestion(self):
        test_file = 'RDR_AoU_CVL_W2SC.csv'
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        bucket_name = 'test_cvl_bucket'
        subfolder = 'cvl_subfolder'

        # wgs members which should be updated
        for num in range(1, 3):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                biobankId=f"{num}",
                sampleId=f"100{num}",
                genomeType="aou_wgs",
                genomicWorkflowState=GenomicWorkflowState.AW1
            )

        test_file_name = create_ingestion_test_file(
            test_file,
            bucket_name,
            folder=subfolder
        )

        task_data = {
            "job": GenomicJob.CVL_W2SC_WORKFLOW,
            "bucket": 'test_cvl_bucket',
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": GenomicManifestTypes.CVL_W2SC,
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name}"
            }
        }

        # Execute from cloud task
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        current_members = self.member_dao.get_all()
        self.assertEqual(len(current_members), 2)

        w2sc_job_run = list(filter(lambda x: x.jobId == GenomicJob.CVL_W2SC_WORKFLOW, self.job_run_dao.get_all()))[0]

        self.assertIsNotNone(w2sc_job_run)
        self.assertEqual(w2sc_job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(w2sc_job_run.runResult, GenomicSubProcessResult.SUCCESS)

        self.assertTrue(len(self.file_processed_dao.get_all()), 1)
        w2sc_file_processed = self.file_processed_dao.get(1)
        self.assertTrue(w2sc_file_processed.runId, w2sc_job_run.jobId)

        self.assertTrue(all(obj.cvlW2scManifestJobRunID is not None for obj in current_members))
        self.assertTrue(all(obj.cvlW2scManifestJobRunID == w2sc_job_run.id for obj in current_members))

