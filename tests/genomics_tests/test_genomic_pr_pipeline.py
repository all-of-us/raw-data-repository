
import datetime

from rdr_service.dao.genomics_dao import GenomicDefaultBaseDao, GenomicManifestFileDao, GenomicFileProcessedDao, \
    GenomicJobRunDao
from rdr_service.genomic_enums import GenomicManifestTypes, GenomicJob, \
    GenomicSubProcessStatus, GenomicSubProcessResult
from rdr_service.model.genomics import GenomicPRRaw
from rdr_service.offline.genomics import genomic_dispatch
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicPRPipelineTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.manifest_file_dao = GenomicManifestFileDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.job_run_dao = GenomicJobRunDao()
        self.gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

    def base_pr_data_insert(self, **kwargs):
        for num in range(1, kwargs.get('num_set_members', 4)):
            participant_summary = self.data_generator.create_database_participant_summary(
                withdrawalStatus=1,
                suspensionStatus=1,
                consentForStudyEnrollment=1
            )
            self.data_generator.create_database_genomic_set_member(
                participantId=participant_summary.participantId,
                genomicSetId=self.gen_set.id,
                biobankId=f"{num}",
                genomeType="aou_array",
                qcStatus=1,
                gcManifestSampleSource="whole blood",
                gcManifestParentSampleId=f"{num}11111111111",
                participantOrigin="vibrent",
                validationStatus=1,
                sexAtBirth="F",
                collectionTubeId=f"{num}2222222222",
                ai_an='Y'
            )

    def execute_base_pr_ingestion(self, **kwargs):
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        bucket_name = 'test_pr_bucket'
        subfolder = 'pr_subfolder'

        self.base_pr_data_insert()

        test_file_name = create_ingestion_test_file(
            kwargs.get('test_file'),
            bucket_name,
            folder=subfolder,
            include_timestamp=kwargs.get('include_timestamp', True),
            include_sub_num=kwargs.get('include_sub_num')
        )

        task_data = {
            "job": kwargs.get('job_id'),
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": kwargs.get('manifest_type'),
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name}"
            }
        }

        # Execute from cloud task
        genomic_dispatch.execute_genomic_manifest_file_pipeline(task_data)

    def test_full_pr_manifest_ingestion(self):

        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_PR_Requests.csv',
            job_id=GenomicJob.PR_PR_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_PR
        )

        # check job run record
        pr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.PR_PR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(pr_job_runs)
        self.assertEqual(len(pr_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in pr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in pr_job_runs))

    def test_pr_manifest_to_raw_ingestion(self):

        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_PR_Requests.csv',
            job_id=GenomicJob.PR_PR_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_PR,
        )

        pr_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicPRRaw
        )

        manifest_type = 'pr'
        pr_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_awn_manifest_into_raw_table(
            pr_manifest_file.filePath,
            manifest_type
        )

        pr_raw_records = pr_raw_dao.get_all()

        self.assertEqual(len(pr_raw_records), 3)
        self.assertTrue(all(obj.file_path is not None for obj in pr_raw_records))
        self.assertTrue(all(obj.biobank_id is not None for obj in pr_raw_records))
        self.assertTrue(all(obj.genome_type is not None for obj in pr_raw_records))
        self.assertTrue(all(obj.p_site_id is not None for obj in pr_raw_records))

        self.assertTrue(all(obj.genome_type == 'aou_proteomics' for obj in pr_raw_records))
        self.assertTrue(all(obj.p_site_id == 'bi' for obj in pr_raw_records))


