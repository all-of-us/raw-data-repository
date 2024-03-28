import datetime

from rdr_service.dao.genomics_dao import GenomicJobRunDao, GenomicSetMemberDao, GenomicDefaultBaseDao, \
    GenomicManifestFileDao, GenomicFileProcessedDao
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessStatus, GenomicSubProcessResult, \
    GenomicWorkflowState, GenomicManifestTypes
from rdr_service.model.genomics import GenomicA2Raw
from rdr_service.offline.genomics import genomic_dispatch
from rdr_service.participant_enums import QuestionnaireStatus
from tests.genomics_tests.test_genomic_utils import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicGEMPipelineTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.manifest_file_dao = GenomicManifestFileDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.job_run_dao = GenomicJobRunDao()
        self.member_dao = GenomicSetMemberDao()
        self.gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

    def execute_base_gem_ingestion(self, **kwargs):
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        bucket_name = 'test_gem_bucket'
        subfolder = 'gem_subfolder'

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

    def test_a2_manifest_ingestion(self):
        for num in range(1, 4):
            participant_summary = self.data_generator.create_database_participant_summary(
                consentForGenomicsROR=QuestionnaireStatus.SUBMITTED,
                consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED
            )
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                participantId=participant_summary.participantId,
                biobankId=f"100{num}",
                genomeType="aou_array",
                collectionTubeId=num,
                sampleId=f"1000{num}",
                genomicWorkflowState=GenomicWorkflowState.A1
            )

        self.execute_base_gem_ingestion(
            test_file='AoU_GEM_A2_manifest.csv',
            job_id=GenomicJob.GEM_A2_MANIFEST,
            manifest_type=GenomicManifestTypes.GEM_A2
        )

        gem_members = self.member_dao.get_all()

        self.assertEqual(len(gem_members), 3)
        self.assertTrue(all(obj.gemPass is not None for obj in gem_members))
        self.assertTrue(all(obj.gemPass in ['Y', 'N'] for obj in gem_members))
        self.assertTrue(all(obj.gemA2ManifestJobRunId is not None for obj in gem_members))
        self.assertTrue(all(obj.gemDateOfImport is not None for obj in gem_members))
        self.assertTrue(all(obj.genomicWorkflowState in [
            GenomicWorkflowState.GEM_RPT_READY,
            GenomicWorkflowState.A2F
            ] for obj in gem_members)
        )

        # check job run record
        a2_job_runs = list(filter(lambda x: x.jobId == GenomicJob.GEM_A2_MANIFEST, self.job_run_dao.get_all()))

        self.assertIsNotNone(a2_job_runs)
        self.assertEqual(len(a2_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in a2_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in a2_job_runs))

    def test_a2_manifest_to_raw_ingestion(self):

        self.execute_base_gem_ingestion(
            test_file='AoU_GEM_A2_manifest.csv',
            job_id=GenomicJob.GEM_A2_MANIFEST,
            manifest_type=GenomicManifestTypes.GEM_A2,
        )

        gem_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicA2Raw
        )

        manifest_type = GenomicJob.GEM_A2_MANIFEST
        gem_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            gem_manifest_file.filePath,
            manifest_type
        )

        gem_raw_records = gem_raw_dao.get_all()

        self.assertEqual(len(gem_raw_records), 3)

        for attribute in GenomicA2Raw.__table__.columns:
            self.assertTrue(all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in gem_raw_records))

        # check job run record
        a2_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_A2_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(a2_raw_job_runs)
        self.assertEqual(len(a2_raw_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in a2_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in a2_raw_job_runs))
