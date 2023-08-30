import datetime
from unittest import mock

from rdr_service.dao.genomics_dao import GenomicDefaultBaseDao, GenomicManifestFileDao, \
    GenomicFileProcessedDao, GenomicJobRunDao, GenomicRNADao
from rdr_service.genomic_enums import GenomicManifestTypes, GenomicJob, \
    GenomicSubProcessStatus, GenomicSubProcessResult
from rdr_service.model.genomics import GenomicRRRaw
from rdr_service.offline.genomics import genomic_dispatch
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicRNAPipelineTest(BaseTestCase):
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
        self.rna_dao = GenomicRNADao()

    def base_rna_data_insert(self, **kwargs):
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
                ai_an='N'
            )

    def execute_base_rna_ingestion(self, **kwargs):
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        bucket_name = 'test_rna_bucket'
        subfolder = 'rna_subfolder'

        self.base_rna_data_insert()

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

    def test_full_rr_manifest_ingestion(self):

        self.execute_base_rna_ingestion(
            test_file='RDR_AoU_RR_Requests.csv',
            job_id=GenomicJob.RNA_RR_WORKFLOW,
            manifest_type=GenomicManifestTypes.RNA_RR
        )

        rr_members = self.rna_dao.get_all()

        self.assertEqual(len(rr_members), 3)
        self.assertTrue(all(obj.biobank_id is not None for obj in rr_members))
        self.assertTrue(all(obj.sample_id is None for obj in rr_members))
        self.assertTrue(all(obj.genome_type == 'aou_rnaseq' for obj in rr_members))
        self.assertTrue(all(obj.r_site_id == 'bi' for obj in rr_members))
        self.assertTrue(all(obj.genomic_set_member_id is not None for obj in rr_members))
        self.assertTrue(all(obj.rna_set == 1 for obj in rr_members))
        self.assertTrue(all(obj.created_job_run_id is not None for obj in rr_members))

        # check job run record
        rr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.RNA_RR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(rr_job_runs)
        self.assertEqual(len(rr_job_runs), 1)
        self.assertTrue(all(obj.created_job_run_id == rr_job_runs[0].id for obj in rr_members))

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in rr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in rr_job_runs))

        self.clear_table_after_test('genomic_rna')

    def test_pr_manifest_ingestion_increments_set(self):

        self.execute_base_rna_ingestion(
            test_file='RDR_AoU_RR_Requests.csv',
            job_id=GenomicJob.RNA_RR_WORKFLOW,
            manifest_type=GenomicManifestTypes.RNA_RR
        )

        rr_members = self.rna_dao.get_all()
        self.assertTrue(all(obj.rna_set == 1 for obj in rr_members))

        # check job run record
        rr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.RNA_RR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(rr_job_runs)
        self.assertEqual(len(rr_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in rr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in rr_job_runs))

        # rerun job should increment set correctly
        self.execute_base_rna_ingestion(
            test_file='RDR_AoU_RR_Requests.csv',
            job_id=GenomicJob.RNA_RR_WORKFLOW,
            manifest_type=GenomicManifestTypes.RNA_RR
        )

        rr_members = self.rna_dao.get_all()
        self.assertTrue(any(obj.rna_set == 2 for obj in rr_members))

        # check job run record
        rr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.RNA_RR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(rr_job_runs)
        self.assertEqual(len(rr_job_runs), 2)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in rr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in rr_job_runs))

        self.clear_table_after_test('genomic_rna')

    def test_rr_manifest_to_raw_ingestion(self):

        self.execute_base_rna_ingestion(
            test_file='RDR_AoU_RR_Requests.csv',
            job_id=GenomicJob.RNA_RR_WORKFLOW,
            manifest_type=GenomicManifestTypes.RNA_RR,
        )

        rr_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicRRRaw
        )

        manifest_type = 'rr'
        rr_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            rr_manifest_file.filePath,
            manifest_type
        )

        rr_raw_records = rr_raw_dao.get_all()

        self.assertEqual(len(rr_raw_records), 3)

        for attribute in GenomicRRRaw.__table__.columns:
            self.assertTrue(all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in rr_raw_records))

        # check job run record
        rr_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_RR_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(rr_raw_job_runs)
        self.assertEqual(len(rr_raw_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in rr_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in rr_raw_job_runs))

        self.clear_table_after_test('genomic_rna')

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_full_rr_to_r0_cloud_task_manifest(self, cloud_task_mock):

        self.execute_base_rna_ingestion(
            test_file='RDR_AoU_RR_Requests.csv',
            job_id=GenomicJob.RNA_RR_WORKFLOW,
            manifest_type=GenomicManifestTypes.RNA_RR
        )

        self.assertEqual(cloud_task_mock.called, True)
        self.assertEqual(cloud_task_mock.call_count, 1)

        # manifest type
        self.assertTrue(len(cloud_task_mock.call_args[1]), 1)
        self.assertTrue(cloud_task_mock.call_args[1].get('payload').get('manifest_type') == 'r0')

        # task queue
        self.assertTrue(cloud_task_mock.call_args[1].get('task_queue') == 'genomic-generate-manifest')

        self.clear_table_after_test('genomic_rna')
