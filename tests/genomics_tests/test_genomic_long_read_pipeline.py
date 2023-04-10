import datetime
from unittest import mock

from rdr_service.dao.genomics_dao import GenomicDefaultBaseDao, GenomicManifestFileDao, GenomicLongReadDao
from rdr_service.genomic_enums import GenomicManifestTypes, GenomicJob
from rdr_service.model.genomics import GenomicLRRaw
from rdr_service.offline.genomics import genomic_dispatch
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicLongReadPipelineTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.manifest_file_dao = GenomicManifestFileDao()
        self.gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        self.long_read_dao = GenomicLongReadDao()

    def execute_base_lr_ingestion(self, **kwargs):
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        bucket_name = 'test_lr_bucket'
        subfolder = 'lr_subfolder'

        for num in range(1, kwargs.get('num_set_members', 4)):
            participant_summary = self.data_generator.create_database_participant_summary(
                withdrawalStatus=1,
                suspensionStatus=1,
                consentForStudyEnrollment=1
            )

            member = self.data_generator.create_database_genomic_set_member(
                participantId=participant_summary.participantId,
                genomicSetId=self.gen_set.id,
                biobankId=f"{num}",
                genomeType="aou_array",
                qcStatus=1,
                gcManifestSampleSource="whole blood",
                gcManifestParentSampleId=f"{num}11111111111",
                participantOrigin="vibrent"
            )
            self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=member.id,
                processingStatus='Pass'
            )

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

    def test_full_lr_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR
        )

        long_read_members = self.long_read_dao.get_all()

        self.assertEqual(len(long_read_members), 3)
        self.assertTrue(all(obj.biobank_id is not None for obj in long_read_members))
        self.assertTrue(all(obj.sample_id is None for obj in long_read_members))
        self.assertTrue(all(obj.genome_type == 'aou_long_read' for obj in long_read_members))
        self.assertTrue(all(obj.long_read_platform == 'pacbio_css' for obj in long_read_members))
        self.assertTrue(all(obj.lr_site_id == 'bcm' for obj in long_read_members))
        self.assertTrue(all(obj.genomic_set_member_id is not None for obj in long_read_members))
        self.assertTrue(all(obj.long_read_set == 1 for obj in long_read_members))

    def test_lr_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR,
        )

        lr_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicLRRaw
        )

        manifest_type = 'lr'
        lr_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_awn_manifest_into_raw_table(
            lr_manifest_file.filePath,
            manifest_type
        )

        lr_raw_records = lr_raw_dao.get_all()

        self.assertEqual(len(lr_raw_records), 3)
        self.assertTrue(all(obj.file_path is not None for obj in lr_raw_records))
        self.assertTrue(all(obj.biobank_id is not None for obj in lr_raw_records))
        self.assertTrue(all(obj.genome_type is not None for obj in lr_raw_records))
        self.assertTrue(all(obj.long_read_platform is not None for obj in lr_raw_records))
        self.assertTrue(all(obj.lr_site_id is not None for obj in lr_raw_records))
        self.assertTrue(all(obj.parent_tube_id is not None for obj in lr_raw_records))

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_full_lr_to_l0_cloud_task_manifest(self, cloud_task_mock):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR
        )

        self.assertEqual(cloud_task_mock.called, True)
        self.assertEqual(cloud_task_mock.call_count, 1)

        call_json = cloud_task_mock.call_args[0][0]
        self.assertTrue(len(call_json), 1)
        self.assertTrue(call_json.get('manifest_type') == 'l0')

