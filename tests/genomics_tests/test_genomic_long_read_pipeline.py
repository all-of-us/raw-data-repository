import datetime

from rdr_service.dao.genomics_dao import GenomicDefaultBaseDao, GenomicManifestFileDao
from rdr_service.genomic_enums import GenomicManifestTypes, GenomicJob
from rdr_service.model.genomics import GenomicLRRaw
from rdr_service.offline import genomic_pipeline
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicLongReadPipelineTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.manifest_file_dao = GenomicManifestFileDao()

    def execute_base_lr_ingestion(self, **kwargs):
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        bucket_name = 'test_lr_bucket'
        subfolder = 'lr_subfolder'

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
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

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

        genomic_pipeline.load_awn_manifest_into_raw_table(
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
