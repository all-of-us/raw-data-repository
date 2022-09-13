

from rdr_service import clock, config
from rdr_service.dao.genomics_dao import GenomicSetMemberDao
from rdr_service.genomic.genomic_job_components import GenomicFileIngester
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicFileIngesterTest(BaseTestCase):
    def setUp(self):
        super(GenomicFileIngesterTest, self).setUp()
        self.bucket_name = 'rdr_fake_genomic_center_a_bucket'
        self.gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        self.member_dao = GenomicSetMemberDao()

    def test_setting_correct_num_row_iterations(self):
        config.override_setting(config.GENOMIC_MAX_NUM_INGEST, [3])

        subfolder = config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
        test_file = 'RDR_AoU_GEN_TestDataManifest.csv'

        job_controller = GenomicJobController(job_id=1)

        file_ingester = GenomicFileIngester(
            job_id=1,
            _controller=job_controller
        )

        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_INGESTION,
            startTime=clock.CLOCK.now()
        )

        test_file_name = create_ingestion_test_file(
            test_file,
            self.bucket_name,
            folder=subfolder,
        )

        gen_processed_file = self.data_generator.create_database_genomic_file_processed(
            runId=gen_job_run.id,
            startTime=clock.CLOCK.now(),
            filePath=f"{self.bucket_name}/{subfolder}/{test_file_name}",
            bucketName=self.bucket_name,
            fileName=test_file_name,
        )

        data = file_ingester._retrieve_data_from_path(gen_processed_file.filePath)

        total_rows = len(data['rows'])
        sample_ids = [obj['Sample ID'] for obj in data['rows']]

        iterations = file_ingester._set_data_ingest_iterations(data['rows'])
        self.assertEqual(len(iterations), 1)

        job_controller.max_num = config.getSetting(config.GENOMIC_MAX_NUM_INGEST)
        iterations = file_ingester._set_data_ingest_iterations(data['rows'])
        correct_length = round(total_rows / job_controller.max_num)  # 3

        self.assertEqual(len(iterations), correct_length)

        distinct_sample_ids = set()
        for iteration in iterations:
            current_sample_ids = set([obj['Sample ID'] for obj in iteration])
            distinct_sample_ids.update(current_sample_ids)

        self.assertEqual(len(distinct_sample_ids), total_rows)

        distinct_list = list(distinct_sample_ids)
        self.assertEqual(distinct_list.sort(), sample_ids.sort())

    def test_replating_copy(self):

        job_controller = GenomicJobController(job_id=1)

        file_ingester = GenomicFileIngester(
            job_id=1,
            _controller=job_controller
        )

        member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=self.gen_set.id,
            biobankId="11111111",
            sampleId="222222222222",
            genomeType="aou_wgs",
        )

        self.assertTrue(len(self.member_dao.get_all()), 1)

        block_research_reason = 'Sample Swap'
        genome_type = 'aou_investigation'

        file_ingester.copy_member_for_replating(
            member=member,
            genome_type=genome_type,
            block_research_reason=block_research_reason
        )

        self.assertTrue(len(self.member_dao.get_all()), 2)

        copy_member = self.member_dao.get(2)

        self.assertEqual(copy_member.collectionTubeId, f'replated_{member.id}')

        self.assertIsNotNone(copy_member.genomeType)
        self.assertEqual(copy_member.genomeType, genome_type)

        self.assertIsNotNone(copy_member.blockResearchReason)
        self.assertEqual(copy_member.blockResearchReason, block_research_reason)
        self.assertEqual(copy_member.blockResearch, 1)




