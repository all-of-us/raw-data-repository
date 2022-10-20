from unittest import mock

from rdr_service import config, clock
from rdr_service.dao.genomics_dao import GenomicGCValidationMetricsDao, GenomicDefaultBaseDao
from rdr_service.genomic.genomic_mappings import array_file_types_attributes, wgs_file_types_attributes
from rdr_service.genomic.genomic_storage_class import GenomicStorageClass
from rdr_service.genomic_enums import GenomicWorkflowState, GenomicJob, GenomicSubProcessResult
from rdr_service.model.genomics import GenomicStorageUpdate
from rdr_service.offline import genomic_pipeline
from tests.helpers.unittest_base import BaseTestCase


class GenomicStorageTest(BaseTestCase):
    def setUp(self):
        super(GenomicStorageTest, self).setUp()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.storage_update_dao = GenomicDefaultBaseDao(
            model_type=GenomicStorageUpdate
        )

        self.gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        self.num_metrics = 4

    @classmethod
    def generate_array_metric_files(cls, **kwargs) -> dict:
        test_bucket = 'test_bucket'
        metric_obj = {}
        prefix = kwargs.get('prefix', '')

        for file_type in array_file_types_attributes:
            metric_obj[file_type['file_path_attribute']] = f"{test_bucket}/{prefix}_{file_type['file_type']}"

        return metric_obj

    @classmethod
    def generate_wgs_metric_files(cls, **kwargs) -> dict:
        test_bucket = 'test_bucket'
        metric_obj = {}
        prefix = kwargs.get('prefix', '')

        for file_type in wgs_file_types_attributes:
            metric_obj[file_type['file_path_attribute']] = f"{test_bucket}/{prefix}_{file_type['file_type']}"

        return metric_obj

    def test_get_array_file_dict_from_metrics(self):
        metric_ids = []
        for num in range(self.num_metrics):
            summary = self.data_generator.create_database_participant_summary()

            member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                sampleId=f'2222222{num}',
                participantId=summary.participantId,
                biobankId="1",
                genomeType="aou_array",
                genomicWorkflowState=GenomicWorkflowState.AW0
            )

            metric_data_files = self.generate_array_metric_files(prefix=member.sampleId)

            metric = self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=member.id,
                **metric_data_files
            )
            metric_ids.append(metric.id)

        all_metrics = self.metrics_dao.get_all()

        update_dict = GenomicStorageClass.get_file_dict_from_metrics(
            metrics=all_metrics,
            metric_type=config.GENOME_TYPE_ARRAY
        )

        self.assertTrue(obj['metric_id'] in metric_ids for obj in update_dict)
        for file_update in update_dict:
            paths = file_update['metric_paths']
            self.assertTrue(len(paths) == len(array_file_types_attributes))
            self.assertTrue(all('test_bucket' in obj for obj in paths))

    def test_get_wgs_file_dict_from_metrics(self):
        metric_ids = []
        for num in range(self.num_metrics):
            summary = self.data_generator.create_database_participant_summary()

            member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                sampleId=f'2222223{num}',
                participantId=summary.participantId,
                biobankId="1",
                genomeType="aou_wgs",
                genomicWorkflowState=GenomicWorkflowState.AW0
            )

            metric_data_files = self.generate_wgs_metric_files(prefix=member.sampleId)

            metric = self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=member.id,
                **metric_data_files
            )
            metric_ids.append(metric.id)

        all_metrics = self.metrics_dao.get_all()

        update_dict = GenomicStorageClass.get_file_dict_from_metrics(
            metrics=all_metrics,
            metric_type=config.GENOME_TYPE_WGS
        )

        self.assertTrue(obj['metric_id'] in metric_ids for obj in update_dict)
        for file_update in update_dict:
            paths = file_update['metric_paths']
            self.assertTrue(len(paths) == len(wgs_file_types_attributes))
            self.assertTrue(all('test_bucket' in obj for obj in paths))

    @mock.patch('rdr_service.storage.GoogleCloudStorageProvider.change_file_storage_class')
    def test_get_array_file_for_storage_update(self, storage_mock):
        num_metrics = 4
        metric_ids = []

        aw4_gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW4_ARRAY_WORKFLOW,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        gem_a2_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.GEM_A2_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        for num in range(num_metrics):
            summary = self.data_generator.create_database_participant_summary()

            member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                sampleId=f'2222222{num}',
                participantId=summary.participantId,
                biobankId="1",
                genomeType="aou_array",
                genomicWorkflowState=GenomicWorkflowState.AW0,
                aw4ManifestJobRunID=aw4_gen_job_run.id,
                gemA2ManifestJobRunId=gem_a2_job_run.id
            )

            metric_data_files = self.generate_array_metric_files(prefix=member.sampleId)

            metric = self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=member.id,
                **metric_data_files
            )
            metric_ids.append(metric.id)

        genomic_pipeline.genomic_update_storage_class(
            storage_job_type=GenomicJob.UPDATE_ARRAY_STORAGE_CLASS
        )

        all_updated_storage = self.storage_update_dao.get_all()

        self.assertTrue(all(obj.genome_type == 'aou_array' for obj in all_updated_storage))
        self.assertTrue(all(obj.storage_class == 'COLDLINE' for obj in all_updated_storage))
        self.assertTrue(all(obj.metrics_id in metric_ids for obj in all_updated_storage))

        # mock checks
        self.assertTrue(storage_mock.called is True)
        self.assertTrue((len(metric_data_files) * num_metrics) == storage_mock.call_count)

        for mock_call in storage_mock.call_args_list:
            self.assertTrue(mock_call[1]['storage_class'] == 'COLDLINE')

        self.clear_table_after_test('genomic_storage_update')

    @mock.patch('rdr_service.storage.GoogleCloudStorageProvider.change_file_storage_class')
    def test_get_wgs_file_for_storage_update(self, storage_mock):
        num_metrics = 4
        metric_ids = []

        aw4_gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW4_ARRAY_WORKFLOW,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        for num in range(num_metrics):
            summary = self.data_generator.create_database_participant_summary()
            member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                sampleId=f'2222222{num}',
                participantId=summary.participantId,
                biobankId="1",
                genomeType="aou_wgs",
                genomicWorkflowState=GenomicWorkflowState.AW0,
                aw4ManifestJobRunID=aw4_gen_job_run.id
            )

            metric_data_files = self.generate_wgs_metric_files(prefix=member.sampleId)

            metric = self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=member.id,
                **metric_data_files
            )
            metric_ids.append(metric.id)

        genomic_pipeline.genomic_update_storage_class(
            storage_job_type=GenomicJob.UPDATE_WGS_STORAGE_CLASS
        )

        all_updated_storage = self.storage_update_dao.get_all()

        self.assertTrue(all(obj.genome_type == 'aou_wgs' for obj in all_updated_storage))
        self.assertTrue(all(obj.storage_class == 'COLDLINE' for obj in all_updated_storage))
        self.assertTrue(all(obj.metrics_id in metric_ids for obj in all_updated_storage))

        # mock checks
        self.assertTrue(storage_mock.called is True)
        self.assertTrue((len(metric_data_files) * num_metrics) == storage_mock.call_count)

        for mock_call in storage_mock.call_args_list:
            self.assertTrue(mock_call[1]['storage_class'] == 'COLDLINE')

        self.clear_table_after_test('genomic_storage_update')
