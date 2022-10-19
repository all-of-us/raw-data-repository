
from rdr_service import config
from rdr_service.dao.genomics_dao import GenomicGCValidationMetricsDao, GenomicDefaultBaseDao
from rdr_service.genomic.genomic_mappings import array_file_types_attributes, wgs_file_types_attributes
from rdr_service.genomic.genomic_storage_class import GenomicStorageClass
from rdr_service.genomic_enums import GenomicWorkflowState
from rdr_service.model.genomics import GenomicStorageUpdate
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

    def test_get_array_file_paths_from_metrics(self):
        num_metrics = 4

        for num in range(num_metrics):
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

            self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=member.id,
                **metric_data_files
            )

        all_metrics = self.metrics_dao.get_all()

        file_for_update = GenomicStorageClass.get_file_paths_from_metrics(
            metrics=all_metrics,
            metric_type=config.GENOME_TYPE_ARRAY
        )

        self.assertTrue(len(file_for_update) == len(array_file_types_attributes) * num_metrics)
        self.assertTrue('test_bucket' in obj for obj in file_for_update)

    def test_get_wgs_file_paths_from_metrics(self):
        num_metrics = 4

        for num in range(num_metrics):
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

            self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=member.id,
                **metric_data_files
            )

        all_metrics = self.metrics_dao.get_all()

        file_for_update = GenomicStorageClass.get_file_paths_from_metrics(
            metrics=all_metrics,
            metric_type=config.GENOME_TYPE_WGS
        )

        self.assertTrue(len(file_for_update) == len(wgs_file_types_attributes) * num_metrics)
        self.assertTrue('test_bucket' in obj for obj in file_for_update)




