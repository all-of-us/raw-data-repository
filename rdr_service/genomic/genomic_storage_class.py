import logging

from typing import List

from rdr_service import config
from rdr_service.dao.genomics_dao import GenomicGCValidationMetricsDao, GenomicDefaultBaseDao
from rdr_service.genomic.genomic_mappings import wgs_file_types_attributes, array_file_types_attributes
from rdr_service.genomic_enums import GenomicJob
from rdr_service.model.genomics import GenomicGCValidationMetrics, GenomicStorageUpdate
from rdr_service.storage import GoogleCloudStorageProvider


class GenomicStorageClass:

    def __init__(self,
                 storage_job_type,
                 storage_provider: GoogleCloudStorageProvider,
                 storage_class='COLDLINE'
                 ):

        self.storage_job_type = storage_job_type
        self.storage_class = storage_class
        self.storage_provider = storage_provider
        self.updated_count = 0

        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.storage_update_dao = GenomicDefaultBaseDao(
            model_type=GenomicStorageUpdate
        )

    def __get_storage_method(self):
        return {
            GenomicJob.UPDATE_ARRAY_STORAGE_CLASS: self.update_array_files,
            GenomicJob.UPDATE_WGS_STORAGE_CLASS: self.update_wgs_files
        }[self.storage_job_type]

    def run_storage_update(self):
        run_method = self.__get_storage_method()
        run_method()

    @classmethod
    def get_file_paths_from_metrics(cls, *,
                                    metrics: List[GenomicGCValidationMetrics],
                                    metric_type: str
                                    ) -> List[str]:

        metric_file_type_map = {
            config.GENOME_TYPE_ARRAY: [obj['file_path_attribute'] for obj in array_file_types_attributes],
            config.GENOME_TYPE_WGS: [obj['file_path_attribute'] for obj in wgs_file_types_attributes]
        }[metric_type]

        files_to_update = []
        for metric in metrics:
            metric_paths = [obj[1] for obj in metric if 'Path' in obj[0] and obj[0] in metric_file_type_map and obj[1]
                            is not None]

            files_to_update.extend(
                metric_paths
            )
        return files_to_update

    def update_array_files(self):
        array_metrics = self.metrics_dao.get_fully_processed_metrics()

        if not array_metrics:
            logging.info('There are currently no array data files to update')
            return

        self.update_file_paths(
            file_paths=self.get_file_paths_from_metrics(
                metrics=array_metrics,
                metric_type=config.GENOME_TYPE_ARRAY
            )
        )

    def update_wgs_files(self):
        wgs_metrics = self.metrics_dao.get_fully_processed_metrics()

        if not wgs_metrics:
            logging.info('There are currently no wgs data files to update')
            return

        self.update_file_paths(
            file_paths=self.get_file_paths_from_metrics(
                metrics=wgs_metrics,
                metric_type=config.GENOME_TYPE_WGS
            )
        )

    def update_file_paths(self, file_paths: List[str]):

        logging.info(f'Updating {len(file_paths)} file path(s) to {self.storage_class} storage class')

        for file_path in file_paths:
            try:
                self.storage_provider.change_file_storage_class(
                    source_path=file_path,
                    storage_class=self.storage_class
                )
                self.updated_count += 1

            # pylint: disable=broad-except
            except Exception:
                logging.warning(f'Storage class update for {file_path} failed to update', exc_info=True)

        logging.info(f'{self.updated_count} genomic data files changed to {self.storage_class} storage class')
