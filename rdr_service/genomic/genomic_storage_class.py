import logging

from typing import List, Dict, Union

from rdr_service import config, clock
from rdr_service.dao.genomics_dao import GenomicGCValidationMetricsDao, GenomicDefaultBaseDao
from rdr_service.genomic.genomic_mappings import wgs_file_types_attributes, array_file_types_attributes
from rdr_service.genomic_enums import GenomicJob
from rdr_service.model.genomics import GenomicGCValidationMetrics, GenomicStorageUpdate
from rdr_service.storage import GoogleCloudStorageProvider


class GenomicStorageClass:

    def __init__(self,
                 storage_job_type,
                 storage_class='COLDLINE'
                 ):
        self.storage_job_type = storage_job_type
        self.storage_class = storage_class
        self.updated_count = 0

        self.storage_provider = GoogleCloudStorageProvider()
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
    def get_file_dict_from_metrics(cls, *,
                                   metrics: List[GenomicGCValidationMetrics],
                                   metric_type: str
                                   ) -> List[Dict[str, Union[int, list]]]:

        metric_file_type_map = {
            config.GENOME_TYPE_ARRAY: [obj['file_path_attribute'] for obj in array_file_types_attributes],
            config.GENOME_TYPE_WGS: [obj['file_path_attribute'] for obj in wgs_file_types_attributes]
        }[metric_type]

        files_to_update = []
        for metric in metrics:
            metric_obj = {'metric_id': metric.id,
                          'metric_paths': [obj[1] for obj in metric if 'Path' in obj[0] and obj[0] in
                                           metric_file_type_map and obj[1]
                                           is not None],
                          'metric_type': metric_type}

            files_to_update.append(
                metric_obj
            )
        return files_to_update

    def update_array_files(self):
        array_metrics = self.metrics_dao.get_fully_processed_metrics()

        if not array_metrics:
            logging.info('There are currently no array data files to update')
            return

        self.update_storage_class_for_file_paths(
            file_dict=self.get_file_dict_from_metrics(
                metrics=array_metrics,
                metric_type=config.GENOME_TYPE_ARRAY
            )
        )

    def update_wgs_files(self):
        wgs_metrics = self.metrics_dao.get_fully_processed_metrics(
            metric_type=config.GENOME_TYPE_WGS
        )

        if not wgs_metrics:
            logging.info('There are currently no wgs data files to update')
            return

        self.update_storage_class_for_file_paths(
            file_dict=self.get_file_dict_from_metrics(
                metrics=wgs_metrics,
                metric_type=config.GENOME_TYPE_WGS
            )
        )

    def update_storage_class_for_file_paths(self, file_dict: List[dict]):

        storage_objs = []
        for metrics_update in file_dict:
            metrics_id = metrics_update.get('metric_id')
            metrics_paths = metrics_update.get('metric_paths')

            insert_obj = {
                'metrics_id': metrics_id,
                'storage_class': self.storage_class,
                'genome_type': metrics_update.get('metric_type'),
                'created': clock.CLOCK.now(),
                'modified': clock.CLOCK.now()
            }

            logging.info(f"Updating metric_id: {metrics_id} "
                         f"{len(metrics_update)} file path("
                         f"s) to {self.storage_class} storage class")

            for metric_path in metrics_paths:
                try:
                    self.storage_provider.change_file_storage_class(
                        source_path=metric_path,
                        storage_class=self.storage_class
                    )
                    self.updated_count += 1

                # pylint: disable=broad-except
                except Exception as e:
                    logging.warning(f'Storage class update for {metric_path} failed to update: error: {e}',
                                    exc_info=True)
                    insert_obj['has_error'] = 1

            storage_objs.append(insert_obj)

        self.storage_update_dao.insert_bulk(storage_objs)

        logging.info(f'{self.updated_count} genomic data files changed to {self.storage_class} storage class')