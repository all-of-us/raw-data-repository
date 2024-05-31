import logging
from abc import ABC, abstractmethod
from rdr_service import config, clock
from typing import List

from rdr_service.dao.exposomics_dao import ExposomicsM0Dao
from rdr_service.offline.sql_exporter import SqlExporter


class ExposomicsManifestWorkflow(ABC):

    @abstractmethod
    def get_source_data(self):
        ...

    @abstractmethod
    def store_manifest_data(self):
        ...


class ExposomicsGenerateManifestWorkflow(ExposomicsManifestWorkflow):

    @abstractmethod
    def generate_manifest(self):
        ...

    @abstractmethod
    def generate_filename(self):
        ...

    def write_upload_manifest(self):
        try:
            with SqlExporter(self.bucket_name).open_cloud_writer(f'{self.destination_path}/{self.file_name}') as writer:
                writer.write_header(self.headers)
                writer.write_rows(self.source_data)

            logging.warning(f'The {self.manifest_type} was generated successfully: {self.file_name}')
            return True

        except RuntimeError as e:
            logging.warning(f'An error occurred generating the {self.manifest_type} manifest: {e}')
            return False


class ExposomicsM0Workflow(ExposomicsGenerateManifestWorkflow):

    def __init__(self, form_data: dict, sample_list: List[dict], set_num: int):
        self.form_data = form_data
        self.sample_list = sample_list
        self.manifest_type = 'mO'
        self.dao = ExposomicsM0Dao()
        self.bucket_name = config.BIOBANK_SAMPLES_BUCKET_NAME
        self.destination_path = f'{config.EXPOSOMICS_MO_MANIFEST_SUBFOLDER}'
        self.file_name = None
        self.set_num = set_num
        self.source_data = []
        self.headers = []

    def generate_filename(self):
        now_formatted = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")
        return (f'AoU_m0_{self.form_data.get("sample_type")}'
                f'_{self.form_data.get("unique_study_identifier")}'
                f'_{now_formatted}_{self.set_num}.csv')

    def get_source_data(self):
        return self.dao.get_manifest_data(
            form_data=self.form_data,
            sample_list=self.sample_list,
            set_num=self.set_num
        )

    def store_manifest_data(self):
        manifest_data = {
            'file_path': f'{self.bucket_name}/{self.destination_path}/{self.file_name}',
            'file_data': self.source_data,
            'file_name': self.file_name,
            'bucket_name': self.bucket_name,
            'exposomics_set': self.set_num
        }
        self.dao.insert(self.dao.model_type(**manifest_data))

    def generate_manifest(self):
        self.file_name = self.generate_filename()
        self.source_data = self.get_source_data()

        if not self.source_data:
            logging.warning('There were no results returned for the M0 generation')
            return

        self.headers = ['biobank_id', 'sample_type'] # ADD THIS

        manifest_created = self.write_upload_manifest()
        if manifest_created:
            self.store_manifest_data()

