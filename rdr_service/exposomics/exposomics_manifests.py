import csv
import logging
from abc import ABC, abstractmethod
from rdr_service import config, clock
from typing import List

from rdr_service.api_util import open_cloud_file
from rdr_service.config import BIOBANK_SAMPLES_BUCKET_NAME, HHEAR_BUCKET_NAME
from rdr_service.dao.exposomics_dao import ExposomicsM0Dao, ExposomicsM1Dao
from rdr_service.offline.sql_exporter import SqlExporter


class ExposomicsManifestWorkflow(ABC):

    @abstractmethod
    def store_manifest_data(self):
        ...


class ExposommicsIngestManifestWorkflow(ExposomicsManifestWorkflow):

    def retrieve_source_data(self):
        try:
            with open_cloud_file(self.file_path) as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter=",")
                return [obj for obj in csv_reader]
        except FileNotFoundError:
            logging.warning(f"File path '{self.file_path}' not found")

    @abstractmethod
    def ingest_manifest(self):
        ...


class ExposomicsGenerateManifestWorkflow(ExposomicsManifestWorkflow):

    @abstractmethod
    def generate_manifest(self):
        ...

    @abstractmethod
    def generate_filename(self):
        ...

    @abstractmethod
    def get_source_data(self):
        ...

    def convert_source_data(self):
        return [el._asdict() for el in self.source_data]

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
        self.bucket_name = config.getSetting(BIOBANK_SAMPLES_BUCKET_NAME)
        self.destination_path = f'{config.EXPOSOMICS_MO_MANIFEST_SUBFOLDER}'
        self.file_name = None
        self.set_num = set_num
        self.source_data = []
        self.headers = []

    def generate_filename(self):
        now_formatted = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")
        return (f'AoU_{self.manifest_type}_{self.form_data.get("sample_type")}'
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
            'file_data': self.convert_source_data(),
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

        self.headers = self.source_data[0]._asdict().keys()

        manifest_created = self.write_upload_manifest()
        if manifest_created:
            self.store_manifest_data()


class ExposomicsM1CopyWorkflow(ExposomicsGenerateManifestWorkflow):

    def __init__(self, copy_file_path):
        self.manifest_type = 'm1'
        self.dao = ExposomicsM1Dao()
        self.copy_file_path = copy_file_path
        self.bucket_name = config.getSetting(HHEAR_BUCKET_NAME)
        # self.destination_path = f'{config.EXPOSOMICS_MO_MANIFEST_SUBFOLDER}'
        # self.file_name = None
        # self.set_num = set_num
        # self.source_data = []
        # self.headers = []

    def generate_filename(self):
        return self.copy_file_path.split('/')[-1]

    def get_source_data(self):
        return self.dao.get_manifest_data(
            file_path=self.copy_file_path
        )

    def store_manifest_data(self):
        ...
        # manifest_data = {
        #     'file_path': f'{self.bucket_name}/{self.destination_path}/{self.file_name}',
        #     'file_data': self.convert_source_data(),
        #     'file_name': self.file_name,
        #     'bucket_name': self.bucket_name,
        #     'exposomics_set': self.set_num
        # }
        # self.dao.insert(self.dao.model_type(**manifest_data))

    def generate_manifest(self):
        ...
        # self.file_name = self.generate_filename()
        # self.source_data = self.get_source_data()
        #
        # if not self.source_data:
        #     logging.warning('There were no results returned for the M0 generation')
        #     return
        #
        # self.headers = self.source_data[0]._asdict().keys()
        #
        # manifest_created = self.write_upload_manifest()
        # if manifest_created:
        #     self.store_manifest_data()


class ExposomicsM1Workflow(ExposommicsIngestManifestWorkflow):

    def __init__(self, file_path: str):
        self.dao = ExposomicsM1Dao()
        self.file_path = file_path
        self.source_data = self.retrieve_source_data()

    def store_manifest_data(self):
        file_path_data = self.file_path.split('/')
        manifest_data = {
            'file_path': self.file_path,
            'file_data': self.retrieve_source_data(),
            'file_name': file_path_data[-1],
            'bucket_name': file_path_data[0]
        }
        self.dao.insert(self.dao.model_type(**manifest_data))

    def ingest_manifest(self):
        self.store_manifest_data()
        ExposomicsM1CopyWorkflow(copy_file_path=self.file_path).generate_manifest()
