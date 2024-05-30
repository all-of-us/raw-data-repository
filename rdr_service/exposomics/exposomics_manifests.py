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

    def get_incremented_set_number(self) -> int:
        current_set: List[int] = self.dao.get_max_set()
        return 1 if current_set[0] is None else current_set[0] + 1

    def write_upload_manifest(self):
        try:
            with SqlExporter(self.bucket_name).open_cloud_writer(self.destination_path) as writer:
                writer.write_header(self.headers)
                writer.write_rows(self.source_data)

            logging.warning(f'The {self.manifest_type} was generated successfully: {self.fie_name}')
            return True

        except RuntimeError as e:
            logging.warning(f'An error occurred generating the {self.manifest_type} manifest: {e}')
            return False


class ExposomicsM0Workflow(ExposomicsGenerateManifestWorkflow):

    def __init__(self, form_data: dict, sample_list: List[dict]):
        self.form_data = form_data
        self.sample_list = sample_list
        self.manifest_type = 'mO'
        self.dao = ExposomicsM0Dao()
        self.bucket_name = config.BIOBANK_SAMPLES_BUCKET_NAME
        self.destination_path = f'/{config.EXPOSOMICS_MO_MANIFEST_SUBFOLDER}'
        self.file_name = None
        self.set_num = None
        self.source_data = []
        self.headers = []

    def generate_filename(self):
        now_formatted = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")
        return (f'AoU_m0_{self.form_data.get("sample_type")}'
                f'_{self.form_data.get("unique_study_identifier")}'
                f'_{now_formatted}_{self.set_num}.csv')

    def get_source_data(self):
        return self.dao.get_manifest_data(form_data=self.form_data, sample_list=self.sample_list)

    def store_manifest_data(self):
        manifest_data = {
            'file_path': f'{self.bucket_name}/{self.destination_path}/',
            'file_data': self.source_data,
            'file_name': self.file_name,
            'bucket_name': self.bucket_name,
            'exposomics_set': self.set_num
        }
        self.dao.insert(self.dao.model_type(**manifest_data))

    def generate_manifest(self):
        self.set_num = self.get_incremented_set_number()
        self.file_name = self.generate_filename()
        self.source_data = self.get_source_data()

        if not self.source_data:
            logging.warning('There were no results returned for the M0 generation')
            return

        updated_records = []
        for el in self.source_data:
            current_record = el._asdict()
            sample_id = self.get_sample_id_from_list(
                biobank_id=self.dao.extract_prefix_from_val(
                    current_record.get('biobank_id'))
            )

            if sample_id:
                current_record['sample_id'] = sample_id
                updated_records.append(current_record)

        self.headers = updated_records[0].keys()
        self.source_data = updated_records

        manifest_created = self.write_upload_manifest()
        if manifest_created:
            self.store_manifest_data()

    def get_sample_id_from_list(self, *, biobank_id: int):
        try:
            return list(filter(lambda x: int(x.get('biobank_id')) == int(biobank_id), self.sample_list))[0].get(
                'sample_id')
        except IndexError:
            logging.warning(f'Can not find sample_id for biobank_id: {biobank_id}')
            return None

