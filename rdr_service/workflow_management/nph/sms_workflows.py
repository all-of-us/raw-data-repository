import csv
import logging

from protorpc import messages

from rdr_service import config
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.study_nph_sms_dao import SmsJobRunDao, SmsSampleDao, SmsN0Dao, SmsN1Mc1Dao
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.workflow_management.general_job_controller import JobController
from rdr_service.services.ancillary_studies.nph_incident import NphIncidentDao, create_nph_incident
from rdr_service.workflow_management.nph.sms_validation import SmsValidator


class SmsJobId(messages.Enum):
    UNSET = 0
    FILE_INGESTION = 1
    FILE_GENERATION = 2


class SmsFileTypes(messages.Enum):
    SAMPLE_LIST = 1
    N0 = 100
    N1_MC1 = 101
    N1_MCC = 102


class SmsJobController(JobController):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.subprocess = kwargs.get("subprocess")


class SmsWorkflow:

    def __init__(self,  workflow_def: dict):
        self.job_run_dao = SmsJobRunDao()
        self.incident_dao = NphIncidentDao()
        self.job = SmsJobId.lookup_by_name(workflow_def['job'])
        self.file_type = SmsFileTypes.lookup_by_name(workflow_def['file_type'])
        self.file_path = workflow_def.get('file_path')
        self.recipient = workflow_def.get('recipient')
        self.package_id = workflow_def.get('package_id')
        self.job_run = None
        self.file_transfer_def = None
        self.file_dao = None
        self.env = workflow_def.get('env')
        self.validator = SmsValidator(self.file_path, self.recipient)

        # Job Map
        self.process_map = {
            SmsJobId.FILE_INGESTION: self.job_ingestion,
            SmsJobId.FILE_GENERATION: self.job_generation,
        }

    @staticmethod
    def read_data_from_cloud_manifest(path: str) -> dict:
        """
        Opens a cloud manifest file at `path`
        Converts headers of file to lowercase since partners choose to capitalize arbitrarily
        returns dictionary
        :param path:
        :return: dict
        """

        with open_cloud_file(path, 'r') as csv_file:

            def clean_file_header(header: str) -> str:
                return header.strip().lower()

            data_to_ingest = {'rows': []}
            csv_reader = csv.DictReader(csv_file, delimiter=",")
            csv_reader.fieldnames = list(map(clean_file_header, csv_reader.fieldnames))
            data_to_ingest['fieldnames'] = csv_reader.fieldnames
            for row in csv_reader:
                for key in row.copy():
                    if not key:
                        del row[key]
                data_to_ingest['rows'].append(row)
            return data_to_ingest

    def validate_columns(self, fieldnames, dao):
        """ Simple check for column names in the model """
        unstored_columns = ['blank']
        expected_columns = dao.model_type.__table__.columns.keys() + unstored_columns
        for column_name in fieldnames:
            if column_name not in expected_columns:
                raise AttributeError(f"{self.file_path}: {column_name} column mismatch for "
                                     f"expected file type: {self.file_type.name}")

    def export_data_to_cloud(self, source_data):
        # Use SQL exporter
        exporter = SqlExporter(self.file_transfer_def['bucket'])
        self.file_path = f"{self.file_transfer_def['bucket']}/{self.file_transfer_def['file_name']}"

        with exporter.open_cloud_writer(self.file_transfer_def['file_name'],
                                        delimiter=self.file_transfer_def['delimiter']) as writer:
            writer.write_header(source_data[0].keys())
            writer.write_rows(source_data)

    def write_data_to_manifest_table(self, data_to_write):
        for record in data_to_write:
            if not isinstance(record, dict):
                record = record._asdict()

            additional_columns = {
                "file_path": self.file_path,
                "job_run_id": self.job_run.id
            }
            record.update(additional_columns)
            model_obj = self.file_dao.get_model_obj_from_items(record.items())
            self.file_dao.insert(model_obj)

    def convert_empty_string_to_none(self, rows):
        for row in rows:
            for key, value in row.items():
                if value == "":
                    row[key] = None
        return rows

    def execute_workflow(self):
        """
        Entrypoint for SMS Workflow execution.
        Creates a SmsJobController and determines which process to run
        :return:
        """
        logging.info(f"called {self.job} with {self.file_type}")

        job_params = {
            "job": self.job,
            "job_run_dao": self.job_run_dao,
            "incident_dao": self.incident_dao,
            "subprocess": self.file_type
        }

        with SmsJobController(**job_params) as controller:
            self.job_run = controller.job_run

            try:
                self.process_map[self.job]()
                controller.job_run_result = controller.run_result_enum.SUCCESS
            except KeyError:
                raise KeyError

    def job_ingestion(self):
        """
        Main method for ingestion jobs.
        """

        file_end = self.file_path.split(".")[-1]
        # Ensure file is CSV
        if not file_end == ".csv":
            if config.getSettingJson(config.NPH_SLACK_WEBHOOKS, {}):
                kwargs = {
                    "slack": True,
                    "message": f"Error ingesting nph file of type {file_end}. "
                               f"File '{self.file_path}' does not conform to csv format."
                }
                create_nph_incident(**kwargs)
            else:
                logging.warning(
                    msg=f"Error ingesting nph file of type {file_end}. "
                        f"File '{self.file_path}' does not conform to csv format."
                )
            return

        # Map a file type to a DAO
        if self.file_type == SmsFileTypes.SAMPLE_LIST:
            self.file_dao = SmsSampleDao()
        elif self.file_type == SmsFileTypes.N0:
            self.file_dao = SmsN0Dao()
        else:
            self.file_dao = None

        if self.file_dao:
            # look up if any rows exist already for the file
            records = self.file_dao.get_from_filepath(self.file_path)
            if records:
                logging.warning(f'File already ingested: {self.file_path}')
                return

            data_to_ingest = self.read_data_from_cloud_manifest(self.file_path)
            self.validate_columns(data_to_ingest["fieldnames"], self.file_dao)
            cleaned_rows = self.convert_empty_string_to_none(data_to_ingest["rows"])

            if self.file_type == SmsFileTypes.SAMPLE_LIST:
                try:
                    self.validator.validate_pull_list(cleaned_rows)
                except ValueError as e:
                    logging.error(f"Validation failed: {e}")
                    return

            self.write_data_to_manifest_table(cleaned_rows)

    def job_generation(self):
        """
        Main method for generation jobs.
        """
        # Map a file type to a DAO
        if self.file_type == SmsFileTypes.N1_MC1:
            self.file_dao = SmsN1Mc1Dao()
        else:
            self.file_dao = None

        if self.file_dao:
            source_data = self.file_dao.source_data(recipient=self.recipient, package_id=self.package_id)
            if source_data:
                try:
                    self.validator.validate_n1(source_data)
                except ValueError as e:
                    logging.error(f"Validation failed: {e}")
                    return

                self.file_transfer_def = self.file_dao.get_transfer_def(recipient=self.recipient, env=self.env)
                self.export_data_to_cloud(source_data)
                self.write_data_to_manifest_table(source_data)
                logging.info(
                    f"N1 successfully generated for package id {self.package_id} and written to the table."
                )
