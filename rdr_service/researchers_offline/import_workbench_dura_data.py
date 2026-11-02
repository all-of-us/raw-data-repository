from datetime import datetime, timedelta
from dateutil import parser
from google.cloud import bigquery
import logging

from rdr_service import config
from rdr_service.dao.workbench_dao import WorkbenchInstitutionalDuraDao
from rdr_service.services.redcap_client import RedcapClient

USER_SYSTEM = 'https://www.pmi-ops.org/redcap'


class WorkbenchDuraImporter:
    """Functionality for importing Workbench DURA data from REDCap"""

    def __init__(self, project='test'):
        self.workbench_dura_dao = WorkbenchInstitutionalDuraDao()
        self.client = bigquery.Client(project=project)
        self.redcap_api_key = config.getSettingJson(config.WB_INSTITUTIONAL_DURA_REDCAP_TOKEN)
        self.labels_table_id = config.getSettingJson(config.WB_DURA_LABELS_TABLE)
        self.codes_table_id = config.getSettingJson(config.WB_DURA_CODES_TABLE)
        self.dura_codes = config.getSettingJson(config.WB_DURA_CODES)
        self.destination_dataset = config.getSettingJson(config.VWB_DATAFEED_DATASET)

    def get_redcap_records(self, request_params, project_api_token, datetime_range_begin: datetime = None):
        if datetime_range_begin:
            request_params['dateRangeBegin'] = datetime_range_begin.strftime('%Y-%m-%d %H:%M:%S')
        redcap = RedcapClient()
        return redcap.send_request(project_api_token, 'record', request_params)

    def load_data_to_bigquery(self, data, table_id):
        # Load records into BigQuery
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # Start the asynchronous batch load job
        load_job = self.client.load_table_from_json(
            data, table_id, job_config=job_config
        )

        # Wait for the job to complete
        return load_job.result()

    def import_reports(self, since: str = None):
        """
        :param since: DateTime to use as start of date range request. Will import all reports created or modified
            after the given date. Defaults to the start (midnight) of yesterday.
            Job is set to ingest ALL DURA data from beginning of project unless otherwise needed - "2020-1-1"
        """
        if since is None or since == "":
            now_yesterday = datetime.now() - timedelta(days=1)
            parsed_since_date = datetime(now_yesterday.year, now_yesterday.month, now_yesterday.day)
        else:
            parsed_since_date = parser.parse(since)

        label_request_parameters = {
            'action': 'export',
            'type': 'flat',
            'csvDelimiter': '',
            'rawOrLabel': 'label',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': True,
            'exportSurveyFields': False,
            'exportDataAccessGroups': False,
        }
        raw_request_parameters = {
            'fields':self.dura_codes,
            'action': 'export',
            'type': 'flat',
            'csvDelimiter': '',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': True,
            'exportSurveyFields': False,
            'exportDataAccessGroups': False,
        }

        # Import DURA data with label values
        label_destination = f'{self.client.project}.{self.destination_dataset}.{self.label_table_id}'
        label_records = self.get_redcap_records(label_request_parameters, self.redcap_api_key, parsed_since_date)
        label_result = self.load_data_to_bigquery(label_records, label_destination)
        print(f"Loaded {len(label_records)} rows into {label_destination} with result: {label_result}")

        # Import DURA data with code values
        code_destination = f'{self.client.project}.{self.destination_dataset}.{self.codes_table_id}'
        raw_records = self.get_redcap_records(raw_request_parameters, self.redcap_api_key, parsed_since_date)
        raw_result = self.load_data_to_bigquery(raw_records, code_destination)
        print(f"Loaded {len(raw_records)} rows into {code_destination} with result: {raw_result}")

        logging.info('Workbench Institutional DURA import complete')
