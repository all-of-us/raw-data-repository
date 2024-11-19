import logging

from google.cloud import bigquery
from werkzeug.exceptions import BadRequest

from rdr_service import config
from rdr_service.dao.ppsc_partner_transfer_dao import PPSCDataTransferBaseDao
from rdr_service.model.ppsc_partner_data_transfer import PPSCCore, PPSCBiobankSample, PPSCHealthData, PPSCEHR
# from rdr_service.cloud_utils import bigquery
from rdr_service.workflow_management.ppsc import data_feed_queries

datafeeds = [
    "core data",
    "biospecimen",
    "healthdata sharing",
    "ehr"
]


class InputFeed:
    def __init__(self, project='test'):
        self.project = project
        self.bq_client = bigquery.Client()

    def make_datafeed_job(self, job_def):
        return self.bq_client.query(job_def)

    def get_datafeed_definition(self, datafeed):
        src = config.getSettingJson(config.PPSC_DATAFEED_SRC_DATASET)[0]
        destination = config.getSettingJson(config.PPSC_DATAFEED_DEST_DATASET)[0]

        job_def = {}

        if datafeed == "core data":
            job_def['staging_data'] = data_feed_queries.insert_core_data(self.project, src, destination)
            job_def['streaming_data'] = data_feed_queries.get_ppsc_core_to_stream(self.project, destination)
            job_def['output_model'] = PPSCCore
            return job_def

        elif datafeed == "biospecimen":
            job_def['staging_data'] = data_feed_queries.insert_biospecimen(self.project, src, destination)
            job_def['streaming_data'] = data_feed_queries.get_ppsc_biospecimen_to_stream(self.project, destination)
            job_def['output_model'] = PPSCBiobankSample
            return job_def

        elif datafeed == "health data sharing":
            job_def['staging_data'] = data_feed_queries.insert_health_data_sharing(self.project, src, destination)
            job_def['streaming_data'] = data_feed_queries.get_health_data_to_stream(self.project, destination)
            job_def['output_model'] = PPSCHealthData
            return job_def

        elif datafeed == "ehr":
            job_def['staging_data'] = data_feed_queries.insert_ehr_receipt(self.project, src, destination)
            job_def['streaming_data'] = data_feed_queries.get_ppsc_ehr_to_stream(self.project, destination)
            job_def['output_model'] = PPSCEHR
            return job_def

        else:
            # Raise error
            raise BadRequest(f"Invalid Datafeed: {datafeed}")

    def run_datafeed(self, datafeed):
        """
        Loads datafeed results in batches and commits updates to database per batch.
        """
        job_def = self.get_datafeed_definition(datafeed)

        # Stage the Data
        job = self.make_datafeed_job(job_def['staging_data'])

        if job is not None:
            logging.info(f"{datafeed} Data Feed Staged")
        else:
            logging.warning(f"Could not run {datafeed} Data Feed because of invalid config")

        streaming_data_rows = list(self.make_datafeed_job(job_def['streaming_data']))

        if streaming_data_rows:
            logging.info(f"{datafeed} Data Feed Staged")
            # Insert into Cloud SQL Table
            rows = [dict(row) for row in streaming_data_rows]
            dao = PPSCDataTransferBaseDao(job_def['output_model'])
            with dao.session() as session:
                session.bulk_insert_mappings(job_def['output_model'], rows)
        else:
            logging.warning(f"No Staged Rows for {datafeed} Data Feed")
