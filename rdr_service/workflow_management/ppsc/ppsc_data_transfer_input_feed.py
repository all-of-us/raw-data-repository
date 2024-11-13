import logging

from werkzeug.exceptions import BadRequest

from rdr_service import config
from rdr_service.cloud_utils import bigquery
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

    def make_datafeed_job(self, job_def):
        return bigquery.BigQueryJob(job_def)

    def get_datafeed_definition(self, datafeed):
        src = config.getSettingJson(config.PPSC_DATAFEED_SRC_DATASET)[0]
        destination = config.getSettingJson(config.PPSC_DATAFEED_DEST_DATASET)[0]

        if datafeed == "core data":
            return data_feed_queries.insert_core_data(self.project, src, destination)

        elif datafeed == "biospecimen":
            return data_feed_queries.insert_biospecimen(self.project, src, destination)

        elif datafeed == "health data sharing":
            return data_feed_queries.insert_health_data_sharing(self.project, src, destination)

        elif datafeed == "ehr":
            return data_feed_queries.insert_ehr_receipt(self.project, src, destination)

        else:
            # Raise error
            raise BadRequest(f"Invalid Datafeed: {datafeed}")

    def run_datafeed(self, datafeed):
        """
        Loads datafeed results in batches and commits updates to database per batch.
        """
        job_def = self.get_datafeed_definition(datafeed)

        job = self.make_datafeed_job(job_def)

        if job is not None:
            logging.info(f"{datafeed} Data Feed completed")
        else:
            logging.warning(f"Could not run {datafeed} Data Feed because of invalid config")
