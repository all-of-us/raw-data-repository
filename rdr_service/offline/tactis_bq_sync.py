import logging
import string
from typing import Union

from datetime import datetime

import google.cloud.bigquery
import google.api_core.exceptions
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

_logger = logging.getLogger("rdr_logger")


class TactisBQDataSync:

    EXTERNAL_CONNECTION = {
        "all-of-us-rdr-sandbox": "all-of-us-rdr-sandbox.us-central1.aou_sandbox_rdrmaindb",
        "all-of-us-rdr-stable": "all-of-us-rdr-stable.us-central1.all-of-us-rdr-stable-mysql",
        "all-of-us-rdr-prod": "all-of-us-rdr-prod.us-central1.bq-rdr-preprod-curation"
    }

    QUERIES = {
        "modified_participants": """
                        SELECT * FROM
                          EXTERNAL_QUERY("{external_connection}",
                            "SELECT t.participant_id, ps.first_name, ps.middle_name, ps.last_name, ps.email, """
                                 """ps.phone_number, ps.participant_origin FROM participant_data_to_tactis t """
                                 """LEFT JOIN participant_summary ps ON ps.participant_id = t.participant_id """
                                 """LEFT JOIN participant p ON p.participant_id = ps.participant_id """
                                 """WHERE p.is_test_participant = 0 AND p.is_ghost_id is null """
                                 """AND p.hpo_id not in (21) AND t.created >= '{since_date}'");"""
    }

    def __init__(self, dataset: string, table_name: string, since_date: datetime):
        self.client = bigquery.Client()
        self.dataset_id = f"{self.client.project}.{dataset}"
        self.table_name = table_name
        self.since_date = since_date
        self.external_connection = self.EXTERNAL_CONNECTION[self.client.project]

    def sync_data_to_bigquery(self):
        table_id = f"{self.dataset_id}.{self.table_name}"
        self.delete_table(table_id)
        self.import_table(table_id)

    def delete_table(self, table_id: string):
        # Delete the existing BQ table if it exists
        self.client.delete_table(table_id, not_found_ok=True)
        _logger.debug("Deleted table '{}'.".format(table_id))

    def run_query(self, sql: str, job_config: Union[None, google.cloud.bigquery.QueryJobConfig]):
        try:
            query_job = self.client.query(sql, job_config=job_config)
            result = query_job.result()  # wait for query to finish
            _logger.debug(f"Rows in result: {result.total_rows}")
        except NotFound as error:
            _logger.error("Dataset does not exist: %s", error)

    def import_table(self, destination: string):
        _logger.debug(f"Importing rdr.{self.table_name} to {destination}")
        job_config = bigquery.QueryJobConfig(destination=destination)
        sql = self.QUERIES['modified_participants'].format(external_connection=self.external_connection,
                                                           since_date=self.since_date)
        self.run_query(sql, job_config)
