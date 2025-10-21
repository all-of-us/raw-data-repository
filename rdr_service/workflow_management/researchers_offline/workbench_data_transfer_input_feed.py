import logging
from google.auth import default, impersonated_credentials
from google.cloud import bigquery

from rdr_service import clock, config
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao
from rdr_service.dao.metadata_dao import WORKBENCH_LAST_SYNC_KEY, MetadataDao
from rdr_service.model.workbench_workspace import WorkbenchWorkspaceSnapshot
from rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed import PPSCBigQueryDatafeedBase
from rdr_service.workflow_management.researchers_offline import data_feed_queries


class WorkbenchWorkspacesFeed(PPSCBigQueryDatafeedBase):

    def __init__(self, project='test'):
        target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        stable_principal = "placid-researcher-workbench@all-of-us-rdr-stable.iam.gserviceaccount.com"
        # prod_principal = "placid-researcher-workbench@all-of-us-rdr-prod.iam.gserviceaccount.com"

        source_credentials, default_project = default()
        creds = impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=stable_principal,  # config.getSettingJson(config.WORKBENCH_SERVICE_ACCOUNT)[0]
            target_scopes=target_scopes,
        )

        self.project = project
        self.bq_client = bigquery.Client(credentials=creds, project=project)

    def make_datafeed_job(self, job_def: str):
        """Runs the query in BQ and returns the result."""
        return self.bq_client.query(job_def).result()

    def get_datafeed_definition(self) -> dict:
        # src = config.getSettingJson(config.WORKBENCH_DATAFEED_SRC_DATASET)[0]
        multi_region_destination = "rdr_workbench_multi_region"  # config.getSettingJson(config.WORKBENCH_DATAFEED_MULTI_REGION__DATASET)[0]
        single_region_destination = "rdr_workbench"  # config.getSettingJson(config.WORKBENCH_DATAFEED_SINGLE_REGION_DATASET)[0]
        mapping_table = "workspace_source_id_mapping"
        data_transfer_table = "workbench_data_transfer_external"
        last_job_run_date = "2025-01-01"

        job_def = {
            # Query to test initial job set up
            # "testing_sql": data_feed_queries.test_insert_workbench_workspaces_staging_data(
            #     self.project, destination, last_job_run_date
            # ),
            # Query to get data from Workbench
            "staging_data_sql": data_feed_queries.insert_workbench_workspaces_staging_data(
                self.project, multi_region_destination, last_job_run_date
            ),
            "export_multi_region_data_sql": data_feed_queries.export_workbench_workspaces_staging_data_multi_region(
                self.project, multi_region_destination
            ),
            # Query to place Workbench data in the correct region
            "staging_single_region_data_sql": data_feed_queries.insert_workbench_workspaces_staging_data_multi_region(
                self.project, single_region_destination
            ),
            # Query to insert new source_id mappings
            "create_mapping_sql": data_feed_queries.create_workspace_source_id_mapping(
                self.project, single_region_destination, mapping_table, data_transfer_table
            ),
            # Query to stream new data to SQL
            "streaming_data_sql": data_feed_queries.get_workbench_workspaces_data_to_stream(
                self.project, single_region_destination, mapping_table, data_transfer_table
            ),
            "destination_model": WorkbenchWorkspaceSnapshot,
        }

        return job_def

    @staticmethod
    def row_to_dict(row: bigquery.Row) -> dict:
        row_dict = {}
        for key in row.keys():
            row_dict[key] = row[key]
        return row_dict

    def run_datafeed(self, datafeed: str) -> None:
        logging.info(f"Running {datafeed} Data Feed...")

        # now = clock.CLOCK.now()
        # metadata_dao = MetadataDao()
        # metadata_dao.upsert(WORKBENCH_LAST_SYNC_KEY, date_value=now)

        datafeed_def = self.get_datafeed_definition()
        # streaming_data_rows = self.make_datafeed_job(datafeed_def["testing_sql"])
        self.make_datafeed_job(datafeed_def["staging_data_sql"])  # Stage data rows in multi region table
        self.make_datafeed_job(datafeed_def["export_multi_region_data_sql"])  # Export data to Cloud Storage
        self.make_datafeed_job(datafeed_def["staging_single_region_data_sql"])  # Stage data rows in single region table
        self.make_datafeed_job(datafeed_def["create_mapping_sql"])  # Insert new source_id mappings
        streaming_data_rows = self.make_datafeed_job(datafeed_def["streaming_data_sql"])  # Stream data rows to MySQL

        dao = WorkbenchWorkspaceDao()
        if streaming_data_rows:
            for row in streaming_data_rows:
                workspaces_dict = WorkbenchWorkspacesFeed.row_to_dict(row)
                # camel_case_awardee_insite_dict = {
                #     dao.snake_to_camel_case(key): val for key, val in workspaces_dict.items()
                # }
        #         # insert_bulk
                x = "test"
        else:
            logging.warning(f"No Staged Rows for {datafeed} Data Feed")

        streaming_data_rows = list(self.make_datafeed_job(datafeed_def["streaming_data_sql"]))

        if streaming_data_rows:
            logging.info(f"{datafeed} Data Feed Staged")
            # Insert into Cloud SQL Table
            rows = [dict(row) for row in streaming_data_rows]
            x = "test"
            # with dao.session() as session:
            #     session.bulk_insert_mappings(job_def['output_model'], rows)

            #         # Support RDR to PDR pipeline
            #         submit_pipeline_pubsub_msg_from_model(result, self.dao.get_connection_database_name())
        else:
            logging.warning(f"No rows to add to {datafeed} Data Feed")
