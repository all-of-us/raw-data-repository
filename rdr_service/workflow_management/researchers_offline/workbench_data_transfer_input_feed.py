import logging
from google.cloud import bigquery

from rdr_service import clock, config
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao, WorkbenchResearcherDao
from rdr_service.dao.metadata_dao import WORKBENCH_LAST_SYNC_KEY, MetadataDao
from rdr_service.model.workbench_researcher import WorkbenchResearcher
from rdr_service.model.workbench_workspace import WorkbenchWorkspaceSnapshot
from rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed import PPSCBigQueryDatafeedBase
from rdr_service.workflow_management.researchers_offline import data_feed_queries


class WorkbenchWorkspacesFeed(PPSCBigQueryDatafeedBase):

    def __init__(self, project='test'):
        self.project = project
        self.bq_client = bigquery.Client()

    def make_datafeed_job(self, job_def: str):
        """Runs the query in BQ and returns the result."""
        return self.bq_client.query(job_def).result()

    def get_datafeed_definition(self) -> dict:
        vwb_dataset = config.getSettingJson(config.VWB_DATAFEED_DATASET, ['rdr_workbench'])[0]
        src_table = config.getSettingJson(config.VWB_WORKSPACES_SRC_TABLE, ['v_all_wsm_workspaces_expanded'])[0]
        mapping_table = config.getSettingJson(config.VWB_WORKSPACES_ID_MAPPING_TABLE,
                                              ['workspace_source_id_mapping'])[0]
        wb_source_table = config.getSettingJson(config.WB_WORKSPACES_SRC_TABLE, ['v_wb_workspaces_expanded'])[0]

        job_def = {
            # Query to insert new source_id mappings
            "create_mapping_sql": data_feed_queries.create_workspace_source_id_mapping(
                self.project, vwb_dataset, mapping_table, src_table, wb_source_table
            ),
            # Query to stream 2.0 data to MySQL
            "streaming_data_sql": data_feed_queries.get_workbench_workspaces_data_to_stream(
                self.project, vwb_dataset, mapping_table, src_table, wb_source_table
            ),
            "destination_model": WorkbenchWorkspaceSnapshot,
            # Query to stream 1.0 data to MySQL
            "legacy_data_sql": data_feed_queries.get_legacy_workbench_workspaces_data_to_stream(
                self.project, vwb_dataset, wb_source_table
            ),
        }

        return job_def

    def run_datafeed(self, datafeed: str) -> None:
        logging.info(f"Running {datafeed} Data Feed...")

        metadata_dao = MetadataDao()
        metadata_dao.upsert(WORKBENCH_LAST_SYNC_KEY, date_value=clock.CLOCK.now())

        datafeed_def = self.get_datafeed_definition()
        self.make_datafeed_job(datafeed_def["create_mapping_sql"])
        streaming_data_rows = list(self.make_datafeed_job(datafeed_def["streaming_data_sql"]))

        dao = WorkbenchWorkspaceDao()
        if streaming_data_rows:
            for row in streaming_data_rows:
                now = clock.CLOCK.now()
                workspaces_dict = dao.bq_row_to_dict(row, now)
                dao.insert([WorkbenchWorkspaceSnapshot(**workspaces_dict)])

    def run_legacy_datafeed(self, datafeed: str) -> None:
        logging.info(f"Running {datafeed} Data Feed...")

        metadata_dao = MetadataDao()
        metadata_dao.upsert(WORKBENCH_LAST_SYNC_KEY, date_value=clock.CLOCK.now())

        datafeed_def = self.get_datafeed_definition()
        streaming_data_rows = list(self.make_datafeed_job(datafeed_def["legacy_data_sql"]))

        dao = WorkbenchWorkspaceDao()
        if streaming_data_rows:
            for row in streaming_data_rows:
                now = clock.CLOCK.now()
                workspaces_dict = dao.bq_row_to_dict(row, now)
                dao.insert([WorkbenchWorkspaceSnapshot(**workspaces_dict)])


class WorkbenchResearchersFeed(PPSCBigQueryDatafeedBase):

    def __init__(self, project='test'):
        self.project = project
        self.bq_client = bigquery.Client()

    def make_datafeed_job(self, job_def: str):
        """Runs the query in BQ and returns the result."""
        return self.bq_client.query(job_def).result()

    def get_datafeed_definition(self) -> dict:
        vwb_dataset = config.getSettingJson(config.VWB_DATAFEED_DATASET, ['rdr_workbench'])[0]
        src_table = config.getSettingJson(config.WB_RESEARCHERS_SRC_TABLE, ['v_researchers_expanded'])[0]

        job_def = {
            # Query to stream new data to MySQL
            "streaming_data_sql": data_feed_queries.get_workbench_researchers_data_to_stream(
                self.project, vwb_dataset, src_table
            ),
            "destination_model": WorkbenchResearcher,
        }

        return job_def

    def run_datafeed(self, datafeed: str) -> None:
        logging.info(f"Running {datafeed} Data Feed...")

        datafeed_def = self.get_datafeed_definition()
        streaming_data_rows = list(self.make_datafeed_job(datafeed_def["streaming_data_sql"]))

        dao = WorkbenchResearcherDao()
        if streaming_data_rows:
            for row in streaming_data_rows:
                now = clock.CLOCK.now()
                researchers_dict = dao.bq_row_to_dict(row, now)
                dao.insert([WorkbenchResearcher(**researchers_dict)])
