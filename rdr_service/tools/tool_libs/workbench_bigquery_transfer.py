import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys

from google import auth
from google.auth import impersonated_credentials
from google.cloud import bigquery
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.workflow_management.researchers_offline import data_feed_queries
from rdr_service.workflow_management.researchers_offline.workbench_data_transfer_input_feed import \
    WorkbenchWorkspacesFeed

_logger = logging.getLogger("rdr_logger")

tool_cmd = "rwb-test"
tool_desc = "put tool help description here"


class ProgramTemplateClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.project = args.project

        # target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        # stable_principal = "placid-researcher-workbench@all-of-us-rdr-stable.iam.gserviceaccount.com"
        # # prod_principal = "placid-researcher-workbench@all-of-us-rdr-prod.iam.gserviceaccount.com"
        #
        # source_credentials, project = auth.default()
        # creds = impersonated_credentials.Credentials(
        #     source_credentials=source_credentials,
        #     target_principal=stable_principal,
        #     target_scopes=target_scopes,
        # )
        # self.bq_client = bigquery.Client(credentials=creds, project=project)

    # def make_datafeed_job(self, job_def: str):
    #     """Runs the query in BQ and returns the result."""
    #     return self.bq_client.query(job_def).result()

    def run(self):
        datafeed = "WorkbenchWorkspacesInputFeed"
        input_feed = WorkbenchWorkspacesFeed(project=self.project)
        input_feed.run_datafeed(datafeed)

        # START Function - set p impersonation credentials
        # target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        # stable_principal = "placid-researcher-workbench@all-of-us-rdr-stable.iam.gserviceaccount.com"
        # prod_principal = "placid-researcher-workbench@all-of-us-rdr-prod.iam.gserviceaccount.com"
        #
        # source_credentials, project = auth.default()
        # creds = impersonated_credentials.Credentials(
        #     source_credentials=source_credentials,
        #     target_principal=stable_principal,
        #     target_scopes=target_scopes,
        # )
        # bq_client = bigquery.Client(credentials=creds, project=project)
        # END Function

        # src = ""
        # destination = "rdr_workbench"

        ### Approach 2
        # job_def = {
        #     # "export_data_sql": data_feed_queries.test_export_data_workbench_workspaces_staging_data(),
        #     # "create_external_table_sql": data_feed_queries.test_external_table_workbench_workspaces_staging_data(),
        #     "staging_data_sql": data_feed_queries.test_insert_workbench_workspaces_staging_data(
        #         self.bq_client.project, destination
        #     )
        # }
        #
        # # self.make_datafeed_job(job_def["export_data_sql"])
        # # staging_data_rows = self.make_datafeed_job(job_def["create_external_table_sql"])
        # staging_data_rows = self.bq_client.query((job_def["staging_data_sql"])).result()
        #
        # ### Approach 1
        # # sql = data_feed_queries.test_insert_workbench_workspaces_staging_data(project, src, destination)
        # # job_config = bigquery.QueryJobConfig()
        # # results = bq_client.query(sql, job_config)
        #
        # if staging_data_rows:
        #     for row in staging_data_rows:
        #         x = dict(row.items())
        #         print(x)

        # for record in results:
        #     x = dict(record.items())
        #     print(x)

def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project) as gcp_env:
        process = ProgramTemplateClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
