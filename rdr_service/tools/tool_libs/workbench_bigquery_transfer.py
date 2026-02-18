import argparse
import logging
import sys

from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.workflow_management.researchers_offline.workbench_data_transfer_input_feed import \
    WorkbenchWorkspacesFeed

_logger = logging.getLogger("rdr_logger")

tool_cmd = "rwb-test"
tool_desc = "Tool to run the Workbench Workspaces Data Feed"


class WorkbenchWorkspacesTool(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.project = args.project

    def run(self):
        self.gcp_env.activate_sql_proxy()
        datafeed = "WorkbenchWorkspacesInputFeed"
        input_feed = WorkbenchWorkspacesFeed(project=self.project)
        input_feed.run_datafeed(datafeed)


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
        process = WorkbenchWorkspacesTool(args, gcp_env)
        process.run()


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
