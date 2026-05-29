import argparse
import logging
import sys

from rdr_service.researchers_offline.import_workbench_dura_data import WorkbenchDuraImporter
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

tool_cmd = "dura-test"
tool_desc = "Tool to run the Workbench Institutional DURA Import"


class WorkbenchInstitutionalDuraTool(object):
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
        importer = WorkbenchDuraImporter()
        importer.import_reports()


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
        process = WorkbenchInstitutionalDuraTool(args, gcp_env)
        process.run()


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
