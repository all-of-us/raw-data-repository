#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys
from os.path import isabs, exists

from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.offline.study_nph_ingest_awardee_data import create_sites_from_csv
from rdr_service.tools.tool_libs.tool_base import ToolBase


_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "study_nph_ingest_org_awardee_site_data_from_csv"
tool_desc = "NPH Study ingest organization awardee site data from csv file"


class IngestNphOrgAwardeeSiteDataFromCsv(ToolBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super().__init__(args, gcp_env)

    def run(self, csv_filepath: str):
        """
        Main program process
        :return: Exit code value
        """
        if self.args.project == 'all-of-us-rdr-prod':
            _logger.error(f'Nph Site Ingest Process cannot be used on project: {self.args.project}')
            return 1
        self.gcp_env.activate_sql_proxy()
        create_sites_from_csv(csv_filepath=csv_filepath)
        # TODO: write program main process here after setting 'tool_cmd' and 'tool_desc'...
        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--csv-file", help="NPH Organization Awardee Site Data File")
    args = parser.parse_args()

    if not isabs(args.csv_file):
        raise ValueError(f"'{args.awardee_file_path}' is not an absolute file path")

    if not exists(args.csv_file):
        raise IOError(f"'{args.awardee_file_path}' filepath does not exist")

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = IngestNphOrgAwardeeSiteDataFromCsv(args, gcp_env)
        exit_code = process.run(args.csv_file)
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
