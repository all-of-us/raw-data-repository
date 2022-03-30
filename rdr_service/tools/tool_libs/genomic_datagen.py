#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import csv
import logging
import os
import sys

from rdr_service.services.system_utils import setup_logging, setup_i18n

# from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.tools.tool_libs.tool_base import ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "genomic_datagen"
tool_desc = ""


class ParticipantGeneratorTool(ToolBase):

    def run(self):
        self.gcp_env.activate_sql_proxy()
        _ = self.get_server_config()


def output_local_csv(*, filename, data):
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[k for k in data[0]])
        writer.writeheader()
        writer.writerows(data)

    _logger.info(f'Generated failures csv: {os.getcwd()}/{filename}')


def get_datagen_process_for_run(args, gcp_env):
    datagen_map = {
        'participants': ParticipantGeneratorTool(args, gcp_env),
    }
    return datagen_map.get(args.util)


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
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa

    subparser = parser.add_subparsers(help='', dest='process')

    participants = subparser.add_parser("participants")
    participants.add_argument("--test-project", help="", default='', required=True) # noqa
    participants.add_argument("--output-only-run-id", help="", default=None)  # noqa
    participants.add_argument("--output-only-sample-ids", help="", default=None)  # noqa
    participants.add_argument("--spec-path", help="", default=None)  # noqa
    participants.add_argument("--output-template-name", help="", default=None)  # noqa
    participants.add_argument("--output-file-directory", help="", default=None)  # noqa

    # args = parser.parse_args()

    # with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
    #     try:
    #         datagen_process = get_datagen_process_for_run(args, gcp_env)
    #         exit_code = datagen_process.run()
    #     except Exception as e:
    #         _logger.info(f'Error has occured, {e}. For help use "genomic --help".')
    #         exit_code = 1


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
