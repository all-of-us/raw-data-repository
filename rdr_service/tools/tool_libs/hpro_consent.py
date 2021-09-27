#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import csv
import logging
import os
import sys

from rdr_service.services.hpro_consent import HealthProConsentFile
from rdr_service.services.system_utils import setup_logging, setup_i18n

from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.tools.tool_libs.tool_base import ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "hpro-consents"
tool_desc = "Tool for initial and subsequent large backfills of consent files to healthpro-consent bucket"


class HealthProConsentTool(ToolBase):

    def run(self):
        transfer_failures = []
        self.gcp_env.activate_sql_proxy()
        hpro_consents = HealthProConsentFile(
            logger=_logger,
            store_failures=True
        )

        hpro_consents.initialize_consent_transfer()

        if hpro_consents.transfer_failures:
            output_local_csv(
                filename='transfer_delta.csv',
                data=transfer_failures
            )


def output_local_csv(*, filename, data):
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[k for k in data[0]])
        writer.writeheader()
        writer.writerows(data)

    _logger.info(f'Generated failures csv: {os.getcwd()}/{filename}')


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
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = HealthProConsentTool(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
