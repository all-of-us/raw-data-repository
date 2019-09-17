#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys

from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "rabbitmq"
tool_desc = "RabbitMQ utilities"


class RabbitMQClass(object):
    def __init__(self, args, gcp_env):
        """
    :param args: command line arguments.
    :param gcp_env: gcp environment information, see: gcp_initialize().
    """
        self.args = args
        self.gcp_env = gcp_env

    def run(self):
        """
    Main program process
    :return: Exit code value
    """
        print("not implemented")
        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="Enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--create-cloud-instance", help="create new gce rabbitmq vm instance",
                            default=False, action="store_true")  # noqa
    parser.add_argument("--create-user", help="create new user with password",
                            default=False, action="store_true")  # noqa
    parser.add_argument("--delete-user", help="delete user", default=False, action="store_true")  # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = RabbitMQClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
