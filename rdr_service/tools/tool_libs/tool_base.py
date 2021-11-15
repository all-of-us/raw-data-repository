import argparse
import logging
import sys
from typing import Type

from rdr_service.dao import database_factory
from rdr_service.services.config_client import ConfigClient
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext

logger = logging.getLogger("rdr_logger")


class ToolBase:
    def __init__(self, args, gcp_env=None, tool_name=None):
        self.args = args
        self.tool_cmd = tool_name
        self.gcp_env = gcp_env

    def initialize_process_context(self, tool_cmd=None, project=None, account=None, service_account=None):
        if tool_cmd is None:
            tool_cmd = self.tool_cmd
        if project is None:
            project = self.args.project
        if account is None:
            account = self.args.account
        if service_account is None:
            service_account = self.args.service_account

        return GCPProcessContext(tool_cmd, project, account, service_account)

    def run_process(self):
        """
        Responsible for setting up the GCP environment and running the tool's code
        :return: Tool's exit code value
        """
        with self.initialize_process_context(self.tool_cmd, self.args.project,
                                             self.args.account, self.args.service_account) as gcp_env:
            self.gcp_env = gcp_env
            return self.run()

    def run(self):
        """Checking codacy build"""
        proxy_pid = self.gcp_env.activate_sql_proxy()
        if not proxy_pid:
            logger.error("activating google sql proxy failed.")
            return 1

    def get_server_config(self):
        client = ConfigClient(self.gcp_env)
        return client.get_server_config()

    @staticmethod
    def get_session(database_name='rdr', alembic=False, **kwargs):
        return database_factory.make_server_cursor_database(
            database_name=database_name,
            alembic=alembic,
            **kwargs
        ).session()


def cli_run(tool_cmd, tool_desc, tool_class: Type[ToolBase], parser_hook=None, defaults={}):
    # Set global debug value and setup application logging.
    setup_logging(
        logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account",
                        help="gcp iam service account",
                        default=defaults.get('service_account', None))  # noqa
    if parser_hook:
        parser_hook(parser)

    args = parser.parse_args()
    process = tool_class(args, tool_name=tool_cmd)
    return process.run_process()
