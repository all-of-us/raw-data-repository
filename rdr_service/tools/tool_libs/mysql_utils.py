#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys

from rdr_service.services.system_utils import setup_logging, setup_i18n, make_api_request
from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.services.gcp_utils import gcp_make_auth_header, gcp_get_mysql_instance_service_account
from rdr_service.services.gcp_config import GCP_INSTANCES

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "mysql"
tool_desc = "mysql database utilities"


class DBUtilClass(object):
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
        print('Not implemented')
        return 0


class ExportTablesClass(object):
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
        # https://cloud.google.com/sql/docs/mysql/import-export/exporting
        # https://cloud.google.com/sql/docs/mysql/admin-api/v1beta4/instances/export
        :return: Exit code value
        """
        if not self.args.bucket_uri.startswith('gs://'):
            _logger.error('bucket uri must be a valid google bucket url and start with "gs://".')
            return 1

        data = {
            'exportContext': {
                'fileType': self.args.format.upper(),
                'uri': self.args.bucket_uri,
                'databases': [self.args.database],
                'csvExportOptions': {
                    'tables': self.args.tables,
                    'schemaOnly': 0,
                    'selectQuery': '*'
                }
            }
        }
        headers = gcp_make_auth_header()
        # Exports can not be done against read-only replicas.
        instance = GCP_INSTANCES[self.gcp_env.project].split(':')[-1:][0]
        path = f'/sql/v1beta4/projects/{self.gcp_env.project}/instances/{instance}/export'

        code, resp = make_api_request('www.googleapis.com', api_path=path, json_data=data, headers=headers,
                                      req_type='POST')
        if code != 200:
            if code == 403:
                if 'The service account does not have the required permissions for the bucket' in resp:
                    sa = gcp_get_mysql_instance_service_account(instance)
                    _logger.error("\nThe MySQL service instance service account does not have permission to write")
                    _logger.error(f"to '{self.args.bucket_uri}'. You need to grant bucket write")
                    _logger.error(f"permission to '{sa}'\nand then try exporting again.\n")
                    return 1

            _logger.error(resp)
        else:
            _logger.info(resp)

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
    parser.add_argument("--project", help="gcp project name", required=True)  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa

    subparser = parser.add_subparsers(help='utilities')

    export_parser = subparser.add_parser("export")
    export_parser.add_argument('--database', help="database to export from", required=True)  # noqa
    export_parser.add_argument('--bucket-uri', help="bucket path to export tables to", required=True)  # noqa
    export_parser.add_argument('--format', help="export tables to file type, default is csv", choices=['csv', 'sql'],
                               default='csv')  # noqa

    export_parser.add_argument('tables', metavar="tables", help="list of tables to export",
                               nargs=argparse.REMAINDER)


    # parser.add_argument("--create-cloud-instance", help="create new gcp mysql database instance and backup",
    #                         default=False, action="store_true")  # noqa
    # parser.add_argument("--change-passwords", help="change mysql user passwords",
    #                         default=False, action="store_true")  # noqa

    args = parser.parse_args()


    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:

        if hasattr(args, 'database') and hasattr(args, 'bucket_uri'):
            process = ExportTablesClass(args, gcp_env)
            exit_code = process.run()
        else:
            _logger.info('Please select a tool option to run. For help use "mysql --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
