# Temporary tool for manually generating consent validation metrics (until it can be automated by dashboard team)
# Also creates CSV files for PTSC with information about consent errors.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys

from datetime import datetime

from rdr_service import config
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.services.gcp_config import RdrEnvironment
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.resource.generators.consent_metrics import ConsentErrorReportGenerator

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "consent-error-report"
tool_desc = "Automatic creation of consent validation error reports.  Currently only for PTSC participants"

class ConsentErrorReportTool(object):
    """"
        The ConsentReport class will contain attributes and methods common to both the daily consent validation report
        and the weekly consent validation status report.
    """
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.db_conn = None

    def _connect_to_rdr_replica(self):
        """ Establish a connection to the replica RDR database for reading consent validation data """
        self.gcp_env.activate_sql_proxy(replica=True)
        self.db_conn = self.gcp_env.make_mysqldb_connection()

    def execute(self):
        """
        Execute the ConsentErrorReport builder
        """

        # Only generate error reports/tickets if project is prod,  or we're directing output to a file instead of
        # generating emails (allows for testing in lower environments if needed)
        if not (self.args.to_file or self.gcp_env.project == 'all-of-us-rdr-prod'):
            return

        self._connect_to_rdr_replica()

        # TODO: Temporary check/exception condition until use of SendGrid to generate emails for PTSC approved
        # if self.gcp_env.project == 'all-of-us-rdr-prod' and not self.args.to_file:
        #   _logger.error('Production error reports currently must be directed to a file via --to-file')
        #   return 1

        if not self.args.to_file:
            project_config = self.gcp_env.get_app_config()
            if not project_config[config.SENDGRID_KEY]:
                raise (config.MissingConfigException, 'No API key configured for sendgrid')
            # This enables use of SendGrid config data if running a tool from a dev server vs. app instance
            config.override_setting(config.SENDGRID_KEY, project_config[config.SENDGRID_KEY])

        report = ConsentErrorReportGenerator()
        report.create_error_reports(errors_created_since=self.args.errors_since,
                                    participant_origin=self.args.origin,
                                    to_file=self.args.to_file)
        return 0

def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.  NOTE:  This tool defaults to PRODUCTION project/service account
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default=RdrEnvironment.PROD.value)  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account",
                        default=f'configurator@{RdrEnvironment.PROD.value}.iam.gserviceaccount.com') #noqa
    parser.add_argument("--errors-since", type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help="Date (YYYY-MM-DD string) filter to apply when finding validation errors for report")
    parser.add_argument("--to-file", help="Output error report content to this file",
                        default=False, type=str, dest="to_file")
    parser.add_argument("--origin", default='vibrent', help="participant_origin value to filter on")
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ConsentErrorReportTool(args, gcp_env)
        exit_code = process.execute()
        return exit_code
