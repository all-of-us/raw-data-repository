# Tool for manually generating consent error reports.  Text reports intended to be sent in an auto-generated email
# to trigger Jira ticket creation for PTSC Service Desk.  Can also redirect output to a file in lieu of emails.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import os
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
    def __init__(self, args, gcp_env: GCPEnvConfigObject, id_list=None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.db_conn = None
        self.id_list = id_list
        self.recipients, self.cc_recipients = self._get_email_addresses(self.args.to, self.args.cc)

    @staticmethod
    def _get_email_addresses(to_list: str or None, cc_list: str or None):
        """
        Transform comma-separated strings passed as tool params into lists
        :param to_list:  A comma-separated list of emails from the args.to param
        :param cc_list: A comma-separated list of emails from the args.cc_list param
        :returns:  recipients, cc_list list objects
        """
        recipients, cc_recipients = None, None
        if to_list:
            recipients = list(to_list.split(','))
        if cc_list:
            cc_recipients = list(cc_list.split(','))
        return recipients, cc_recipients

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

        if not self.args.to_file:
            project_config = self.gcp_env.get_app_config()
            if not project_config[config.SENDGRID_KEY]:
                raise (config.MissingConfigException, 'No API key configured for sendgrid')
            # This enables use of SendGrid email service when running this tool from a dev server vs. app instance
            config.override_setting(config.SENDGRID_KEY, project_config[config.SENDGRID_KEY])

        errors_since = self.args.errors_since
        report = ConsentErrorReportGenerator()
        # Specific ids will override any date filter
        if self.id_list:
            errors_since = None
        report.create_error_reports(errors_created_since=errors_since,
                                    id_list=self.id_list,
                                    recipients=self.recipients,
                                    cc_list=self.cc_recipients,
                                    participant_origin=self.args.origin,
                                    to_file=self.args.to_file)
        return 0

def get_id_list(fname):
    """
    Shared helper routine for tool classes that allow input from a file of integer ids (participant ids or
    id values from a specific table).
    :param fname:  The filename passed with the --from-file argument
    :return: A list of integers, or None on missing/empty fname
    """
    filename = os.path.expanduser(fname)
    if not os.path.exists(filename):
        _logger.error(f"File '{fname}' not found.")
        return None

    # read ids from file.
    ids = open(os.path.expanduser(fname)).readlines()
    # convert ids from a list of strings to a list of integers.
    ids = [int(i) for i in ids if i.strip()]
    return ids if len(ids) else None

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
    parser.add_argument("--id", type=int, help="Specific consent_file primary key id to create an error report for, " +\
                                               "takes precedence over --errors-since")
    parser.add_argument("--from-file", type=str, help="File with list of consent_file primary_key ids to create " + \
                                                      "error reports for")
    parser.add_argument("--to-file", help="Output error report content to this file",
                        default=False, type=str, dest="to_file")
    parser.add_argument("--origin", default='vibrent', help="participant_origin value to filter on")
    parser.add_argument("--to", default=None,
                        help="Comma-separated list of email addresses for To: list.  Overrides app config setting")
    parser.add_argument("--cc", default=None,
                        help="Comma-separated list of email address for the Cc: list. Overrides app config setting")
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        ids = None
        if hasattr(args, 'from_file') and args.from_file:
            ids = get_id_list(args.from_file)
        elif hasattr(args, 'id') and args.id:
            ids = [args.id, ]

        process = ConsentErrorReportTool(args, gcp_env, ids)
        exit_code = process.execute()
        return exit_code
