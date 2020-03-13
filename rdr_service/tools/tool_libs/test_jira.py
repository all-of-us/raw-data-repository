#! /bin/env python
# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import argparse
import logging
import os
import sys
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.services.jira_utils import JiraTicketHandler

_LOGGER = logging.getLogger("rdr_logger")
TOOL_CMD = "test-jira"
TOOL_DESC = "tests the jira api on the DRC API Test project"


class JiraTestClass:

    deploy_version = None

    def __init__(self, args, gcp_env):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.jira_ready = False
        self._jira_handler = JiraTicketHandler()
        self.test_tags = {'drc_analytics': ['michael.mead@vumc.org'],
                               'change_manager': ['michael.mead@vumc.org', 'michael.mead@vumc.org']}

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        clr = self.gcp_env.terminal_colors
        if 'JIRA_API_USER_NAME' in os.environ and 'JIRA_API_USER_PASSWORD' in os.environ:
            self.jira_ready = True

        if self.jira_ready:
            _LOGGER.info('  JIRA Credentials      : %s', clr.fmt("Set"))
            jira = JiraTicketHandler()
            jira._connect_to_jira()  # pylint: disable=W0212
        else:
            if self.gcp_env.project in ('all-of-us-rdr-prod', 'all-of-us-rdr-stable'):
                _LOGGER.info('  JIRA Credentials      : %s', clr.fmt("*** Not Set ***", clr.fg_bright_red))

        _LOGGER.info('')
        _LOGGER.info('=' * 90)

        # start calling commands...
        # DAT board id = DRC API Test project
        summary = 'test summary'

        tickets = jira.find_ticket_from_summary(summary, board_id='DAT')
        issue = None
        user = jira.search_user('michael.mead@vumc.org')
        if user:
            comment = f"Ready for QA: [~accountid:{user.accountId}]."
        else:
            msg = f'Error: JIRA user (michael.mead@vumc.org) not found.'
            _LOGGER.error(msg)
            comment = msg

        if tickets:
            print(f'tickets: {tickets}')
            issue = tickets[0]
        else:
            create = jira.create_ticket(summary, 'test description', issue_type='Task', board_id='DAT')
            _LOGGER.info('Created Ticket %s', create)

        if issue:
            jira.add_ticket_comment(issue, comment)
            _LOGGER.info("Added comment to %s", issue)
            self.tag_people(jira, issue)

        return 0

    def tag_people(self, jira, issue):
        tag_unames = {}
        for position, names in self.test_tags.items():
            tmp_list = []
            for i in names:
                tmp_list.append('[~' + self._jira_handler.search_user(i) + ']')

            tag_unames[position] = tmp_list

        comment = "Notificiations for the following positions: "
        for k, v in tag_unames.items():
            comment += k + ': \n'
            for i in v:
                comment += i + '\n'
        jira.add_ticket_comment(issue, comment)
        #jira.add_ticket_comment(issue, str(tag_unames).strip('{}[]').replace("''", ''))




def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _LOGGER, TOOL_CMD, "--debug" in sys.argv, "{0}.log".format(TOOL_CMD) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    parser = argparse.ArgumentParser(prog=TOOL_CMD, description=TOOL_DESC)
    parser.add_argument("--debug", help="Enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    args = parser.parse_args()
    with GCPProcessContext(TOOL_CMD, args.project, args.account, args.service_account) as gcp_env:
        process = JiraTestClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
