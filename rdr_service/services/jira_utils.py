#
# Methods for interacting with Jira tickets
#
import jira
import os
import re

from rdr_service.services.system_utils import run_external_program

_REPO_BASE_URL = "https://github.com/all-of-us/raw-data-repository"
_JIRA_INSTANCE_URL = "https://precisionmedicineinitiative.atlassian.net/"
_JIRA_BOARD_ID = "PD"


class JiraTicketHandler:
    """
    Search, create and update Jira tickets.
    """
    def __init__(self):
        self._jira_user = os.environ.get('JIRA_API_USER_NAME', None)
        self._jira_password = os.environ.get('JIRA_API_USER_PASSWORD', None)
        self._jira_watchers = os.environ.get('JIRA_WATCHER_NAMES', None)
        self._jira_connection = None
        self._required_tags = {'drc_analytics': ['alpha.parrott@vumc.org'],
                               'qa': ['rohini.chavan@vumc.org', 'ashton.e.rollings@vumc.org'],
                               'change_management_board': ['charissa.r.rotundo@vumc.org', 'neil.bible@vumc.org'],
                               'change_manager': ['asmita.gauchan@vumc.org', 'bhinnata.piya@vumc.org', 'katherine.j.worley@vumc.org']}
        self._connect_to_jira()

    def _connect_to_jira(self):
        """
        Opens a JIRA API connection based on username/pw from env vars.
        """
        if not self._jira_connection:
            if not self._jira_user or not self._jira_password:
                raise ValueError('Jira user name or password not set in environment.')
            self._jira_connection = jira.JIRA(_JIRA_INSTANCE_URL, basic_auth=(self._jira_user, self._jira_password))

    def current_user(self):
        return self._jira_connection.current_user()

    def search_user(self, user: str) -> str:
        """param: user = email
           return: username"""
        un_resource = self._jira_connection.search_users(user)
        return un_resource[0].name

    def find_ticket_from_summary(self, summary, board_id=_JIRA_BOARD_ID):
        """
        Find tickets matching a given summary in a Jira project.
        :param summary: Ticket summary to search for.
        :param board_id: Jira board id, IE: PD.
        :return: list of tickets
        """
        tickets = self._jira_connection.search_issues(
                        f'project = "{board_id}" and summary ~ "{summary}" order by created desc')
        return tickets

    def create_ticket(self, summary, descr, issue_type="Task", board_id=_JIRA_BOARD_ID):
        """
        Create a new ticket in a Jira project.
        :param summary: Ticket summary to search for.
        :param descr: Summary description for ticket.
        :param issue_type: Type of ticket to create.
        :param board_id: Jira board id, IE: PD.
        """
        if not summary:
            raise ValueError('Jira ticket summary may not be empty')
        if not descr:
            raise ValueError('Jira ticket description may not be empty')

        ticket = self._jira_connection.create_issue(
            project=board_id, summary=summary, description=descr, issuetype={"name": issue_type}
        )

        if self._jira_watchers:
            for name in [n.strip() for n in self._jira_watchers.split(",")]:
                if name:
                    try:
                        self._jira_connection.add_watcher(ticket, name)
                    except jira.exceptions.JIRAError:
                        pass

        return ticket

    def add_ticket_comment(self, ticket, comment):
        """
        Add a comment to an existing ticket.
        :param ticket: Jira ticket object
        :param comment: Comment string to add to ticket.
        """
        if not ticket:
            raise ValueError('Invalid Jira ticket object')
        if not comment:
            return
        self._jira_connection.add_comment(ticket, comment)

    def get_release_notes_since_tag(self, git_tag):
        """
        Formats release notes for JIRA from commit messages, from the given tag to HEAD.
        :param git_tag: get messages after given git tag.
        """
        #args = ['git', 'log', f'{git_tag}..', '--pretty=format:"%h||%aN, %ad||%s"']
        # They want pretty, we'll give them pretty !
        args = ['git', 'log', f'{git_tag}..', "--graph --decorate \
                --format=format:'%C(bold blue)%h%C(reset) - %C(bold green)(%ar)%C(reset) \
                %C(white)%s%C(reset) %C(dim white)- %an%C(reset)%C(bold yellow)%d%C(reset)' "]
        # pylint: disable=unused-variable
        code, so, se = run_external_program(args=args)

        if code != 0:
            return None

        message = re.sub(r'([\w]{8})\|\|(.*?-[\d]{4})',
                         r'* [\2|https://github.com/all-of-us/raw-data-repository/commit/\1]', so)
        message = re.sub(r"\(#([0-9]+)\)",
                         r"([#\1|https://github.com/all-of-us/raw-data-repository/pull/\1])", message)
        message = message.replace('||', ' ').replace('"', '')

        return message
