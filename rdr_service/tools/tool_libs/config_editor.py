#! /bin/env python
#
# Edit cloud app configurations.
#
import argparse
import json
import logging
import os
import sys
import tempfile
from datetime import datetime
from difflib import unified_diff
from subprocess import call

from rdr_service.services.jira_utils import JiraTicketHandler
from rdr_service.services.system_utils import setup_logging, setup_i18n, which
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "edit-config"
tool_desc = "Edit RDR App Configuration"


class ProgramTemplateClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.gcsp = GoogleCloudStorageProvider()



    def edit_config(self, config_root, config):
        """
        Edit configuration using Vim editor.
        :param config_root: The configuration project root.
        :param config: Configuration text to edit.
        :return: Edited configuration text.
        """
        editor_executable_name = os.getenv('RDR_TEXT_EDITOR') or 'vim'
        editor = which(editor_executable_name)
        if not editor:
            raise FileNotFoundError(f'{editor_executable_name} executable not found.')

        with tempfile.NamedTemporaryFile(prefix=f'{self.args.key}.{config_root}.', suffix='.json', delete=False) as h:
            filename = h.name
            h.write(config.encode('utf-8'))
            h.flush()
        # Launch editor for editing config.
        args = [editor, filename]
        call(args)
        # Now read vim-edited file and return
        with open(filename, mode='r+b') as h:
            # Read edited file
            config = h.read().decode('utf-8')
        os.remove(filename)
        return config

    def confirm(self, message):
        """
        Ask if the yser
        :param message: Message to display to user.
        :return: True if Yes otherwise False.
        """
        confirm = input(f'\n{message} (Y/n)? : ')
        if confirm and confirm.lower().strip() != 'y':
            return False
        return True

    def update_jira_ticket(self, config_root):
        """
        Add comment to ticket to get developer approval for changes.
        """
        jira = JiraTicketHandler()
        ticket = jira.get_ticket(self.args.jira_ticket)
        if ticket:
            users = ''
            for email in jira.developer_tags["developers"]:
                user = jira.search_user(email)
                if user:
                    users += f'[~accountid:{user.accountId}]'
            comment = f'App configuration has changed for: *{config_root}*.\n'
            comment += f'For developer approval: {users}'
            jira.add_ticket_comment(ticket, comment)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        config_is_valid = False
        config_root = self.gcp_env.project if not self.args.base_config else 'base-config'
        config, config_file = self.gcp_env.get_latest_config_from_bucket(config_root, self.args.key)

        if not config:
            _logger.error(f'Error: no configuration found in bucket for project {config_root}, aborting')
            return 1

        clr = self.gcp_env.terminal_colors
        _logger.info(clr.fmt('App Configuration:', clr.custom_fg_color(156)))
        _logger.info(clr.fmt(''))
        _logger.info('=' * 90)
        _logger.info('  Target                : {0}'.format(clr.fmt(config_root)))
        _logger.info('  Config Key            : {0}'.format(clr.fmt(self.args.key)))
        _logger.info('  Latest Config File    : {0}'.format(clr.fmt(config_file)))
        _logger.info('=' * 90)

        if not self.confirm('Edit config'):
            return 0

        edited_config = config

        while not config_is_valid:
            edited_config = self.edit_config(config_root, edited_config)
            # Test changes
            if edited_config == config:
                _logger.warning('Warning: configuration unchanged.')
                if not self.confirm('Continue editing config'):
                    _logger.warning('Aborting.')
                    return 0
                continue
            # Test json structure
            try:
                json.loads(edited_config)
                config_is_valid = True
            except json.decoder.JSONDecodeError:
                _logger.error('Warning: json structure contains errors.')
                if not self.confirm('Continue editing config'):
                    _logger.warning('Aborting.')
                    return 0

            _logger.info('-' * 90)
            # Output the diff
            result = unified_diff(str.splitlines(config), str.splitlines(edited_config),
                                  'Current Config', 'Updated Config')
            for item in result:
                if item.startswith(' '):
                    continue
                _logger.warning(item)
            _logger.info('-' * 90)

            if self.confirm('Continue editing config'):
                config_is_valid = False
                continue

        if not self.confirm('Save changes'):
            _logger.warning('Aborting.')
            return 0

        _logger.info('Saving configuration to bucket...')
        # Write the updated config back to the bucket.
        filename = f"/app_engine_configs/{config_root}/{self.args.key}.{datetime.utcnow().isoformat()}.json"
        with self.gcsp.open(filename, mode='wt') as fo:
            fo.write(edited_config.encode('utf-8'))
        if self.args.base_config:
            with open('config/base_config.json', 'w+') as h:
                h.write(edited_config)

        if self.args.jira_ticket:
            _logger.info(f'Updating JIRA ticket {self.args.jira_ticket}...')
            self.update_jira_ticket(config_root)

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
    parser.add_argument('--key', help='configuration key', default='current_config', type=str)  # noqa
    parser.add_argument('--base-config', help='edit base configuration', default=False, action="store_true")  # noqa
    parser.add_argument('--jira-ticket', help='jira ticket id', default=None, type=str)  # noqa
    args = parser.parse_args()

    if args.key != 'current_config':
        # TODO: Should we need to support 'db_config' and 'geocode_key' config keys?
        _logger.error('Error: currently only the "current_config" key is supported.')
        return 0

    if args.jira_ticket:
        if not 'JIRA_API_USER_NAME' in os.environ or not 'JIRA_API_USER_PASSWORD' in os.environ:
            _logger.error('Error: JIRA API credentials are not set, aborting.')
            return 0

    if args.project != 'localhost' and args.base_config is True:
        _logger.error('Error: Conflicting arguments, --project and --base-config are not allowed together.')
        return 0

    if args.project == 'localhost' and args.base_config is False:
        _logger.error('Error: --project or --base-config argument required.')
        return 0

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:

        args.git_project = gcp_env.git_project

        process = ProgramTemplateClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
