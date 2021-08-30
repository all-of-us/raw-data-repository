#! /bin/env python
#
# Tool to deploy app to Google App Engine.
#
import argparse
import datetime
import json
import logging
import os
import re
import sys
import difflib

import yaml
from yaml import Loader as yaml_loader

from rdr_service.dao import database_factory
from rdr_service.services.data_dictionary_updater import DataDictionaryUpdater, dictionary_tab_id,\
    internal_tables_tab_id
from rdr_service.services.system_utils import setup_logging, setup_i18n, git_current_branch, \
    git_checkout_branch, is_git_branch_clean, make_api_request
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.services.config_client import ConfigClient
from rdr_service.services.gcp_config import GCP_SERVICES, GCP_SERVICE_CONFIG_MAP, RdrEnvironment
from rdr_service.services.gcp_utils import gcp_get_app_versions, gcp_deploy_app, gcp_app_services_split_traffic, \
    gcp_application_default_creds_exist, gcp_restart_instances, gcp_delete_versions
from rdr_service.tools.tool_libs.alembic import AlembicManagerClass
from rdr_service.tools.tool_libs.tool_base import ToolBase
from rdr_service.services.jira_utils import JiraTicketHandler
from rdr_service.services.documentation_utils import ReadTheDocsHandler
from rdr_service.config import DATA_DICTIONARY_DOCUMENT_ID, READTHEDOCS_CREDS


_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "app-engine"
tool_desc = "manage google app engine services"


class DeployAppClass(ToolBase):

    deploy_type = 'prod'
    deploy_sub_type = 'default'
    services = GCP_SERVICES
    jira_ready = False
    deploy_version = None
    deploy_root = None
    _current_git_branch = None
    _jira_handler = None

    def __init__(self, args, gcp_env=None, tool_name=None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        super(DeployAppClass, self).__init__(args, gcp_env, tool_name)

        self.deploy_root = args.git_project
        self._current_git_branch = git_current_branch()
        self.jira_board = 'PD'
        self.docs_version = 'stable'  # Use as default version slug for readthedocs

        self.environment = RdrEnvironment(self.args.project)

    def write_config_file(self, key: str, config: list, filename: str = None):
        """
        Combine config files and write them out.
        :param key: lookup key
        :param config: configuration dict
        :param filename: override the default config file name.
        :return: path to config file.
        """
        if not filename:
            config_file = os.path.join(self.args.git_project, '{0}.yaml'.format(key))
        else:
            config_file = os.path.join(self.args.git_project, filename)

        config_data = ''

        for file in config:
            tmp_config = os.path.join(self.args.git_project, file)
            if os.path.exists(tmp_config):
                config_data += open(tmp_config, 'r').read()
                config_data += '\n'
            else:
                _logger.error('Error: config file not found {0} for {1}, skipping file.'.format(file, key))

        # extra clean up for cron config.
        if key == 'cron':
            lines = config_data.split('\n')
            config_data = 'cron:\n'
            for line in lines:
                if not line.strip() or 'cron:' in line or line.strip().startswith('#'):
                    continue
                config_data += '{0}\n'.format(line)

            # Remove any cron jobs that are not fully configured.
            cron_yaml = {'cron': list()}
            tmp_yaml = yaml.load(config_data, Loader=yaml_loader)
            for item in tmp_yaml['cron']:
                if 'schedule' in item and 'url' in item:
                    cron_yaml['cron'].append(item)

            config_data = yaml.dump(cron_yaml)

        open(config_file, 'w').write(config_data)

        return config_file

    def setup_service_config_files(self):
        """
        Using the deploy types, create the service yaml files in the git project root.
        :return: list of configuration files
        """
        config_files = list()
        deploy_configs = GCP_SERVICE_CONFIG_MAP[self.deploy_type]

        for key, service in deploy_configs.items():

            if self.deploy_sub_type in service:
                config = service[self.deploy_sub_type]
            else:
                config = service['default']

            # check to see if we are deploying this service.
            if service['type'] == 'service' and key not in self.services:
                continue

            config_file = self.write_config_file(key, config, service.get('config_file', None))
            config_files.append(config_file)

        return config_files

    def setup_services(self):
        """
        See which services we should deploy, if we have an override.
        :return: True if valid services to deploy, otherwise False.
        """
        if self.args.services:
            self.services = []
            items = self.args.services.split(',')
            for item in items:
                if item.strip() in GCP_SERVICES:
                    self.services.append(item.strip())
                else:
                    _logger.error('Error: invalid service name "{0}", aborting deployment.'.format(item.strip()))
                    return False

            if not self.services:
                _logger.error('Error: no services to deploy, aborting deployment.')
                return False

        # determine deployment type and sub-type.
        if self.gcp_env.project != 'all-of-us-rdr-prod':
            self.deploy_type = 'nonprod'
            if 'careevo' in self.gcp_env.project:
                self.deploy_sub_type = 'careevo'
            elif 'ptsc' in self.gcp_env.project:
                self.deploy_sub_type = 'ptsc'
            elif 'sandbox' in self.gcp_env.project:
                self.deploy_sub_type = 'sandbox'
            elif 'stable' in self.gcp_env.project:
                self.deploy_sub_type = 'stable'
            elif 'drc-api-test' in self.gcp_env.project:  # TODO: replace subtype references with environment
                self.deploy_sub_type = 'test'
        else:
            self.docs_version = 'latest'  # readthedocs version slug for production releases

        return True

    @staticmethod
    def clean_up_config_files(config_files):
        """
        Remove the config files.
        :param config_files: list of config files.
        """
        for c in config_files:
            if os.path.exists(c):
                os.remove(c)

    def create_jira_ticket(self, summary, descr=None, board_id=None):
        """
        Create a Jira ticket.
        """

        code, resp = make_api_request(f'{self.gcp_env.project}.appspot.com', api_path='/')
        if code != 200:
            deployed_version = 'unknown'
        else:
            deployed_version = resp.get('version_id', 'unknown').replace('-', '.')

        if not descr:
            circle_ci_url = '<CircleCI URL>'
            if 'CIRCLE_BUILD_URL' in os.environ:
                circle_ci_url = os.environ.get('CIRCLE_BUILD_URL')
            change_log = self._jira_handler.get_release_notes_since_tag(deployed_version, self.args.git_target)

            today = datetime.datetime.today()
            descr = f"""h1. Release Notes for {self.args.git_target}
            h2.deployed to {self.gcp_env.project}, listing changes since {deployed_version}:
            {change_log}

            h3. Change Management Description
            System: All of Us DRC, Raw Data Repository (RDR)
            Developers: Robert Abram, Yu Wang, Josh Kanuch, Kenny Skaggs, Peggy Bertsch, Darryl Tharpe
            Needed By Date/Event: <target release date>
            Priority: <Low, Medium, High>
            Configuration/Change Manager: Megan Morris

            Anticipated Impact: <None, Low, Medium, High>
            Software Impact: <Software Impact>
            Training Impact: <Training Impact>
            Data Impact: <Data Impact>

            Testing
            Tester: Yu Wang, Robert Abram, Josh Kanuch, Kenny Skaggs, Peggy Bertsch, Darryl Tharpe
            Date Test Was Completed: {today.strftime("%b %-d, %Y")}
            Implementation/Deployment Date: Ongoing

            Security Impact: <None, Low, Medium, High>

            CircleCI Output: {circle_ci_url}
            """

        if not board_id:
            board_id = self.jira_board

        ticket = self._jira_handler.create_ticket(summary, descr, board_id=board_id)
        return ticket

    def add_jira_comment(self, comment):
        """
        Add a comment to a Jira ticket
        :param comment: Comment to add to Jira ticket.
        """
        if not self.jira_ready:
            return

        matches = re.match(r"^(\d-[\d]+-[\d]+)", self.deploy_version)
        if not matches:
            return comment

        # If this description changes, change in 'create_jira_roc_ticket' as well.
        summary = f"Release tracker for {self.args.git_target}"
        tickets = self._jira_handler.find_ticket_from_summary(summary, board_id=self.jira_board)

        ticket = None
        if tickets:
            ticket = tickets[0]
        else:
            # Determine if this is a CircleCI deploy.
            if self.gcp_env.project == 'all-of-us-rdr-staging':
                ticket = self.create_jira_ticket(summary)
                if not ticket:
                    _logger.error('Failed to create JIRA ticket')
                else:
                    _logger.info(f'Created JIRA ticket {ticket.key} for tracking release.')

        if ticket:
            self._jira_handler.add_ticket_comment(ticket, comment)

        return comment

    @staticmethod
    def find_prod_release_date(run_date: datetime):
        num_days_since_end_of_sprint = run_date.weekday() - 3  # results in negative value with run_date before Thursday
        if num_days_since_end_of_sprint < -2:
            # run_date is likely Monday after the end of the sprint,
            # should still target releasing the Thursday following the end of the sprint
            num_days_since_end_of_sprint += 7  # Back to a positive number in order to target Thursday of the same week
        num_days_to_release_thursday = 7 - num_days_since_end_of_sprint
        return (run_date + datetime.timedelta(num_days_to_release_thursday)).strftime('%b %d, %Y')

    def create_jira_roc_ticket(self):
        """
        Create a reminder JIRA ROC ticket
        """
        matches = re.match(r"^(\d-[\d]+-[\d]+)", self.deploy_version)
        if not matches:
            _logger.warning('Version for deployment is not standard.')
            return

        # Get version and make sure this is a primary sprint release.
        version = matches.group().replace('-', '.')
        if version[-2:] != '.1':
            _logger.warning(f'Hotfix release {version}, skipping adding ROC ticket.')
            return

        today = datetime.date.today()
        summary = f'Deploy RDR v{version} to production on {self.find_prod_release_date(today)}.'

        ticket = self.create_jira_ticket(summary, summary, 'ROC')

        # Add ticket to current sprint
        board = self._jira_handler.get_board_by_id('ROC')
        sprint = self._jira_handler.get_active_sprint(board)
        ticket = self._jira_handler.add_ticket_to_sprint(ticket, sprint)

        # Attempt to change state to In Progress.
        ticket = self._jira_handler.set_ticket_transition(
            ticket,
            self._jira_handler.get_ticket_transition_by_name(
                ticket,
                'Created'
            )
        )

        # Attempt to link the PD release tracker ticket.
        pd_summary = f"Release tracker for {self.args.git_target}"
        tickets = self._jira_handler.find_ticket_from_summary(pd_summary, board_id=self.jira_board)
        if tickets:
            self._jira_handler.link_tickets(tickets[0], ticket, 'Relates')

    def trigger_doc_build(self, config):
        """
        Trigger a documentation build in readthedocs.org
        """
        api_token = None
        rtd_creds = config.get_config_item(READTHEDOCS_CREDS)
        if rtd_creds:
            api_token = rtd_creds['readthedocs_rdr_api_token']
        docs = ReadTheDocsHandler(api_token)

        try:
            if self.deploy_type == 'prod':
                docs.update_project_to_release(self.args.git_target)
                _logger.info(f'ReadTheDocs latest version default branch/tag updated to {self.args.git_target}')

            build_id = docs.build_the_docs(self.docs_version)
            _logger.info(f'Started documentation build for version {self.docs_version} (build ID: {build_id})')
        except (ValueError, RuntimeError) as e:
            _logger.error(f'Failed to trigger readthedocs documentation build for version {self.docs_version}.  {e}')

    def update_data_dictionary(self, server_config, rdr_version):
        configurator_account = f'configurator@{RdrEnvironment.PROD.value}.iam.gserviceaccount.com'
        with self.initialize_process_context(service_account=configurator_account) as gcp_env:
            updater = DataDictionaryUpdater(
                gcp_env.service_key_id,
                server_config[DATA_DICTIONARY_DOCUMENT_ID],
                rdr_version
            )
            updater.download_dictionary_values()

        with self.initialize_process_context() as gcp_env:
            self.gcp_env = gcp_env
            self.gcp_env.activate_sql_proxy()
            with database_factory.make_server_cursor_database(alembic=True).session() as session:
                updater.session = session
                changelog = updater.find_data_dictionary_diff()
                if any(changelog.values()):
                    for tab_id, tab_changelog in changelog.items():
                        if tab_changelog:
                            if tab_id in [dictionary_tab_id, internal_tables_tab_id]:
                                # The schema tabs are the only ones that list out detailed changes
                                _logger.info(f'The following changes were found on the "{tab_id}" tab')
                                for (table_name, column_name), changes in tab_changelog.items():
                                    if isinstance(changes, str):  # Adding or removing a column will give a string
                                        _logger.info(f'{changes} {table_name}.{column_name}')
                                    else:
                                        _logger.info('')
                                        _logger.info(f'changes for {table_name}.{column_name}:')
                                        for change_description in changes:
                                            _logger.info(change_description)
                                        _logger.info('')
                            else:
                                _logger.info(f'The "{tab_id}" tab has been updated')

        if any(changelog.values()):
            with self.initialize_process_context(service_account=configurator_account) as gcp_env:
                update_message = input('What is a summary of the above changes?: ')
                _logger.info('uploading data-dictionary updates')
                updater.gcp_service_key_id = gcp_env.service_key_id
                updater.upload_changes(update_message, self.gcp_env.account)
        else:
            _logger.info('No data-dictionary changes needed')

    def deploy_app(self):
        """
        Deploy the app
        """

        if not self.jira_ready and self.environment in (RdrEnvironment.PROD, RdrEnvironment.STABLE):
            _logger.error('Jira credentials not set, aborting.')
            return 1

        # Disable any other user prompts.
        self.args.quiet = True

        # Change current git branch/tag to git target.
        if not git_checkout_branch(self.args.git_target):
            _logger.error(f'Unable to switch to git branch/tag {self.args.git_target}, aborting.')
            return 1
        _logger.info(f'Switched to {git_current_branch()} branch/tag.')

        # Run database migration
        _logger.info('Applying database migrations...')
        alembic = AlembicManagerClass(self.args, self.gcp_env, ['upgrade', 'heads'])
        if alembic.run() != 0:
            _logger.warning('Deploy process stopped.')
            return 1
        else:
            self.add_jira_comment(f'Migration results:\n{alembic.output}')

        _logger.info('Preparing configuration files...')
        config_files = self.setup_service_config_files()

        # Install app config
        _logger.info(self.add_jira_comment(f"Updating config for '{self.gcp_env.project}'"))
        app_config = AppConfigClass(self.args, self.gcp_env, restart=False)
        app_config.update_app_config(store_config=True)
        _logger.info(self.add_jira_comment(f"Config for '{self.gcp_env.project}' updated."))

        _logger.info(self.add_jira_comment(f"Deploying app to '{self.gcp_env.project}'."))
        result = gcp_deploy_app(self.gcp_env.project, config_files, self.deploy_version, not self.args.no_promote)

        _logger.info(self.add_jira_comment(f"App deployed to '{self.gcp_env.project}'."))
        if self.environment == RdrEnvironment.STABLE:
            self.create_jira_roc_ticket()

        # Automatic doc build limited to stable or prod deploy (unless overridden)
        if self.args.no_docs:
            _logger.debug('Skipping documentation build...')
        elif self.deploy_type == 'prod' or self.deploy_sub_type == 'stable':
            self.trigger_doc_build(app_config)

        _logger.info('Cleaning up...')
        self.clean_up_config_files(config_files)

        # Note: self.services will either be a user-provided list from --services arg, or the GCP_SERVICES list from
        # gcp_config.  Need to iterate through each service to make sure appropriate instances get restarted
        gcp_restart_instances(self.gcp_env.project)

        return 0 if result else 1

    @staticmethod
    def manage_cloud_version_numbers():
        _logger.info('Getting version list for services...')
        version_lists_by_service = gcp_get_app_versions(sort_by=['LAST_DEPLOYED'])  # ordered with oldest versions first

        # GAE allows a max of 210 versions across all services
        max_versions_per_service = 200 // len(version_lists_by_service)  # max version count for each service
        num_to_trim = 10  # Number of versions to delete each time we hit our max_versions count
        for service_name, version_list in version_lists_by_service.items():
            version_count = len(version_list)
            if version_count > max_versions_per_service:
                _logger.warning(f'{version_count} versions found on {service_name.upper()}, deleting the following:')

                versions_to_delete = [version_data['version'] for version_data in version_list[:num_to_trim]]
                _logger.warning(versions_to_delete)

                gcp_delete_versions(service_name, versions_to_delete)

    def tag_people(self):

        # Note: Tagging people is broken because the JIRA python library is making an invalid
        #       API call to Atlassian. See: https://ecosystem.atlassian.net/browse/ACJIRA-1795
        #       Until this is resolved in a newer JIRA python library (current is: 2.0.0), this code is blocked.

        # _logger.info('Updating JIRA ticket...')
        # tag_unames = {}
        # for position, names in self._jira_handler.required_tags.items():
        #     tmp_list = []
        #     for i in names:
        #         user = self._jira_handler.search_user(i)
        #         if user:
        #             tmp_list.append(f'[~accountid:{user.accountId}]')
        #
        #     tag_unames[position] = tmp_list
        #
        # comment = "Notification/approval for the following roles: "
        # for k, v in tag_unames.items():  #pylint: disable=invalid-name
        #     comment += k + ': \n'
        #     for i in v:
        #         comment += i + '\n'
        #
        # self.add_jira_comment(comment)
        pass

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        with self.initialize_process_context() as gcp_env:
            self.gcp_env = gcp_env

            _check_for_git_project(self.args, self.gcp_env)

            clr = self.gcp_env.terminal_colors

            # Installing the app config makes API calls and needs an oauth token to succeed.
            if not self.args.quiet and not gcp_application_default_creds_exist() and not self.args.service_account:
                _logger.error('\n*** Google application default credentials were not found. ***')
                _logger.error("Run 'gcloud auth application-default login' and then try deploying again.\n")
                return 1

            if not is_git_branch_clean():
                _logger.error('*** There are uncommitted changes in current branch, aborting. ***\n')
                return 1

            if not self.setup_services():
                return 1

            self.deploy_version = self.args.deploy_as if self.args.deploy_as else \
                                    self.args.git_target.replace('.', '-')

            running_services = gcp_get_app_versions(running_only=True)
            if not running_services:
                running_services = {}

            _logger.info(clr.fmt('Deployment Information:', clr.custom_fg_color(156)))
            _logger.info(clr.fmt(''))
            _logger.info('=' * 90)
            _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))
            _logger.info('  Branch/Tag To Deploy  : {0}'.format(clr.fmt(self.args.git_target)))
            _logger.info('  App Source Path       : {0}'.format(clr.fmt(self.deploy_root)))
            _logger.info('  Promote               : {0}'.format(clr.fmt('No' if self.args.no_promote else 'Yes')))

            if 'JIRA_API_USER_NAME' in os.environ and 'JIRA_API_USER_PASSWORD' in os.environ:
                self.jira_ready = True
                self._jira_handler = JiraTicketHandler()

            if self.jira_ready:
                _logger.info('  JIRA Credentials      : {0}'.format(clr.fmt('Set')))
            else:
                if self.gcp_env.project in ('all-of-us-rdr-prod', 'all-of-us-rdr-stable'):
                    _logger.info('  JIRA Credentials      : {0}'.format(clr.fmt('*** Not Set ***', clr.fg_bright_red)))

            for service in self.services:
                _logger.info('\n  Service : {0}'.format(service))
                _logger.info('-' * 90)
                if service in running_services:
                    cur_services = running_services[service]
                    for cur_service in cur_services:
                        _logger.info('    Deployed Version    : {0}, split : {1}, deployed : {2}'.
                                     format(clr.fmt(cur_service['version'], clr.bold, clr.fg_bright_blue),
                                            clr.fmt(cur_service['split'], clr.bold, clr.fg_bright_blue),
                                            clr.fmt(cur_service['deployed'], clr.bold, clr.fg_bright_blue)))

                    _logger.info('    Target Version      : {0}'.format(clr.fmt(self.deploy_version)))

            _logger.info('')
            _logger.info('=' * 90)

            if not self.args.quiet:
                confirm = input('\nStart deployment (Y/n)? : ')
                if confirm and confirm.lower().strip() != 'y':
                    _logger.warning('Aborting deployment.')
                    return 1

            result = self.deploy_app()
            self.manage_cloud_version_numbers()

            git_checkout_branch(self._current_git_branch)
            _logger.info('Returned to git branch/tag: %s ...', self._current_git_branch)

            server_config = self.get_server_config()

        if self.environment == RdrEnvironment.PROD:
            _logger.info('Comparing production database schema to data-dictionary...')
            self.update_data_dictionary(server_config, self.deploy_version)

        return result


class ListServicesClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
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
        running_services = gcp_get_app_versions(running_only=self.args.running_only)

        _logger.info('\nListing Services:')
        _logger.info('=' * 90)
        _logger.info('  Project : {0}'.format(self.gcp_env.project))

        version_line = '     {0:35} {1:10} {2:6} {3}'

        for service, versions in running_services.items():
            _logger.info('\n  Service Name : {0}'.format(service))
            _logger.info('-' * 90)
            _logger.info(version_line.format('Version', 'Status', 'Split', 'Deployed'))
            _logger.info('-' * 90)
            versions = sorted(versions, key=lambda k: k['deployed'], reverse=True)

            for info in versions:
                _logger.info(version_line.format(info['version'], info['status'].lower(), str(info['split']),
                                                 info['deployed']))

        _logger.info('')
        _logger.info('=' * 90)

        return 0


class SplitTrafficClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
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
        data = []
        versions = self.args.versions.split(',')

        for item in versions:
            if ':' not in item:
                _logger.error('Error: invalid version name and split ratio: {0}'.format(item))
                return 1
            parts = item.split(':')
            try:
                version = parts[0]
                ratio = float(parts[1])

                if not version or not ratio:
                    _logger.error('Error: invalid version and split ratio {0}.'.format(item))
                if ratio < 0.0 or ratio > 1.0:
                    _logger.error('Error: split ratio out of bound (0.0 -> 1.0).')
                    return 1
                data.append((version, ratio))

            except TypeError:
                _logger.error('Error: invalid split ratio {0}.'.format(parts[1]))
                return 1

        version_line = '     {0:35} {1}'

        _logger.info('\nTraffic Split Information:')
        _logger.info('=' * 60)
        _logger.info('  Project : {0}'.format(self.gcp_env.project))
        _logger.info('  Service : {0}'.format(self.args.service))
        _logger.info('  Split By : {0}'.format(self.args.split_by))
        _logger.info('-' * 60)
        _logger.info(version_line.format('Version', 'Split'))
        _logger.info('-' * 60)

        for _set in data:
            _logger.info(version_line.format(_set[0], _set[1]))

        if not self.args.quiet:
            confirm = input('\nApply changes (Y/n)? : ')
            if confirm and confirm.lower().strip() != 'y':
                _logger.warning('Aborting.')
                return 1

        _logger.info('Applying traffic split...')
        if not gcp_app_services_split_traffic(self.args.service, data, self.args.split_by):
            return 1

        return 0


class AppConfigClass(object):

    _config_dir = None
    _provider = None
    _config_items = {}

    def __init__(self, args, gcp_env: GCPEnvConfigObject, restart=True):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.restart = restart

        self._config_dir = os.path.join(self.args.git_project, 'rdr_service/config')
        if not os.path.exists(self._config_dir):
            raise FileNotFoundError('Unable to locate the app config directory.')

        from rdr_service.config import GoogleCloudDatastoreConfigProvider
        self._provider = GoogleCloudDatastoreConfigProvider()

        if not hasattr(self.args, 'key'):
            setattr(self.args, 'key', 'current_config')

    def get_bucket_app_config(self):
        """
        Combine saved bucket app config files and return it.
        :return: dict
        """
        client = ConfigClient(self.gcp_env)
        return client.get_server_config()

    def get_config_from_file(self):
        """
        Load a config from a local file.
        :return: dict
        """
        if not os.path.exists(self.args.from_file):
            raise FileNotFoundError(f'Unable to find {self.args.from_file}.')

        data = open(self.args.from_file, 'r').read()
        config = json.loads(data)
        return config

    def get_config_item(self, key=None):
        """
        Extract a config item from the stored config data
        """
        if key and key in self._config_items:
            return self._config_items[key]

        return None

    def update_app_config(self, store_config=False):
        """
        Put the local config into the cloud datastore.
        :param store_config:  Keep a copy of the config data in the instance
        """
        if not hasattr(self.args, 'from_file') or not self.args.from_file:
            config = self.get_bucket_app_config()
        else:
            config = self.get_config_from_file()

        if self.gcp_env.project != 'localhost' and self.args.key == 'current_config' and not \
                    config.get('geocode_api_key', None):
            _logger.error("Config must include 'geocode_api_key', unable to write.")
            return 1

        self._provider.store(self.args.key, config, project=self.gcp_env.project)
        _logger.info(f'Successfully updated {self.args.key} configuration.')

        if self.restart:
            _logger.info('Restarting instances...')
            gcp_restart_instances(self.gcp_env.project)

        if store_config:
            self._config_items = config

        return 0

    def get_cloud_app_config(self, display=True):
        """
        Get the cloud datastore config.
        :param display: Print the config to stdout.
        """
        config = self._provider.load(self.args.key, project=self.gcp_env.project)

        if self.args.to_file:
            open(self.args.to_file, 'w').write(json.dumps(config, indent=2, sort_keys=True))

        if display:
            # Mask passwords when writing to stdout.
            for k, v in config.items():  # pylint: disable=unused-variable
                if 'db_connection_string' in k:
                    parts = config[k].split('@')
                    config[k] = parts[0][:parts[0].rfind(':') + 1] + '*********@' + parts[1]
                if 'password' in k:
                    config[k] = '********'

            print(json.dumps(config, indent=2, sort_keys=True))

    def compare_configs(self):
        """
        Compare the remote and local configs for changes.
        """
        if not self.args.from_file:
            local_config = self.get_bucket_app_config()
        else:
            local_config = self.get_config_from_file()

        remote_config = self._provider.load(self.args.key, project=self.gcp_env.project)

        for k, v in local_config.items():  # pylint: disable=unused-variable
            if k not in remote_config:
                remote_config[k] = ''
        for k, v in remote_config.items():
            if k not in local_config:
                local_config[k] = ''

        lc_str = json.dumps(local_config, indent=2, sort_keys=True)
        rc_str = json.dumps(remote_config, indent=2, sort_keys=True)

        if lc_str == rc_str:
            print('\nNo configuration changes detected.\n')
            return

        print('\nShowing configuration changes:\n')

        for line in difflib.context_diff(rc_str.splitlines(keepends=True), lc_str.splitlines(keepends=True),
                                         fromfile='remote_config', tofile='local_config', n=2):
            tmp_v = line
            if 'db_connection_string' in line:
                parts = tmp_v.split('@')
                tmp_v = parts[0][:parts[0].rfind(':') + 1] + '*********@' + parts[1] + "\n"
            elif 'password' in line:
                parts = tmp_v.split(':')
                tmp_v = parts[0] + ': "*********"\n'

            print(tmp_v.replace('\n', ''))

        print('')

    def run(self):

        # this tool makes API calls and needs an oauth token to succeed.
        if not gcp_application_default_creds_exist() and not self.args.service_account:
            _logger.error(
                '\n*** Google application default credentials were not found. ***')
            _logger.error(
                "Run 'gcloud auth application-default login' to create credentials or add '--service-account' arg.\n")
            return 1

        # Argument checks.
        if self.args.key not in {'current_config', 'db_config', 'geocode_key'}:
            _logger.error('\nInvalid --key argument.\n')
            return 1
        if self.args.key != 'current_config' and not self.args.from_file:
            _logger.error('Conflict: Only "current_config" config key may be used without --from-file argument.')
            return 1
        if self.args.compare and self.args.update:
            _logger.error('\nConflict: --compare and --update args may not be used together.\n')
            return 1
        if (self.args.update or self.args.compare) and self.args.to_file:
            _logger.error('\nConflict: --to-file argument may not be used with --update or --compare.\n')
            return 1
        if (not self.args.update and not self.args.compare) and self.args.from_file:
            _logger.error('\nConflict: --from-file argument may not be used without --update or --compare.\n')
            return 1
        if self.args.key == 'db_config':
            if (self.args.update or self.args.compare) and not self.args.from_file:
                _logger.error('\nRequired: The --from-file arg must be set.\n')
                return 1

        if self.args.update:
            return self.update_app_config()
        elif self.args.compare:
            self.compare_configs()
        else:
            self.get_cloud_app_config()

        return 0


class RunGCPUtilCommand:
    _config_dir = None
    _provider = None

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: The argument to 'run_single_util' should be a function inside of gcp_utils
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env


    def run(self):
        function = self.args.run_single_util
        try:
            package = "rdr_service.services.gcp_utils"
            imported = getattr(__import__(package, fromlist=[function]), function)
            # call the function
            return_code = imported(self)
        except ImportError as err:
            _logger.warning(err)

        return return_code


def _check_for_git_project(args, gcp_env):
    # determine the git project root directory.
    if not args.git_project:
        if gcp_env.git_project:
            args.git_project = gcp_env.git_project
        else:
            _logger.error("No project root found, set '--git-project' arg or set RDR_PROJECT environment var.")
            exit(1)


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
    parser.add_argument("--git-project", help="path to git project root directory", default=None)  # noqa

    subparser = parser.add_subparsers(title='actions', dest='action', help='app engine services')

    # Deploy app
    deploy_parser = subparser.add_parser("deploy")
    deploy_parser.add_argument("--quiet", help="do not ask for user input", default=False, action="store_true") # noqa
    deploy_parser.add_argument("--git-target", help="git branch/tag to deploy.", default=git_current_branch())  # noqa
    deploy_parser.add_argument("--deploy-as", help="deploy as version", default=None)  #noqa
    deploy_parser.add_argument("--services", help="comma delimited list of service names to deploy",
                               default=None)  # noqa

    deploy_parser.add_argument("--no-promote", help="do not promote version to serving state.",
                               default=False, action="store_true")  # noqa
    deploy_parser.add_argument("--no-docs", help="Skip triggering a documentation build on readthedocs.org",
                               default=False, action="store_true")  # noqa

    # List app engine services
    service_list_parser = subparser.add_parser("list")
    service_list_parser.add_argument('--running-only', help="show only services that are actively serving",
                                     default=False, action='store_true')  # noqa

    # Manage service traffic.
    split_parser = subparser.add_parser("split-traffic")
    split_parser.add_argument("--quiet", help="do not ask for user input", default=False, action="store_true") # noqa
    split_parser.add_argument('--service', help='name of service to split traffic on.', required=True)
    split_parser.add_argument('--versions', required=True,
                              help='a list of versions and split ratios, ex: service_a:0.4,service_b:0.6 ')
    split_parser.add_argument('--split-by', help='split traffic by', choices=['random', 'ip', 'cookie'],
                              default='random')

    # Manage app datastore configs
    config_parser = subparser.add_parser("config")
    config_parser.add_argument('--key', help='datastore config key.', default='current_config', type=str)  # noqa
    config_parser.add_argument('--compare', help='Compare app config to local config.', default=False,
                               action="store_true")  # noqa
    config_parser.add_argument('--update', help='update cloud app config.', default=False, action="store_true")  # noqa
    config_parser.add_argument('--to-file', help='download config to file', default='', type=str)  # noqa
    config_parser.add_argument('--from-file', help='upload config from file', default='', type=str)  # noqa

    config_parser = subparser.add_parser("test")
    config_parser.add_argument('--run-single-util', help='runs single gcp util command, good for testing',
                               default='', type=str)  # noqa

    args = parser.parse_args()

    if args.action == 'deploy':
        exit_code = DeployAppClass(args, tool_name=tool_cmd).run()
    else:
        with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
            _check_for_git_project(args, gcp_env)

            if args.action == 'list':
                process = ListServicesClass(args, gcp_env)
                exit_code = process.run()

            elif args.action == 'split-traffic':
                process = SplitTrafficClass(args, gcp_env)
                exit_code = process.run()

            elif args.action == 'config':
                process = AppConfigClass(args, gcp_env)
                exit_code = process.run()

            elif args.action == 'test':
                process = RunGCPUtilCommand(args, gcp_env)
                exit_code = process.run()

            else:
                _logger.info('Please select a service option to run. For help use "app-engine --help".')
                exit_code = 1

    return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
