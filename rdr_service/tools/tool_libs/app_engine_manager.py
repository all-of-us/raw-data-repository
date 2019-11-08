#! /bin/env python
#
# Tool to deploy app to Google App Engine.
#
import argparse
import logging
import os
import sys
import yaml
from yaml import Loader as yaml_loader

from rdr_service.services.system_utils import setup_logging, setup_i18n, git_project_root, git_current_branch, \
    git_checkout_branch, is_git_branch_clean
from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.services.gcp_config import GCP_SERVICES, GCP_SERVICE_CONFIG_MAP
from rdr_service.services.gcp_utils import gcp_get_app_versions, gcp_deploy_app, gcp_app_services_split_traffic

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "app-engine"
tool_desc = "manage google app engine services"


class DeployAppClass(object):

    deploy_type = 'prod'
    deploy_sub_type = 'default'
    services = GCP_SERVICES
    jira_ready = False
    deploy_version = None
    deploy_root = None

    _current_git_branch = None

    def __init__(self, args, gcp_env):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

        self.deploy_root = args.git_project
        self._current_git_branch = git_current_branch()

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

    def setup_config_files(self):
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
            if service['type'] == 'service'and key not in self.services:
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

        return True

    def clean_up_config_files(self, config_files):
        """
        Remove the config files.
        :param config_files: list of config files.
        """
        for c in config_files:
            if os.path.exists(c):
                os.remove(c)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        _logger.info('')

        if not is_git_branch_clean():
            _logger.error('Error: there are uncommitted changes in current branch, aborting.\n')
            return 1

        if not self.setup_services():
            return 1

        self.deploy_version = self.args.deploy_as if self.args.deploy_as else self.args.git_branch

        running_services = gcp_get_app_versions(running_only=True)

        _logger.info('Deployment Information:')
        _logger.info('=' * 90)
        _logger.info('  Target Project : {0}'.format(self.gcp_env.project))
        _logger.info('  Branch/Tag To Deploy : {0}'.format(self.args.git_branch))
        _logger.info('  App Source Path : {0}'.format(self.deploy_root))
        _logger.info('  Promote : {0}'.format('Yes' if self.args.promote else 'No'))

        if self.gcp_env.project in ('all-of-us-rdr-prod', 'all-of-us-rdr-stable'):
            if 'JIRA_API_USER_NAME' in os.environ and 'JIRA_API_USER_PASSWORD' in os.environ:
                self.jira_ready = True
                _logger.info('  JIRA Credentials: Set')
            else:
                _logger.warning('  JIRA Credentials: !!! Not Set !!!')

        for service in self.services:
            _logger.info('\n  Service : {0}'.format(service))
            _logger.info('-' * 90)
            if service in running_services:
                cur_services = running_services[service]
                for cur_service in cur_services:
                    _logger.info('    Deployed Version  : {0}, split : {1}, deployed : {2}'.
                                 format(cur_service['version'], cur_service['split'], cur_service['deployed']))

                _logger.info('    Target Version    : {0}'.format(self.deploy_version))

        _logger.info('')
        _logger.info('=' * 90)

        if not self.args.quiet:
            confirm = input('\nStart deployment (Y/n)? : ')
            if confirm and confirm.lower().strip() != 'y':
                _logger.warning('Aborting deployment.')
                return 1

        # Attempt to switch to the git branch we need to deploy.
        _logger.info('Switching to git branch/tag: {0}...'.format(self.args.git_branch))
        if not git_checkout_branch(self.args.git_branch):
            return 1

        _logger.info('Preparing configuration files...')
        config_files = self.setup_config_files()
        _logger.info('Deploying app...')
        result = gcp_deploy_app(self.gcp_env.project, config_files, self.deploy_version, self.args.promote)
        _logger.info('Cleaning up...')
        self.clean_up_config_files(config_files)

        _logger.info('Switching back to git branch/tag: {0}...'.format(self._current_git_branch))
        git_checkout_branch(self._current_git_branch)

        return 0 if result else 1


class ListServicesClass(object):
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

    subparser = parser.add_subparsers(help='app engine services')

    deploy_parser = subparser.add_parser("deploy")
    deploy_parser.add_argument("--quiet", help="do not ask for user input.", default=False)  # noqa
    deploy_parser.add_argument("--git-branch", help="git branch/tag to deploy.", required=True)  # noqa
    deploy_parser.add_argument("--deploy-as", help="deploy as version", default=None)  #noqa
    deploy_parser.add_argument("--git-project", help="path to git project root directory", default=None)  # noqa
    deploy_parser.add_argument("--services", help="comma delimited list of service names to deploy",
                               default=None)  # noqa
    deploy_parser.add_argument("--promote", help="promote version to serving state.",
                        default=False, action="store_true")  # noqa

    service_list_parser = subparser.add_parser("list")
    service_list_parser.add_argument('--running-only', help="show only services that are actively serving",
                                     default=False, action='store_true')  # noqa

    split_parser = subparser.add_parser("split-traffic")
    split_parser.add_argument("--quiet", help="do not ask for user input.", default=False)  # noqa
    split_parser.add_argument('--service', help='name of service to split traffic on.', required=True)
    split_parser.add_argument('--versions', required=True,
                              help='a list of versions and split ratios, ex: service_a:0.4,service_b:0.6 ')
    split_parser.add_argument('--split-by', help='split traffic by', choices=['random', 'ip', 'cookie'],
                              default='random')

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:

        if hasattr(args, 'git_branch'):
            if not args.project or args.project == 'localhost':
                _logger.error('unable to deploy without a project.')
                exit(1)

            if not args.git_project:
                envron_path = os.environ.get('RDR_PROJECT', None)
                git_root_path = git_project_root()
                if envron_path:
                    args.git_project = envron_path
                elif git_root_path:
                    args.git_project = git_root_path
                else:
                    _logger.error("No project root found, set '--git-project' arg or set RDR_PROJECT environment var.")
                    exit(1)

            process = DeployAppClass(args, gcp_env)
            exit_code = process.run()

        elif hasattr(args, 'running_only'):
            process = ListServicesClass(args, gcp_env)
            exit_code = process.run()

        elif hasattr(args, 'split_by'):
            process = SplitTrafficClass(args, gcp_env)
            exit_code = process.run()

        else:
            _logger.info('Please select a service option to run. For help use "app-engine --help".')
            exit_code = 1
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
