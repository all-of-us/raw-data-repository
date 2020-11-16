#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import argparse
import json
import logging
import os
import pwd
import random
import sys
import time
import traceback
from datetime import datetime
from getpass import getpass

from dateutil.parser import ParserError, parse
from google.api_core.exceptions import NotFound

from aou_cloud.services.gcp_cloud_datastore import GoogleCloudDatastoreConfigProvider
from aou_cloud.services.gcp_cloud_storage import GoogleCloudStorageProvider
from aou_cloud.services.gcp_utils import gcp_activate_sql_proxy, gcp_cleanup, gcp_initialize
from aou_cloud.services.system_utils import remove_pidfile, write_pidfile_or_die, git_project_root, \
    TerminalColors, setup_logging


_logger = logging.getLogger("pdr")


class GCPEnvConfigObject(object):
    """ GCP environment configuration object """

    _postgres_conn = None
    _sql_proxy_process = None
    _sql_proxy_port = None

    project = None
    git_project = None
    terminal_colors = TerminalColors()

    def __init__(self, items):
        """
        :param items: dict of config key value pairs
        """
        # https://github.com/googleapis/google-auth-library-python/issues/271
        import warnings
        warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

        for key, val in items.items():
            self.__dict__[key] = val

        # Determine the git project root directory.
        envron_path = os.environ.get('PDR_PROJECT', None)
        git_root_path = git_project_root()
        if envron_path:
            self.git_project = envron_path
        elif git_root_path:
            self.git_project = git_root_path
        else:
            _logger.warning("GCPEnvConfigObject: no git project root found.")

        # Turn on terminal colors.
        clr = self.terminal_colors

        clr.set_default_formatting(clr.bold, clr.custom_fg_color(43))
        clr.set_default_foreground(clr.custom_fg_color(152))
        _logger.info('')

    def cleanup(self):
        """ Clean up or close everything we need to """
        if self._postgres_conn:
            self._postgres_conn.close()
        if self._sql_proxy_process:
            self._sql_proxy_process.terminate()
        # Turn off terminal colors.
        _logger.info(self.terminal_colors.reset)

    def get_app_config(self, config_key='app_config', project=None):
        """
        Get the running application config.
        :return: dict
        """
        if not project:
            project = self.project

        # See if we should use local configs or cloud configs.
        if not project or project == 'localhost':
            paths = [f'django_service/.configs/{config_key}.json',
                     f'pdr-django-app/django_service/.configs/{config_key}.json']
            for path in paths:
                file = os.path.join(self.git_project, path)
                if os.path.exists(file):
                    config = json.loads(open(file, 'r').read())
                    return config
            raise FileNotFoundError(f'Error: could not locate {config_key}.json file.')
        else:
            provider = GoogleCloudDatastoreConfigProvider()
            config = provider.load(config_key, project=project)

        return config

    def get_app_db_config(self, project=None):
        """
        Get the running application database config.
        :return: dict
        """
        return self.get_app_config(config_key='db_config', project=project)

    @staticmethod
    def get_latest_config_from_bucket(bucket, config_root, config_key):
        """
        Return the latest configuration file stored in the 'gcp_configs' bucket.
        :param bucket: The bucket the configs are stored in.
        :param config_root: The configuration project root, either project id or 'base-config'.
        :param config_key: configuration key.
        :return: Configuration text or None
        """
        gcsp = GoogleCloudStorageProvider()
        ts = datetime.min
        filename = None

        try:
            files = list(gcsp.list(bucket, prefix=config_root))
        except NotFound as e:
            _logger.error(e)
            _logger.error('Error: no config files found, aborting.')
            return None, None

        for file in files:
            file_atom = file.name.split('/')[1].replace(f'{config_key}.', '').replace('.json', '')
            try:
                f_ts = parse(file_atom)
                if ts < f_ts:
                    ts = f_ts
                    filename = file.name.split('/')[1]
            except ParserError:
                if len(file_atom) < 30:
                    _logger.warning(f'Warning: skipping invalid config file: {file_atom}.')

        if ts == datetime.min:
            return None, None

        with gcsp.open(f'/{bucket}/{config_root}/{filename}', mode='rt') as h:
            config = h.read().decode('utf-8')

        return config, filename

    # TODO: Convert these RDR based functions for usage with PDR.
    # def get_gcp_configurator_account(self, project: str = None) -> (str, None):
    #     """
    #     Return the GCP app engine configurator account for the given project id.
    #     :param project: GCP project id
    #     :return: service account or None
    #     """
    #     if not project:
    #         project = self.project
    #     # pylint: disable=unused-variable
    #     config_str, filename = self.get_latest_config_from_bucket('pdr_app_engine_configs', project, 'app_config')
    #
    #     if not config_str:
    #         _logger.error(f'Failed to get local config for "{project}".')
    #         return None
    #     config = json.loads(config_str)
    #
    #     users = config['user_info']
    #     for user, data in users.items():
    #         try:
    #             if data['clientId'] == 'configurator':
    #                 return user
    #         except KeyError:
    #             pass
    #
    #     return None
    #
    # def activate_sql_proxy(self, project=None, user=None, password=None, instance=None, port=5432):
    #     """
    #     Activate a google sql proxy instance service and set DB_CONNECTION_STRING environment var.
    #     :param project: GCP project id string.
    #     :param user: database user
    #     :param password: database password
    #     :param instance: may be provided from tools
    #     :param port: may be provided from tools
    #     :return: pid, port
    #     """
    #     if self._sql_proxy_process:
    #         self._sql_proxy_process.terminate()
    #         self._sql_proxy_process = None
    #
    #     if not project:
    #         project = self.project
    #
    #     db_config = self.get_app_db_config(project=project)
    #     databases = db_config['databases']
    #
    #     # If localhost project, just point to the local instance of postgresql.
    #     if (project and project == 'localhost') or (self.project and self.project == 'localhost'):
    #         # If no user, set user to the currently logged in OS user.
    #         if not user:
    #             user = pwd.getpwuid(os.getuid())[0]
    #
    #         for k, v in databases.items():
    #             databases[k]['USER'] = user
    #             databases[k]['PASSWORD'] = password if password else ''
    #             databases[k]['PORT'] = str(port)
    #             databases[k]['HOST'] = '127.0.0.1'
    #         os.environ['PDR_DJANGO_DATABASES'] = json.dumps(databases)
    #         return 1, port
    #
    #     _logger.debug("Starting google sql proxy...")
    #     # Choose a random port if port is set to standard port.
    #     port = random.randint(10000, 65535) if port == 5432 else port
    #
    #     instance = instance if instance else gcp_format_sql_instance(
    #         project if project else self.project, port=port)
    #
    #     # Look up configurator user.
    #     if not user:
    #         for k, v in db_config['database-users'].items():
    #             if k == 'configurator':
    #                 user = db_config['database-users'][k]['USER']
    #                 if not password:
    #                     password = db_config['database-users'][k]['PASSWORD']
    #
    #     for k, v in databases.items():
    #         databases[k]['USER'] = user
    #         databases[k]['PASSWORD'] = password if password else ''
    #         databases[k]['PORT'] = str(port)
    #         databases[k]['HOST'] = '127.0.0.1'
    #     os.environ['PDR_DJANGO_DATABASES'] = json.dumps(databases)
    #
    #     self._sql_proxy_process = gcp_activate_sql_proxy(instance)
    #     if self._sql_proxy_process:
    #         time.sleep(6)  # allow time for sql connection to be made.
    #         self._sql_proxy_port = port
    #         return self._sql_proxy_process.pid, port
    #
    #     _logger.error('Error: failed to activate cloud sql proxy.')
    #
    #     return 0, 0
    #
    # def connect_database(self, user=None, password=None, database=None, port=None, host='127.0.0.1'):
    #     """
    #     Connect to a PostgreSQL database instance.
    #     """
    #     if self._postgres_conn:
    #         self._postgres_conn.close()
    #         self._postgres_conn = None
    #
    #     clr = self.terminal_colors
    #     if not port:
    #         port = self._sql_proxy_port
    #     _logger.warning(f' Port: {port}')
    #     if not user:
    #         user = 'postgres'
    #     if not password:
    #         # try reading from local file (~/.pdr/postgres.txt)
    #         passfile = os.path.expanduser('~/.pdr/postgres.txt')
    #         if user == 'postgres' and os.path.exists(passfile):
    #             lines = open(passfile).readlines()
    #             for line in lines:
    #                 instance, _password = line.strip().split(':')
    #                 if instance == self.project:
    #                     password = _password
    #                     break
    #
    #         if not password and self.project != 'localhost':
    #             # Prompt for the 'postgres' user password, may be empty for localhost.
    #             password = getpass(prompt=clr.fmt(f'Enter {user} password: ', clr.fg_bright_blue),
    #                                        stream=None)
    #             if not password and self.project != 'localhost':
    #                 _logger.error(f'Error: {user} password may not be empty for cloud instances.')
    #                 return 1
    #
    #     import psycopg2
    #     self._postgres_conn = psycopg2.connect(
    #         user=user,
    #         password=password,
    #         database=database,
    #         port=port,
    #         host=host
    #     )
    #     self._postgres_conn.autocommit = True
    #
    #     cursor = self._postgres_conn.cursor()
    #     cursor.execute('SELECT version();')
    #     record = cursor.fetchone()
    #     cursor.close()
    #     _logger.info(f'Connected to {record}')
    #
    #     return self._postgres_conn
    #
    # def get_database_list(self):
    #     """ Return the list of database names for the current connection """
    #     if not self._postgres_conn:
    #         raise ConnectionError('Not connected to database.')
    #     sql = "SELECT datname FROM pg_database WHERE datistemplate = false and datname != 'cloudsqladmin' and datname != 'postgres';"
    #
    #     cursor = self._postgres_conn.cursor()
    #     cursor.execute(sql)
    #
    #     databases = [t[0] for t in cursor.fetchall()]
    #     cursor.close()
    #     return databases
    #
    # def get_database_users(self):
    #     """
    #     Return all the database users, always excludes 'postgres' user and any system users.
    #     """
    #     sql = "select usename from pg_user where usename not like 'cloudsql%' and usename not like 'postgres';"
    #     cursor = self._postgres_conn.cursor()
    #     cursor.execute(sql)
    #
    #     users = [t[0] for t in cursor.fetchall()]
    #     cursor.close()
    #     return users
    #
    # def get_database_roles(self):
    #     """
    #     Return all database roles, always excludes 'postgres' role and any system roles.
    #     """
    #     sql = "select rolname from pg_roles where rolname not like 'cloudsql%' and rolname not like 'pg_%' and rolname not like 'postgres';"
    #
    #     cursor = self._postgres_conn.cursor()
    #     cursor.execute(sql)
    #
    #     roles = [t[0] for t in cursor.fetchall()]
    #     cursor.close()
    #     return roles


class GCPProcessContext(object):
    """
    A processing context manager for GCP operations
    """
    _tool_cmd = None
    _command = None
    _project = 'localhost'  # default to localhost.
    _account = None
    _service_account = None
    _env = None

    _env_config_obj = None

    def __init__(self, command, args):
        """
        Initialize GCP Context Manager
        :param command: command name
        :param args: parsed argparser commandline arguments object.
        """
        if not command:
            _logger.error("command not set, aborting.")
            exit(1)

        self._command = command
        self._project = args.project
        self._account = args.account
        self._service_account = args.service_account

        write_pidfile_or_die(command)

        self._env = gcp_initialize(self._project, self._account, self._service_account)
        if not self._env:
            remove_pidfile(command)
            exit(1)

    def __enter__(self):
        """ Return object with properties set to config values """
        self._env_config_obj = GCPEnvConfigObject(self._env)
        return self._env_config_obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Clean up or close everything we need to """
        self._env_config_obj.cleanup()
        gcp_cleanup(self._account)
        remove_pidfile(self._command)

        if exc_type is not None:
            print((traceback.format_exc()))
            _logger.error("program encountered an unexpected error, quitting.")
            exit(1)

    @staticmethod
    def setup_logging(tool_cmd):
        setup_logging(
            _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None)

    @staticmethod
    def get_argparser(tool_cmd, tool_desc):
        """
        :param tool_cmd: Tool command line id.
        :param tool_desc: Tool description.
        """
        # Setup program arguments.
        parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
        parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
        parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
        parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
        parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
        parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
        return parser