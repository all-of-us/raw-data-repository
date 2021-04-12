# pylint: disable=superfluous-parens
import json
import logging
import os
import time
import traceback
import random
from datetime import datetime
from dateutil.parser import ParserError, parse

from google.api_core.exceptions import NotFound

from rdr_service.config import GoogleCloudDatastoreConfigProvider
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.services.gcp_utils import gcp_activate_sql_proxy, gcp_cleanup, gcp_initialize, gcp_format_sql_instance
from rdr_service.services.system_utils import remove_pidfile, write_pidfile_or_die, git_project_root, TerminalColors

_logger = logging.getLogger("rdr_logger")


class GCPEnvConfigObject(object):
    """ GCP environment configuration object """

    _sql_proxy_process = None
    _sql_proxy_port = None
    _mysql_connection = None

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
        envron_path = os.environ.get('RDR_PROJECT', None)
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
        if self._sql_proxy_process:
            self._sql_proxy_process.terminate()
        # Turn off terminal colors.
        _logger.info(self.terminal_colors.reset)

        # If mysql connection, close it.
        if self._mysql_connection:
            self._mysql_connection.close()

    def get_app_config(self, config_key='current_config', project=None):
        """
        Get the running application config.
        :return: dict
        """
        if not project:
            project = self.project

        # See if we should use local configs or cloud configs.
        if not project or project == 'localhost':
            file = os.path.join(self.git_project, f'rdr_service/.configs/{config_key}.json')
            config = json.loads(open(file, 'r').read())
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
    def get_latest_config_from_bucket(config_root, config_key):
        """
        Return the latest configuration file stored in the 'app_engine_configs' bucket.
        :param config_root: The configuration project root.
        :param config_key: configuration key.
        :return: Configuration text or None
        """
        gcsp = GoogleCloudStorageProvider()
        ts = datetime.min
        filename = None

        try:
            files = list(gcsp.list(f'app_engine_configs', prefix=config_root))
        except NotFound as e:
            _logger.error(e)
            _logger.error('Error: no config files found, aborting.')
            return 1

        for file in files:
            try:
                f_ts = parse(file.name.split('/')[1].replace(f'{config_key}.', '').replace('.json', ''))
                if ts < f_ts:
                    ts = f_ts
                    filename = file.name.split('/')[1]
            except ParserError:
                _logger.warning(f'Warning: skipping invalid config file: {filename}.')

        if ts == datetime.min:
            _logger.error('No configuration file found')
            return None, None

        with gcsp.open(f'/app_engine_configs/{config_root}/{filename}', mode='rt') as h:
            config = h.read().decode('utf-8')

        return config, filename


    def get_gcp_configurator_account(self, project: str = None) -> (str, None):
        """
        Return the GCP app engine configurator account for the given project id.
        :param project: GCP project id
        :return: service account or None
        """
        if not project:
            project = self.project
        # pylint: disable=unused-variable
        config_str, filename = self.get_latest_config_from_bucket(project, 'current_config')

        if not config_str:
            _logger.error(f'Failed to get local config for "{project}".')
            return None
        config = json.loads(config_str)

        users = config['user_info']
        for user, data in users.items():
            try:
                if data['clientId'] == 'configurator':
                    return user
            except KeyError:
                pass

        return None

    def activate_sql_proxy(self, user: str = 'rdr', project: str = None,
                           replica: bool = False, instance: str = None,
                           port: int = None) -> int:
        """
        Activate a google sql proxy instance service and set DB_CONNECTION_STRING environment var.
        :param user: database user, must be one of ['root', 'alembic', 'rdr'].
        :param project: GCP project id.
        :param replica: Use replica db instance or Primary instance.
        :param instance: may be provided from tools
        :param port: may be provided from tools
        :return: pid
        """
        if self._sql_proxy_process:
            self._sql_proxy_process.terminate()
            self._sql_proxy_process = None

        db_config = self.get_app_db_config(project=project)

        # If localhost project, just point to the local instance of mysql.
        if (project and project == 'localhost') or (self.project and self.project == 'localhost'):
            passwd = 'root' if user == 'root' else 'rdr!pwd'
            os.environ['DB_CONNECTION_STRING'] = f'mysql+mysqldb://{user}:{passwd}@127.0.0.1:3306/rdr?charset=utf8mb4'
            return 1

        _logger.debug("Starting google sql proxy...")
        self._sql_proxy_port = port = port if port else random.randint(10000, 65535)
        instance = instance if instance else gcp_format_sql_instance(
            project if project else self.project, port=port, replica=replica)

        self._sql_proxy_process = gcp_activate_sql_proxy(instance)

        if self._sql_proxy_process:
            time.sleep(6)  # allow time for sql connection to be made.
            cfg_user = 'root' if user == 'root' else 'rdr'
            passwd = db_config[f'{cfg_user}_db_password']
            os.environ['DB_CONNECTION_STRING'] = f'mysql+mysqldb://{user}:{passwd}@127.0.0.1:{port}/rdr?charset=utf8mb4'
            return self._sql_proxy_process.pid

        _logger.error('Failed to activate sql proxy.')

        return 0

    def make_mysqldb_connection(self, user: str = 'rdr', database: str = 'rdr'):
        """
        Make a standard mysql db connection to the database.
        :return: MySQLDB object.
        """
        if self.project != 'localhost' and not self._sql_proxy_process:
            raise EnvironmentError("'activate_sql_proxy' method must be called first.")

        db_config = self.get_app_db_config(project=self.project)
        cfg_user = 'root' if user == 'root' else 'rdr'
        passwd = db_config[f'{cfg_user}_db_password']

        import MySQLdb

        self._mysql_connection = MySQLdb.connect(user=user, passwd=passwd, database=database,
                                  host='127.0.0.1', port=self._sql_proxy_port, connect_timeout=30,
                                  charset='utf8')
        return self._mysql_connection


class GCPProcessContext(object):
    """
  A processing context manager for GCP operations
  """

    _command = None
    _project = None
    _account = None
    _service_account = None
    _env = None

    _env_config_obj = None

    def __init__(self, command, project, account=None, service_account=None):
        """
        Initialize GCP Context Manager
        :param command: command name
        :param project: gcp project name
        :param account: pmi-ops account
        :param service_account: gcp iam service account
        """
        if not command:
            _logger.error("command not set, aborting.")
            exit(1)

        self._command = command
        self._project = project
        self._account = account
        self._service_account = service_account

        write_pidfile_or_die(command)
        self._env = gcp_initialize(project, account, service_account)
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
