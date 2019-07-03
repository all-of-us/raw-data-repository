# pylint: disable=superfluous-parens

import logging
import time
import traceback

from services.gcp_utils import gcp_initialize, gcp_cleanup, gcp_activate_sql_proxy
from services.system_utils import write_pidfile_or_die, remove_pidfile

_logger = logging.getLogger('rdr_logger')

class GCPEnvConfigObject(object):
  """ GCP environment configuration object """

  _sql_proxy_process = None

  def __init__(self, items):
    """
    :param items: dict of config key value pairs
    """
    for key, val in items.iteritems():
      self.__dict__[key] = val


  def cleanup(self):
    """ Clean up or close everything we need to """
    if self._sql_proxy_process:
      self._sql_proxy_process.terminate()


  def activate_sql_proxy(self, instances):
    """
    Activate a google sql proxy instance service
    :param instances: string of instances to connect to
    :return: pid or None
    """
    if not instances:
      raise ValueError('invalid instance value')

    if self._sql_proxy_process:
      self._sql_proxy_process.terminate()
      self._sql_proxy_process = None

    self._sql_proxy_process = gcp_activate_sql_proxy(instances)
    if self._sql_proxy_process:
      time.sleep(6)  # allow time for sql connection to be made.
    return self._sql_proxy_process.pid

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
      _logger.error('command not set, aborting.')
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
      print(traceback.format_exc())
      _logger.error('program encountered an unexpected error, quitting.')
      exit(1)
