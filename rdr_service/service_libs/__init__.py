# pylint: disable=superfluous-parens

import logging
import traceback

from services.gcp_utils import gcp_initialize, gcp_cleanup
from services.system_utils import write_pidfile_or_die, remove_pidfile

_logger = logging.getLogger('rdr_logger')


class GCPProcessContext(object):
  """
  A processing context manager for GCP operations
  """
  _command = None
  _project = None
  _account = None
  _service_account = None
  _env = None

  def __init__(self, command, project, account=None, service_account=None):
    """
    Initialize GCP Contect Manager
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
    return self._env

  def __exit__(self, exc_type, exc_val, exc_tb):
    gcp_cleanup(self._account)
    remove_pidfile(self._command)

    if exc_type is not None:
      print(traceback.format_exc())
      _logger.error('program encountered an unexpected error, quitting.')
      exit(1)
