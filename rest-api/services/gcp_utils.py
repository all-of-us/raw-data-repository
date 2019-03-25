#
# Google Cloud Platform helpers
#
# !!! This file is python 3.x compliant !!!
#

import logging
import os
from random import choice
import shlex
import subprocess

from system_utils import run_external_program, which
from gcp_config import GCP_INSTANCES, GCP_REPLICA_INSTANCES, GCP_SERVICE_KEY_STORE

_logger = logging.getLogger(__name__)


def gcp_set_account(account):
  """
  Call gcloud to set current account
  :param account: gcloud account
  :return: True if successful otherwise False
  """

  _logger.debug('setting authentication login to {0}'.format(account))

  if not account:
    _logger.error('no GCP account given, aborting...')
    return False

  prog = which('gcloud')
  args = shlex.split('{0} auth login {1}'.format(prog, account))

  # pylint: disable=W0612
  pcode, so, se = run_external_program(args)

  if pcode != 0:
    _logger.error('failed to set gcp auth login account. ({0}: {1})'.format(pcode, so))
    return False

  _logger.debug('successfully set gcp auth login account')

  return True


# TODO: Create gcp_set_project function

def gcp_format_sql_instance(project, port=3320, replica=False):
  """
  Use the project and port to craft a cloud_sql_proxy instance string
  :param project: project name
  :param port: local tcp port
  :param replica: use replica instance
  :return: instance string
  """
  name = GCP_INSTANCES[project] if not replica else GCP_REPLICA_INSTANCES[project]
  instance = '{0}=tcp:{1}'.format(name, port)

  return instance

def gcp_activate_sql_proxy(instances):
  """
  Call cloud_sql_proxy to make a connection to the given instance.
  :param instances: full instance information, format "name:location:database=tcp:PORT, ...".
  :return: popen object
  """
  prog = which('cloud_sql_proxy')

  if not prog:
    raise IOError('cloud_sql_proxy executable not found')

  p = subprocess.Popen(shlex.split('{0} -instances={1}'.format(prog, instances)))

  return p

def gcp_create_iam_service_creds(account, creds_account=None):
  """
  # Note: Untested
  :param account:
  :param creds_account:
  :return: reference key
  """

  # make sure key store directory exists
  if not os.path.exists(GCP_SERVICE_KEY_STORE):
    os.makedirs(GCP_SERVICE_KEY_STORE)

  # make sure we never duplicate an existing key
  while True:
    service_key = '{0}.json'.format(''.join(choice('0123456789ABCDEF') for _ in xrange(6)))
    service_key_file = os.path.join(GCP_SERVICE_KEY_STORE, service_key)

    if not os.path.exists(os.path.join(GCP_SERVICE_KEY_STORE, service_key_file)):
      break

  if creds_account is None:
    creds_account = account

  prog = which('gcloud')
  args = shlex.split('{0} iam service-accounts keys create "{1}" --iam-account={2} --account={3}'
                     .format(prog, service_key_file, account, creds_account))

  # pylint: disable=W0612
  pcode, so, se = run_external_program(args)

  if pcode != 0:
    _logger.error('failed to create iam service account key. ({0}: {1})'.format(pcode, so))
    return False

  _logger.debug('successfully created iam service account key ({0})'.format(service_key))

  return service_key


def gcp_delete_iam_service_creds(service_key, account, creds_account=None):
  """
  # Note: Untested
  :param service_key:
  :param project:
  :param account:
  :param creds_account:
  :return:
  """

  srv_key_file = os.path.join(GCP_SERVICE_KEY_STORE, service_key)

  if not os.path.exists(srv_key_file):
    _logger.error('specified iam service key does not exist ({0})'.format(service_key))
    return False

  if creds_account is None:
    creds_account = account

  prog = which('gcloud')
  args = shlex.split('{0} iam service-accounts keys delete "{1}" --iam-account={2} --account={3}'
                     .format(prog, srv_key_file, account, creds_account))

  # pylint: disable=W0612
  pcode, so, se = run_external_program(args)

  if pcode != 0:
    _logger.error('failed to delete iam service account key. ({0}: {1})'.format(pcode, so))
    return False

  _logger.debug('successfully deleted iam service account key ({0})'.format(service_key))

  return service_key
