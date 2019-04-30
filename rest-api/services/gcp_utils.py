#
# Google Cloud Platform helpers
#
# !!! This file is python 3.x compliant !!!
#
# superfluous-parens
# pylint: disable=W0612
import glob
import logging
import os
import shlex
import subprocess
from collections import OrderedDict
from random import choice

from gcp_config import GCP_INSTANCES, GCP_REPLICA_INSTANCES, GCP_SERVICE_KEY_STORE, GCP_PROJECTS
from system_utils import run_external_program, which

_logger = logging.getLogger('rdr_logger')


def gcp_test_environment():
  """
  Make sure the local environment is good
  :return: True if yes, False if not.
  """
  progs = ['gcloud', 'gsutil', 'cloud_sql_proxy', 'grep']

  for prog in progs:

    if not which(prog):
      _logger.error('[{0}] executable is not found.'.format(prog))
      return False

  # TODO: Future: put additional checks here as needed, IE: required environment vars.

  _logger.info('local environment is good.')
  return True

def gcp_validate_project(project):
  """
  Make sure project given is a valid GCP project. Allow short or long project names.
  :param project: project name
  :return: long project id or None if invalid
  """
  if not project:
    _logger.error('project name not set, unable to validate.')
    return None
  if project in ['localhost', '127.0.0.1']:
    return project
  # check for full length project name
  if 'pmi-drc-api' in project or 'all-of-us-rdr' in project:
    if project not in GCP_PROJECTS:
      _logger.error('invalid project name [{0}].'.format(project))
      return None
    return project

  # check short project name
  if 'test' in project:
    project = 'pmi-drc-api-{0}'.format(project)
  else:
    project = 'all-of-us-rdr-{0}'.format(project)

  if project not in GCP_PROJECTS:
    _logger.error('invalid project name [{0}].'.format(project))
    return None

  return project

def gcp_get_project_short_name(project):
  """
  Return the short name for the given project
  :param project: project name
  :return: project short name
  """
  if not project:
    return None

  if project in ['localhost', '127.0.0.1']:
    return project

  project = gcp_validate_project(project)

  if not project:
    return None

  return project.split('-')[-1]

def gcp_initialize(project, account=None, service_account=None):
  """
  Apply settings to local GCP environment. This must be called first to set the
  account and project.
  :param project: gcp project name
  :param account: pmi-ops account
  :param service_account: gcp iam service account
  :return: environment dict
  """
  if not gcp_test_environment():
    return None
  if project:
    if project not in ['localhost', '127.0.0.1'] and not gcp_validate_project(project):
      return None
  else:
    project = 'localhost'

  # Use the account and service_account parameters if set, otherwise try the environment var.
  account = account if account else \
            (os.environ['RDR_ACCOUNT'] if 'RDR_ACCOUNT' in os.environ else None)
  service_account = service_account if service_account else \
            (os.environ['RDR_SERVICE_ACCOUNT'] if 'RDR_SERVICE_ACCOUNT' in os.environ else None)

  env = OrderedDict()
  env['project'] = project
  env['account'] = account
  env['service_account'] = service_account
  env['service_key_id'] = None

  if account and not gcp_activate_account(account):
    return None
  # if this is a local project, just return now.
  if project in ['localhost', '127.0.0.1']:
    return env
  # Set current project.
  if not gcp_set_config('project', project):
    return False
  # set service account and generate a service key.
  if service_account:
    env['service_key_id'] = gcp_create_iam_service_key(service_account, account)
    if not env['service_key_id']:
      return None
    if not gcp_activate_iam_service_key(env['service_key_id']):
      return None

  for key, value in env.items():
    _logger.info('{0}: [{1}]'.format(key, value))

  return env

def gcp_cleanup(account):
  """
  Clean up items to do at the program's completion.
  """
  # activate the pmi-ops account so we can delete.
  if account:
    gcp_activate_account(account)

  # Scan for keys in GCP_SERVICE_KEY_STORE and delete them.
  service_key_path = os.path.join(GCP_SERVICE_KEY_STORE, '*.json')
  files = glob.glob(service_key_path)

  for filename in files:
    service_key_id = os.path.basename(filename).split('.')[0]
    gcp_delete_iam_service_key(service_key_id)


def gcp_gcloud_command(group, args, flags=None):
  """
  Run a gcloud command
  :param group: group name
  :param args: command arguments
  :param flags: additional flags to pass to gcloud executable
  :return: (exit code, stdout, stderr)
  """
  if not group or not args or not isinstance(args, str):
    _logger.error('invalid parameters passed to gcp_gcloud_command.')
    return False

  prog = which('gcloud')
  args = shlex.split('{0} {1} {2} {3}'.format(prog, group, args, flags if flags else ''))

  return run_external_program(args)

def gcp_gsutil_command(cmd, args, flags=None):
  """
  Run a gsutil command
  :param cmd: gsutil command name
  :param args: command arguments
  :param flags: additional flags to pass to gsutil executable
  :return: (exit code, stdout, stderr)
  """
  if not cmd or not args or not isinstance(args, str):
    _logger.error('invalid parameters passed to gcp_gsutil_command.')
    return False

  prog = which('gsutil')
  args = shlex.split('{0} {1} {2} {3}'.format(prog, flags if flags else '', cmd, args))

  return run_external_program(args)

def gcp_set_config(prop, value, flags=None):
  """
  Generic function to set the local GCP SDK config properties.
  https://cloud.google.com/sdk/gcloud/reference/config/set
  :param prop: property name to set value to
  :param value: property value string
  :param flags: additional flags to pass to gcloud executable
  :return: True if successful otherwise False
  """
  if not prop or not value or not isinstance(value, str):
    _logger.error('invalid parameters passed to gcp_set_config.')
    return False

  if prop.lower() == 'project':
    value = gcp_validate_project(value)
    if not value:
      _logger.error('"{0}" is an invalid project.'.format(value))
      return False

  _logger.debug('setting gcp config property "{0}" to "{1}".'.format(prop, value))

  # Ex: 'gcloud config set prop value'
  args = 'set {0} {1}'.format(prop, value)
  pcode, so, se = gcp_gcloud_command('config', args, flags)

  if pcode != 0:
    _logger.error('failed to set gcp config property. ({0}: {1}).'.format(pcode, so))
    return False

  _logger.debug('successfully set gcp config property.')

  if prop.lower() == 'project':
    _logger.info('The current project is now [{0}].'.format(value))
  else:
    _logger.info('config: [{0}] is now [{1}].'.format(prop, value))

  return True

def gcp_unset_config(prop, value, flags=None):
  """
  Generic function to unset the local GCP SDK config properties.
  https://cloud.google.com/sdk/gcloud/reference/config/set
  :param prop: property name to unset value
  :param value: property value string
  :param flags: additional flags to pass to gcloud executable
  :return: True if successful otherwise False
  """
  if not prop or not value or not isinstance(value, str):
    _logger.error('invalid parameters passed to gcp_unset_config.')
    return False

  _logger.debug('setting gcp config property "{0}" to "{1}".'.format(prop, value))

  # Ex: 'gcloud config unset prop value'
  args = 'unset {0} {1}'.format(prop, value)
  pcode, so, se = gcp_gcloud_command('config', args, flags)

  if pcode != 0:
    _logger.error('failed to unset gcp config property. ({0}: {1}).'.format(pcode, so))
    return False

  _logger.debug('successfully unset gcp config property.')

  return True

def gcp_get_config(prop, flags=None):
  """
  Generic function to get a value from the local GCP SDK config properties.
  https://cloud.google.com/sdk/gcloud/reference/config/set
  :param prop: property name to get value
  :param flags: additional flags to pass to gcloud executable
  :return: config property value
  """
  if not prop:
    _logger.error('invalid parameters passed to gcp_unset_config.')
    return None

  _logger.debug('getting gcp config property "{0}".'.format(prop))

  # Ex: 'gcloud config get-value prop'
  args = 'get-value {0}'.format(prop)
  pcode, so, se = gcp_gcloud_command('config', args, flags)

  if pcode != 0:
    _logger.error('failed to get gcp config property. ({0}: {1}).'.format(pcode, so))
    return None

  _logger.debug('successfully unset gcp config property.')

  return so.strip()

def gcp_activate_account(account, flags=None):
  """
  Call gcloud to set current account
  :param account: pmi-ops account
  :param flags: additional flags to pass to gcloud command
  :return: True if successful otherwise False
  """
  _logger.debug('setting activate gcp account to {0}.'.format(account))

  if not account:
    _logger.error('no GCP account given, aborting.')
    return False

  # Ex: 'gcloud auth login xxx.xxx@pmi-ops.org'
  args = 'login {0}'.format(account)
  pcode, so, se = gcp_gcloud_command('auth', args, flags)

  if pcode != 0:
    _logger.error('failed to set gcp auth login account. ({0}: {1}).'.format(pcode, so))
    return False

  _logger.debug('successfully set account to active.')

  lines = se.split('\n')
  for line in lines:
    if 'You are now logged in as' in line:
      _logger.debug(line)

  return True

def gcp_get_app_host_name(project=None):
  """
  Return the App Engine hostname for the given project
  :param project: gcp project name
  :return: hostname
  """
  # Get the currently configured project
  if not project:
    project = gcp_get_config('project')

  if project in ['localhost', '127.0.0.1']:
    return project

  project = gcp_validate_project(project)
  if not project:
    _logger.error('"{0}" is an invalid project.'.format(project))
    return None

  host = "{0}.appspot.com".format(project)
  return host

def gcp_get_app_access_token():
  """
  Get the OAuth2 access token for active gcp account.
  :return: access token string
  """
  args = 'print-access-token'
  pcode, so, se = gcp_gcloud_command('auth', args)

  if pcode != 0:
    _logger.error('failed to retrieve auth access token. ({0}: {1}).'.format(pcode, se))
    return None

  _logger.debug('retrieved auth access token.')

  return so.strip()

def gcp_make_auth_header():
  """
  Make an oauth authentication header
  :return: dict
  """
  headers = dict()
  headers['Authorization'] = 'Bearer {0}'.format(gcp_get_app_access_token())
  return headers

def gcp_get_private_key_id(service_key_path):
  """
  Return the private key id for the given key file.
  :param service_key_path: path to service key json file.
  :return: private key id, service account
  """
  private_key = None
  service_account = None

  if not os.path.exists(service_key_path):
    _logger.error('service key file not found ({0}).'.format(service_key_path))
    return private_key

  lines = open(service_key_path).readlines()
  for line in lines:
    if 'private_key_id' in line:
      private_key = shlex.split(line)[1].replace(',', '')
    if 'client_email' in line:
      service_account = shlex.split(line)[1].replace(',', '')

  return private_key, service_account

def gcp_create_iam_service_key(service_account, account=None):
  """
  # Note: Untested
  :param service_account: service account
  :param account: authenticated account if needed
  :return: service key id
  """
  _logger.debug('creating iam service key for service account [{0}].'.format(service_account))

  # make sure key store directory exists
  if not os.path.exists(GCP_SERVICE_KEY_STORE):
    os.makedirs(GCP_SERVICE_KEY_STORE)

  # make sure we never duplicate an existing key
  while True:
    service_key_id = ''.join(choice('0123456789ABCDEF') for _ in xrange(12))
    service_key_file = '{0}.json'.format(service_key_id)
    service_key_path = os.path.join(GCP_SERVICE_KEY_STORE, service_key_file)

    if not os.path.exists(os.path.join(GCP_SERVICE_KEY_STORE, service_key_path)):
      break

  # Ex: 'gcloud iam service-accounts keys create "path/key.json" ...'
  args = 'service-accounts keys create "{0}"'.format(service_key_path)
  flags = '--iam-account={0}'.format(service_account)
  if account:
    flags += ' --account={0}'.format(account)
  pcode, so, se = gcp_gcloud_command('iam', args, flags)

  if pcode != 0:
    _logger.error('failed to create iam service account key. ({0}: {1})'.format(pcode, se))
    return None

  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_key_path

  pkid, sa = gcp_get_private_key_id(service_key_path)

  _logger.info('created key file [{0}] with id [{1}]'.format(service_key_id, pkid))

  return service_key_id

def gcp_delete_iam_service_key(service_key_id, creds_account=None):
  """
  # Note: Untested
  :param service_key_id: local service key file ID
  :param creds_account: authenticated account if needed
  :return: True if successful else False
  """
  _logger.debug('deleting iam service key [{0}].'.format(service_key_id))

  service_key_file = '{0}.json'.format(service_key_id)
  service_key_path = os.path.join(GCP_SERVICE_KEY_STORE, service_key_file)

  if not os.path.exists(service_key_path):
    _logger.error('service key file does not exist ({0})'.format(service_key_id))
    return False

  # Get the private key value so we can delete the key
  pkid, service_account = gcp_get_private_key_id(service_key_path)
  if not pkid:
    return False

  # Ex: 'gcloud iam service-accounts keys delete "private key id" ...'
  args = 'service-accounts keys delete "{0}"'.format(pkid)
  flags = '--quiet --iam-account={0}'.format(service_account)
  if creds_account:
    flags += ' --account={0}'.format(creds_account)

  pcode, so, se = gcp_gcloud_command('iam', args, flags)

  if pcode != 0:
    _logger.warning('failed to delete iam service account key. ({0}: {1})'.format(pcode, se))
    if 'NOT_FOUND' in se:
      os.remove(service_key_path)
    return False

  os.remove(service_key_path)

  _logger.info('deleted service account key [{0}] with id [{1}]'.format(service_key_id, pkid))

  return True

def gcp_activate_iam_service_key(service_key_id, flags=None):
  """
  Activate the service account key
  :param service_key_id: local service key file ID
  :return: True if successful else False
  """
  _logger.debug('activating iam service key [{0}].'.format(service_key_id))

  service_key_file = '{0}.json'.format(service_key_id)
  service_key_path = os.path.join(GCP_SERVICE_KEY_STORE, service_key_file)

  if not os.path.exists(service_key_path):
    _logger.error('service key file does not exist ({0})'.format(service_key_id))
    return False

  # Get the private key value so we can delete the key
  pkid, service_account = gcp_get_private_key_id(service_key_path)
  if not pkid:
    return False

  args = 'activate-service-account --key-file={0}'.format(service_key_path)
  pcode, so, se = gcp_gcloud_command('auth', args, flags)

  if pcode != 0:
    _logger.error('failed to activate iam service account key. ({0}: {1})'.format(pcode, se))
    return False

  _logger.info('activated iam service key [{0}] with id [{1}].'.format(service_key_id, pkid))

  return True

def gcp_format_sql_instance(project, port=3320, replica=False):
  """
  Use the project and port to craft a cloud_sql_proxy instance string
  :param project: project name
  :param port: local tcp port
  :param replica: use replica instance
  :return: instance string
  """
  # We don't check for a localhost project here, because establishing a proxy to localhost
  # does not make sense.
  project = gcp_validate_project(project)
  if not project:
    _logger.error('"{0}" is an invalid gcp project.'.format(project))
    return None

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
  p = subprocess.Popen(shlex.split('{0} -instances={1}'.format(prog, instances)))

  return p

