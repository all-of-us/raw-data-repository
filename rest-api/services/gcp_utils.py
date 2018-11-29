#
# Authors: Robert Abram <robert.m.abram@vumc.org>
#
# Google Cloud Platform helpers
#
#

import logging
from system_utils import run_external_program, which

_logger = logging.getLogger(__name__)


def gcp_set_account(account):
  """
  Call gcloud to set current account
  :param account: gcloud account
  :return: True if successful otherwise False
  """

  if not account:
    return False

  prog = which('gcloud')

  args = [prog, 'auth', 'login', account]
  code, so, se = run_external_program(args, )

  if code != 0:
    _logger.error('failed to set gcp auth login account. ({0}: {1})'.format(code, so))
    return False

  _logger.debug('successfully set gcp auth login account')

  return True

def gcp_activate_proxy():

  # cloud_sql_proxy -instances=myProject:us-central1:myInstance=tcp:3306,myProject:us-central1:myInstance2=tcp:3307 &
# mysql -u myUser --host 127.0.0.1  --port 3307
