#! /bin/env python
#
# Simply verify the environment is valid for running the client apps.
#

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import importlib
import logging
import sys
import traceback

import argparse
from services.gcp_utils import gcp_initialize, \
  gcp_cleanup, gcp_get_app_host_name, gcp_get_app_access_token
from services.system_utils import setup_logging, setup_unicode, write_pidfile_or_die, \
  remove_pidfile, make_api_request

_logger = logging.getLogger('rdr_logger')

group = 'verify'
group_desc = 'test local environment'


class Verify(object):

  def __init__(self, args):
    self.args = args

  def run(self):
    """
    Main program process
    :return: Exit code value
    """
    result = 0
    requests_mod = False
    #
    # python modules that need to be installed in the local system
    #
    modules = ['requests', 'urllib3']

    for module in modules:
      try:
        mod = importlib.import_module(module)
        _logger.info('found python module [{0}].'.format(mod.__name__))
        if module == 'requests':
          requests_mod = True
      except ImportError:
        _logger.error('missing python [{0}] module, please run "pip --install {0}"'.format(module))
        result = 1

    if self.args.project in ['localhost', '127.0.0.1']:
      _logger.warning('unable to perform additional testing unless project parameter is set.')
      return result


    # Try making some API calls to to verify OAuth2 token functions.
    if requests_mod:

      host = gcp_get_app_host_name(self.args.project)
      url = 'rdr/v1'

      _logger.info('attempting simple api request.')
      code, resp = make_api_request(host, url)

      if code != 200 or 'version_id' not in resp:
        _logger.error('simple api request failed')
        return 1

      _logger.info('{0} version is [{1}].'.format(host, resp['version_id']))

      # verify OAuth2 token can be retrieved.
      token = gcp_get_app_access_token()
      if token and token.startswith('ya'):
        _logger.info('verified app authentication token.')

        # TODO: Make an authenticated API call here. What APIs are avail for this?

      else:
        _logger.error('app authentication token verification failed.')
        result = 1

    else:
      _logger.warning('skipping api request tests.')

    return result


def run():
  # Set global debug value and setup application logging.
  setup_logging(_logger, group, '--debug' in sys.argv, '{0}.log'.format(group) if '--log-file' in sys.argv else None)
  setup_unicode()
  exit_code = 1

  # Setup program arguments.
  parser = argparse.ArgumentParser(prog=group, description=group_desc)
  parser.add_argument('--debug', help='Enable debug output', default=False, action='store_true')  # noqa
  parser.add_argument('--log-file', help='write output to a log file', default=False, action='store_true')  # noqa
  parser.add_argument('--project', help='gcp project name', default='localhost')  # noqa
  parser.add_argument('--account', help='pmi-ops account', default=None)  # noqa
  parser.add_argument('--service-account', help='gcp service account', default=None)  # noqa
  args = parser.parse_args()

  # Ensure only one copy of the program is running at the same time.
  write_pidfile_or_die(group)
  # initialize gcp environment.
  env = gcp_initialize(args.project, args.account, args.service_account)
  if not env:
    remove_pidfile(group)
    exit(exit_code)

  try:
    process = Verify(args)
    exit_code = process.run()
  except IOError:
    _logger.error('io error')
  except Exception:
    print(traceback.format_exc())
    _logger.error('program encountered an unexpected error, quitting.')
    exit_code = 1
  finally:
    gcp_cleanup(args.account)
    remove_pidfile(group)

  if exit_code != 0:
    _logger.warning('errors in the local environment have been detected, please fix.')
  _logger.info('done')
  exit(exit_code)


# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
