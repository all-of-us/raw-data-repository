#! /bin/env python
#
# Template for RDR tool python program.
#

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import signal
import time
import sys

import argparse
from tools.tool_libs import GCPProcessContext
from rdr_service.services.system_utils import setup_logging, setup_unicode
from rdr_service.services.gcp_utils import gcp_get_app_access_token

_logger = logging.getLogger('rdr_logger')

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = 'oauth-token'
tool_desc = 'get oauth token for account or service account'

do_continue = True
original_sigint = signal.getsignal(signal.SIGINT)

class OAuthTokenClass(object):

  def __init__(self, args):
    self.args = args

  def run(self):
    """
    Main program process
    :return: Exit code value
    """
    global do_continue
    token = gcp_get_app_access_token()
    print('\ntoken: {0}\n'.format(token))
    print('press ctrl-c to clean up key')

    while do_continue:
      time.sleep(0.25)
    print('\ncleaning up...')
    return 0

def exit_program(signum, frame):  # pylint: disable=unused-argument
  """
  Gracefully handle ctrl-c
  """
  global do_continue
  do_continue = False
  # restore original handler
  signal.signal(signal.SIGINT, original_sigint)


def run():
  # Set global debug value and setup application logging.
  setup_logging(_logger, tool_cmd,
                '--debug' in sys.argv, '{0}.log'.format(tool_cmd) if '--log-file' in sys.argv else None)
  setup_unicode()

  # Setup program arguments.
  parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
  parser.add_argument('--debug', help='Enable debug output', default=False, action='store_true')  # noqa
  parser.add_argument('--log-file', help='write output to a log file', default=False, action='store_true')  # noqa
  parser.add_argument('--project', help='gcp project name', default='localhost')  # noqa
  parser.add_argument('--account', help='pmi-ops account', default=None)  # noqa
  parser.add_argument('--service-account', help='gcp iam service account', default=None)  # noqa
  args = parser.parse_args()

  global original_sigint
  original_sigint = signal.getsignal(signal.SIGINT)
  signal.signal(signal.SIGINT, exit_program)

  with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account):
    process = OAuthTokenClass(args)
    exit_code = process.run()
    return exit_code


# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
