#! /bin/env python
#
# Template for RDR tool python program.
#

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys

import argparse
from tools.tool_libs import GCPProcessContext
from services.system_utils import setup_logging, setup_unicode

_logger = logging.getLogger('rdr_logger')

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = 'template'
tool_desc = 'put tool help description here'


class ProgramTemplateClass(object):

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
    # TODO: write program main process here after setting 'tool_cmd' and 'tool_desc'...
    return 0


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

  with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
    process = ProgramTemplateClass(args, gcp_env)
    exit_code = process.run()
    return exit_code


# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
