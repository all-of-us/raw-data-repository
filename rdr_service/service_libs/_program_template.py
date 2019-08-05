#! /bin/env python
#
# Template for RDR python program.
#

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys

import argparse
from service_libs import GCPProcessContext
from services.system_utils import setup_logging, setup_unicode

_logger = logging.getLogger('rdr_logger')

# mod_cmd and mod_desc name are required.
mod_cmd = 'template'
mod_desc = 'put program description here for help'


class ProgramTemplateClass(object):

  def __init__(self, args):
    self.args = args

  def run(self):
    """
    Main program process
    :return: Exit code value
    """
    # TODO: write program main process here after setting 'mod_cmd' and 'mod_desc'...
    return 0


def run():
  # Set global debug value and setup application logging.
  setup_logging(_logger, mod_cmd,
                '--debug' in sys.argv, '{0}.log'.format(mod_cmd) if '--log-file' in sys.argv else None)
  setup_unicode()

  # Setup program arguments.
  parser = argparse.ArgumentParser(prog=mod_cmd, description=mod_desc)
  parser.add_argument('--debug', help='Enable debug output', default=False, action='store_true')  # noqa
  parser.add_argument('--log-file', help='write output to a log file', default=False, action='store_true')  # noqa
  parser.add_argument('--project', help='gcp project name', default='localhost')  # noqa
  parser.add_argument('--account', help='pmi-ops account', default=None)  # noqa
  parser.add_argument('--service-account', help='gcp iam service account', default=None)  # noqa
  args = parser.parse_args()

  with GCPProcessContext(mod_cmd, args.project, args.account, args.service_account):
    process = ProgramTemplateClass(args)
    exit_code = process.run()
    return exit_code


# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
