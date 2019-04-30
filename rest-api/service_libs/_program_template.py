#! /bin/env python
#
# Template for RDR python program.
#

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys
import traceback

import argparse
from services.gcp_utils import gcp_initialize, gcp_cleanup
from services.system_utils import setup_logging, setup_unicode, write_pidfile_or_die, remove_pidfile

_logger = logging.getLogger('rdr_logger')

# group name is required.
group = 'template'
group_desc = 'put program description here for help'


class ProgramTemplateClass(object):

  def __init__(self, args):
    self.args = args

  def run(self):
    """
    Main program process
    :return: Exit code value
    """
    # TODO: write program main process here after setting 'group' and 'group_desc'...
    return 0


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
  parser.add_argument('--service-account', help='gcp iam service account', default=None)  # noqa
  args = parser.parse_args()

  # Ensure only one copy of the program is running at the same time
  write_pidfile_or_die(group)
  # initialize gcp environment.
  env = gcp_initialize(args.project, args.account, args.service_account)
  if not env:
    remove_pidfile(group)
    exit(exit_code)

  try:
    process = ProgramTemplateClass(args)
    exit_code = process.run()
  except IOError:
    _logger.error('io error')
  except Exception:
    print(traceback.format_exc())
    _logger.error('program encountered an unexpected error, quitting.')
  finally:
    gcp_cleanup(args.account)
    remove_pidfile(group)

  _logger.info('done')
  exit(exit_code)


# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
