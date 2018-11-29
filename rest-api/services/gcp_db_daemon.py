#
# Authors: Robert Abram <robert.m.abram@vumc.org>
#
#
#
#

import argparse
import logging
import os
import signal
import sys
import time

from system_utils import setup_logging, setup_unicode, is_valid_email
from daemon import Daemon
from gcp_config import validate_project
from gcp_utils import gcp_set_account

_logger = logging.getLogger(__name__)

progname = 'gcp-db-daemon'


def run():

  class TemplateDaemon(Daemon):

    stopProcessing = False

    def __init__(self, *args, **kwargs):
      self._args = kwargs.pop('args', None)
      super(TemplateDaemon, self).__init__(*args, **kwargs)

    def run(self):
      """
      Main program process
      :return: Exit code value
      """

      # Set SIGTERM signal Handler
      signal.signal(signal.SIGTERM, signal_term_handler)
      _logger.debug('signal handlers set.')

      project = validate_project(args.project)

      result = gcp_set_account(project)

      if result is not True:
        _logger.error('aborting')
        return 1

      while self.stopProcessing is False:

        time.sleep(0.5)

      return 0

  def signal_term_handler(signal, frame):

    if not _daemon.stopProcessing:
      _logger.debug('received SIGTERM signal.')
    _daemon.stopProcessing = True

  # Set global debug value and setup application logging.
  setup_logging(_logger, progname, '--debug' in sys.argv)

  setup_unicode()

  # Setup program arguments.
  parser = argparse.ArgumentParser(prog=progname)
  parser.add_argument('--debug', help=_('Enable debug output'), default=False, action='store_true')  # noqa
  parser.add_argument('--root-only', help=_('Must run as root user'), default=False, action='store_true')  # noqa
  parser.add_argument('--nodaemon', help=_('Do not daemonize process'), default=False,
                      action='store_true')  # noqa
  parser.add_argument('--account', help=_('Security account'), required=True)
  parser.add_argument('--project', help=_('GCP Project Name'), required=True)
  parser.add_argument('action', choices=('start', 'stop', 'restart'), default='')  # noqa

  args = parser.parse_args()

  if args.root_only is True and os.getuid() != 0:
    _logger.warning('daemon must be run as root')
    sys.exit(4)

  project = validate_project(args.project)

  if not project:
    _logger.error('invalid project name')
    sys.exit(1)

  if is_valid_email(args.account) is False:
    _logger.error('account is invalid')

  # --nodaemon only valid with start action
  if args.nodaemon and args.action != 'start':
    print('{0}: error: --nodaemon option not valid with stop or restart action'.format(progname))
    sys.exit(1)

  _logger.info('  Account:          {0}'.format(args.account))
  _logger.info('  Project:          {0}'.format(project))

  # Do not fork the daemon process for systemd service or debugging, run in foreground.
  if args.nodaemon is True:
    if args.action == 'start':
      _logger.debug('running daemon in foreground.')
    _daemon = TemplateDaemon(args=args)
    _daemon.run()
  else:
    if args.action == 'start':
      _logger.debug('running daemon in background.')
    # Setup daemon object

    _daemon = TemplateDaemon(
      procbase='/root/',
      dirmask='0o700',
      pidfile='/tmp/{0}.pid'.format(progname),
      uid='root',
      gid='root',
      stdin='/dev/null',
      stdout='/tmp/{0}.log'.format(progname),
      stderr='/tmp/{0}.log'.format(progname),
      args=args
    )

    if args.action == 'start':
      _logger.debug('Starting daemon.')
      _daemon.start()
    elif args.action == 'stop':
      _logger.debug('Stopping daemon.')
      _daemon.stop()
    elif args.action == 'restart':
      _logger.debug('Restarting daemon.')
      _daemon.restart()

  _logger.debug('done')

  return 0


# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
