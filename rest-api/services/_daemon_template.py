#
# Authors: Robert Abram <rabram991@gmail.com>
#
# Template for system services.
#
# Notes: removed support for System V Init services.
#

import argparse
import logging
import os
import signal
import sys


from system_utils import setup_logging, setup_unicode
from daemon import Daemon

_logger = logging.getLogger(__name__)


progname = 'XXXXX-daemon'


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

      return 0

  def signal_term_handler(signal, frame):

    if not _daemon.stopProcessing:
      _logger.debug('received SIGTERM signal.')
    _daemon.stopProcessing = True

  # Set global debug value and setup application logging.
  setup_logging(_logger, progname, '--debug' in sys.argv, '{0}.log'.format(progname))

  setup_unicode()

  # Setup program arguments.
  parser = argparse.ArgumentParser(prog=progname)
  parser.add_argument('--debug', help=_('Enable debug output'), default=False, action='store_true')  # noqa
  parser.add_argument('--root-only', help=_('Must run as root user'), default=False, action='store_true')  # noqa
  parser.add_argument('--nodaemon', help=_('Do not daemonize process'), default=False,
                      action='store_true')  # noqa
  parser.add_argument('action', choices=('start', 'stop', 'restart'), default='')  # noqa

  args = parser.parse_args()

  if args.root_only is True and os.getuid() != 0:
    _logger.warning('daemon must be run as root')
    sys.exit(4)

  # --nodaemon only valid with start action
  if args.nodaemon and args.action != 'start':
    print('{0}: error: --nodaemon option not valid with stop or restart action'.format(progname))
    sys.exit(1)

  # Do not fork the daemon process for systemd service or debugging, run in foreground.
  if args.nodaemon is True:
    _logger.debug('running daemon in foreground.')
    _daemon = TemplateDaemon(args=args)
    _daemon.run()
  else:
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
