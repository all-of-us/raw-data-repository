#
# Authors: Robert Abram <robert.m.abram@vumc.org>
#
# !!! This file is python 3.x compliant !!!
#
#

# Note: disable specific pylint checks globally here.
# superfluous-parens
# pylint: disable=C0325


import argparse
import logging
import os
import signal
import sys
import time

from system_utils import setup_logging, setup_unicode, is_valid_email, which
from daemon import Daemon
from gcp_utils import gcp_set_account, gcp_activate_proxy

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

      result = gcp_set_account(args.account)
      if result is not True:
        _logger.error('failed to set authentication account, aborting.')
        return 1

      po_proxy = gcp_activate_proxy(args.enable_sandbox, args.enable_test)
      if not po_proxy:
        _logger.error('cloud_sql_proxy process failed, aborting.')
        return 1

      while self.stopProcessing is False:

        time.sleep(0.5)

      _logger.debug('stopping cloud_sql_proxy process...')

      po_proxy.kill()

      _logger.debug('stopped')

      return 0

  def signal_term_handler(_sig, _frame):

    if not _daemon.stopProcessing:
      _logger.debug('received SIGTERM signal.')
    _daemon.stopProcessing = True

  # Set global debug value and setup application logging.
  setup_logging(_logger, progname, '--debug' in sys.argv)

  setup_unicode()

  # Setup program arguments.
  parser = argparse.ArgumentParser(prog=progname)
  # pylint: disable=E0602
  parser.add_argument('--debug', help=_('Enable debug output'), default=False,
                      action='store_true')  # noqa
  # pylint: disable=E0602
  parser.add_argument('--root-only', help=_('Must run as root user'), default=False,
                      action='store_true')  # noqa
  # pylint: disable=E0602
  parser.add_argument('--nodaemon', help=_('Do not daemonize process'), default=False,
                      action='store_true')  # noqa
  # pylint: disable=E0602
  parser.add_argument('--account', help=_('Security account'))
  # pylint: disable=E0602
  parser.add_argument('--enable-sandbox', help=_('Add proxy to all-of-us-rdr-sandbox'),
                                                 default=False, action='store_true')  # noqa
  # pylint: disable=E0602
  parser.add_argument('--enable-test', help=_('Add proxy to pmi-drc-api-test'),
                      default=False, action='store_true')  # noqa
  parser.add_argument('action', choices=('start', 'stop', 'restart'), default='')  # noqa

  args = parser.parse_args()

  if args.root_only is True and os.getuid() != 0:
    _logger.warning('daemon must be run as root')
    sys.exit(4)

  if is_valid_email(args.account) is False:
    if 'RDR_ACCOUNT' not in os.environ:
      _logger.error('account is invalid')
    else:
      args.account = os.environ['RDR_ACCOUNT']

  if which('cloud_sql_proxy') is None:
    _logger.error('cloud_sql_proxy executable not found, ' +
                  'create symlink to cloud_sql_proxy in /usr/local/bin/ directory')

  # --nodaemon only valid with start action
  if args.nodaemon and args.action != 'start':
    print('{0}: error: --nodaemon option not valid with stop or restart action'.format(progname))
    sys.exit(1)

  _logger.info('  Account:          {0}'.format(args.account))

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
