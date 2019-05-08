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
from gcp_utils import gcp_activate_account, gcp_format_sql_instance, gcp_activate_sql_proxy

_logger = logging.getLogger('rdr_logger')

progname = 'gcp-db-daemon'


def run():

  class SQLProxyDaemon(Daemon):

    stopProcessing = False

    def __init__(self, *args, **kwargs):
      self._args = kwargs.pop('args', None)
      super(SQLProxyDaemon, self).__init__(*args, **kwargs)

    def print_instances(self):

      _logger.info('    all-of_us-rdr-prod:     primary    -> tcp: 127.0.0.1:9900')
      if self._args.enable_replica:
        _logger.info('    all-of_us-rdr-prod:     replica    -> tcp: 127.0.0.1:9905')

      _logger.info('    all-of_us-rdr-stable:   primary    -> tcp: 127.0.0.1:9910')
      if self._args.enable_replica:
        _logger.info('    all-of_us-rdr-stable:   replica    -> tcp: 127.0.0.1:9915')

      _logger.info('    all-of_us-rdr-staging:  primary    -> tcp: 127.0.0.1:9920')
      if self._args.enable_replica:
        _logger.info('    all-of_us-rdr-staging:  replica    -> tcp: 127.0.0.1:9925')

      if self._args.enable_sandbox is True:
        _logger.info('    all-of_us-rdr-sandbox:  primary    -> tcp: 127.0.0.1:9930')
        # Sandbox does not currently have a replica enabled
        # if self._args.enable_replica:
        #   _logger.info('    all-of_us-rdr-sandbox:     replica    -> tcp: 127.0.0.1:9935')

      if self._args.enable_test is True:
        _logger.info('    pmi-drc-api-test:       primary    -> tcp: 127.0.0.1:9940')
        if self._args.enable_replica:
          _logger.info('    pmi-drc-api-test:       replica    -> tcp: 127.0.0.1:9945')

    def get_instances(self):
      """
      Build all instances we are going to connect to
      :return: string
      """

      instances = ''

      instances += gcp_format_sql_instance('all-of-us-rdr-prod', 9900) + ','
      if self._args.enable_replica:
        instances += gcp_format_sql_instance('all-of-us-rdr-prod', 9905, True) + ','

      instances += gcp_format_sql_instance('all-of-us-rdr-stable', 9910) + ','
      if self._args.enable_replica:
        instances += gcp_format_sql_instance('all-of-us-rdr-stable', 9915, True) + ','

      instances += gcp_format_sql_instance('all-of-us-rdr-staging', 9920) + ','
      if self._args.enable_replica:
        instances += gcp_format_sql_instance('all-of-us-rdr-staging', 9925, True) + ','

      if self._args.enable_sandbox is True:
        instances += gcp_format_sql_instance('all-of-us-rdr-sandbox', 9930) + ','
        # Sandbox does not currently have a replica enabled
        # if self._args.enable_replica:
        #   instances += gcp_format_sql_instance('all-of-us-rdr-sandbox', 9935, True) + ','

      if self._args.enable_test is True:
        instances += gcp_format_sql_instance('pmi-drc-api-test', 9940) + ','
        if self._args.enable_replica:
          instances += gcp_format_sql_instance('pmi-drc-api-test', 9945, True) + ','

      # remove trailing comma
      instances = instances[:-1]

      return instances

    def run(self):
      """
      Main program process
      :return: Exit code value
      """

      # Set SIGTERM signal Handler
      signal.signal(signal.SIGTERM, signal_term_handler)
      _logger.debug('signal handlers set.')

      result = gcp_activate_account(args.account)
      if result is not True:
        _logger.error('failed to set authentication account, aborting.')
        return 1

      po_proxy = gcp_activate_sql_proxy(self.get_instances())
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
      _logger.warning('received SIGTERM signal.')
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
  parser.add_argument('--enable-replica', help=_('Enable connections to replica instances'),
                      default=False, action='store_true')  # noqa
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
      _logger.error('account parameter is invalid and RDR_ACCOUNT shell var is not set.')
      return 1
    else:
      args.account = os.environ['RDR_ACCOUNT']

  if which('cloud_sql_proxy') is None:
    _logger.error('cloud_sql_proxy executable not found, ' +
                  'create symlink to cloud_sql_proxy in /usr/local/bin/ directory')

  # --nodaemon only valid with start action
  if args.nodaemon and args.action != 'start':
    print('{0}: error: --nodaemon option not valid with stop or restart action'.format(progname))
    sys.exit(1)

  _logger.info('account:          {0}'.format(args.account))

  # Do not fork the daemon process for systemd service or debugging, run in foreground.
  if args.nodaemon is True:
    if args.action == 'start':
      _daemon = SQLProxyDaemon(args=args)
      _daemon.print_instances()
      _logger.info('running daemon in foreground.')
      _daemon.run()
  else:

    pidpath = os.path.expanduser('~/.local/run')
    pidfile = os.path.join(pidpath, '{0}.pid'.format(progname))

    logpath = os.path.expanduser('~/.local/log')
    logfile = os.path.join(logpath, '{0}.log'.format(progname))

    # Setup daemon object
    # make sure PID and Logging path exist
    if not os.path.exists(pidpath):
      os.makedirs(pidpath)
    if not os.path.exists(logpath):
      os.makedirs(logpath)

    _daemon = SQLProxyDaemon(
      procbase='',
      dirmask='0o700',
      pidfile=pidfile,
      uid='root',
      gid='root',
      stdin='/dev/null',
      stdout=logfile,
      stderr=logfile,
      args=args
    )

    if args.action == 'start':
      _daemon.print_instances()
      _logger.info('running daemon in background.')
      _daemon.start()
    elif args.action == 'stop':
      _logger.info('Stopping daemon.')
      _daemon.stop()
    elif args.action == 'restart':
      _logger.info('Restarting daemon.')
      _daemon.restart()

  return 0


# --- Main Program Call ---
if __name__ == '__main__':
  sys.exit(run())
