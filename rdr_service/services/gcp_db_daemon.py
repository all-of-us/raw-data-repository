import argparse
from dataclasses import dataclass
import logging
import os
import signal
import sys
import time
from typing import Dict

from rdr_service.services.daemon import Daemon
from rdr_service.services.gcp_config import RdrEnvironment
from rdr_service.services.gcp_utils import build_gcp_instance_connection_name, gcp_activate_account,\
    gcp_activate_sql_proxy
from rdr_service.services.system_utils import is_valid_email, setup_logging, setup_i18n, which

_logger = logging.getLogger("rdr_logger")
progname = "gcp-db-daemon"

MYSQL_PROXY_KEY = 'mysql8'
POSTGRES_PROXY_KEY = 'postgres'


@dataclass
class DatabaseProxy:
    """Class for bundling primary and replica proxy ports for an environment"""
    name: str
    port: int


@dataclass
class ProxyData:
    primary: DatabaseProxy
    replica: DatabaseProxy = None  # Not every environment has a replica db
    extras: Dict[str, DatabaseProxy] = None  # Some servers have extra possible connections


def run():
    class SQLProxyDaemon(Daemon):

        stopProcessing = False

        def __init__(self, *args, **kwargs):
            self._args = kwargs.pop("args", None)
            super(SQLProxyDaemon, self).__init__(*args, **kwargs)

            self.host = '127.0.0.1'
            self.environment_proxy_port_map = {
                RdrEnvironment.PROD: ProxyData(
                    primary=DatabaseProxy(name='rdrmaindb', port=9900),
                    replica=DatabaseProxy(name='rdrbackupdb-a', port=9905)
                ),
                RdrEnvironment.STABLE: ProxyData(
                    primary=DatabaseProxy(name='rdrmaindb', port=9910),
                    replica=DatabaseProxy(name='rdrbackupdb', port=9915)
                ),
                RdrEnvironment.STAGING: ProxyData(
                    primary=DatabaseProxy(name='rdrmaindb', port=9920),
                    replica=DatabaseProxy(name='rdrbackupdb', port=9925)
                ),
                RdrEnvironment.SANDBOX: ProxyData(
                    primary=DatabaseProxy(name='rdrmaindb', port=9930)
                ),
                RdrEnvironment.TEST: ProxyData(
                    primary=DatabaseProxy(name='rdrmaindb', port=9940),
                    replica=DatabaseProxy(name='rdrbackupdb', port=9945),
                    extras={
                        MYSQL_PROXY_KEY: DatabaseProxy(name='rdrmysql8', port=9941),
                        POSTGRES_PROXY_KEY: DatabaseProxy(name='rdrpostgresql', port=9942)
                    }
                ),
                RdrEnvironment.CAREEVO_TEST: ProxyData(
                    primary=DatabaseProxy(name='rdrmaindb', port=9950),
                    replica=DatabaseProxy(name='rdrbackupdb', port=9955)
                ),
                RdrEnvironment.PTSC_1_TEST: ProxyData(
                    primary=DatabaseProxy(name='rdrmaindb', port=9960),
                    replica=DatabaseProxy(name='rdrbackupdb', port=9965)
                ),
                RdrEnvironment.PTSC_2_TEST: ProxyData(
                    primary=DatabaseProxy(name='rdrmaindb', port=9970),
                    replica=DatabaseProxy(name='rdrbackupdb', port=9975)
                ),
                RdrEnvironment.PTSC_3_TEST: ProxyData(
                    primary=DatabaseProxy(name='rdrmaindb', port=9980),
                    replica=DatabaseProxy(name='rdrbackupdb', port=9985)
                )
            }

            self.environments_to_activate = [RdrEnvironment.PROD, RdrEnvironment.STABLE, RdrEnvironment.STAGING]

            if self._args.enable_sandbox:
                self.environments_to_activate.append(RdrEnvironment.SANDBOX)
            if self._args.enable_test:
                self.environments_to_activate.append(RdrEnvironment.TEST)
            if self._args.enable_care_evo:
                self.environments_to_activate.append(RdrEnvironment.CAREEVO_TEST)
            if self._args.enable_ptsc_1_test:
                self.environments_to_activate.append(RdrEnvironment.PTSC_1_TEST)
            if self._args.enable_ptsc_2_test:
                self.environments_to_activate.append(RdrEnvironment.PTSC_2_TEST)
            if self._args.enable_ptsc_3_test:
                self.environments_to_activate.append(RdrEnvironment.PTSC_3_TEST)

        def _print_instance_line(self, project_name, db_type, port_number):
            project_name_display = f'{project_name}:'.ljust(30)
            _logger.info(f'    {project_name_display}{db_type.ljust(15)}-> tcp: {self.host}:{port_number}')

        def print_instances(self):
            for environment in self.environments_to_activate:
                proxy_data = self.environment_proxy_port_map[environment]
                self._print_instance_line(environment.value, 'primary', proxy_data.primary.port)
                if self._args.enable_replica and proxy_data.replica is not None:
                    self._print_instance_line(environment.value, 'replica', proxy_data.replica.port)

            test_proxy_data = self.environment_proxy_port_map[RdrEnvironment.TEST]
            if self._args.enable_test_mysql_8:
                mysql8_proxy = test_proxy_data.extras[MYSQL_PROXY_KEY]
                self._print_instance_line(RdrEnvironment.TEST.value, 'mysql8', mysql8_proxy.port)
            if self._args.enable_test_postgres:
                postgres_proxy = test_proxy_data.extras[POSTGRES_PROXY_KEY]
                self._print_instance_line(RdrEnvironment.TEST.value, 'postgres', postgres_proxy.port)

        def _get_instance_arg_list_for_environment(self, environment: RdrEnvironment):
            proxy_data = self.environment_proxy_port_map[environment]
            instance_arg_list = [build_gcp_instance_connection_name(
                project_name=environment.value,
                port=proxy_data.primary.port,
                database_name=proxy_data.primary.name
            )]

            if self._args.enable_replica and proxy_data.replica is not None:
                instance_arg_list.append(
                    build_gcp_instance_connection_name(
                        project_name=environment.value,
                        port=proxy_data.replica.port,
                        database_name=proxy_data.replica.name
                    )
                )

            return instance_arg_list

        def get_instances_arg_str(self):
            """
            Build all instances we are going to connect to
            :return: string
            """
            instance_string_list = []
            for environment in self.environments_to_activate:
                instance_string_list.extend(self._get_instance_arg_list_for_environment(environment))

            test_proxy_data = self.environment_proxy_port_map[RdrEnvironment.TEST]
            if self._args.enable_test_mysql_8:
                mysql8_proxy = test_proxy_data.extras[MYSQL_PROXY_KEY]
                instance_string_list.append(build_gcp_instance_connection_name(
                    project_name=RdrEnvironment.TEST.value,
                    port=mysql8_proxy.port,
                    database_name=mysql8_proxy.name
                ))
            if self._args.enable_test_postgres:
                postgres_proxy = test_proxy_data.extras[POSTGRES_PROXY_KEY]
                instance_string_list.append(build_gcp_instance_connection_name(
                    project_name=RdrEnvironment.TEST.value,
                    port=postgres_proxy.port,
                    database_name=postgres_proxy.name
                ))

            return ','.join(instance_string_list)

        def run(self):
            """
      Main program process
      :return: Exit code value
      """

            # Set SIGTERM signal Handler
            signal.signal(signal.SIGTERM, signal_term_handler)
            _logger.debug("signal handlers set.")

            result = gcp_activate_account(args.account)
            if result is not True:
                _logger.error("failed to set authentication account, aborting.")
                return 1

            po_proxy = gcp_activate_sql_proxy(self.get_instances_arg_str())
            if not po_proxy:
                _logger.error("cloud_sql_proxy process failed, aborting.")
                return 1

            while self.stopProcessing is False:

                time.sleep(0.5)

            _logger.debug("stopping cloud_sql_proxy process...")

            po_proxy.kill()

            _logger.debug("stopped")

            return 0

    def signal_term_handler(_sig, _frame):

        if not _daemon.stopProcessing:
            _logger.warning("received SIGTERM signal.")
        _daemon.stopProcessing = True

    # Set global debug value and setup application logging.
    setup_logging(_logger, progname, "--debug" in sys.argv)

    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=progname)
    parser.add_argument("--debug", help="Enable debug output", default=False, action="store_true")
    parser.add_argument("--root-only", help="Must run as root user", default=False, action="store_true")
    parser.add_argument("--nodaemon", help="Do not daemonize process", default=False, action="store_true")
    parser.add_argument("--account", help="Security account")
    parser.add_argument(
        "--enable-replica", help="Enable connections to replica instances", default=False, action="store_true"
    )
    parser.add_argument(
        "--enable-sandbox", help="Add proxy to all-of-us-rdr-sandbox", default=False, action="store_true"
    )
    parser.add_argument(
        "--enable-test", help="Add proxy to pmi-drc-api-test", default=False, action="store_true"
    )
    parser.add_argument('--enable-care-evo', help='Add proxy to all-of-us-rdr-careevo-test',
                        default=False, action='store_true')
    parser.add_argument('--enable-ptsc-1-test', help='Add proxy to all-of-us-rdr-ptsc-1-test',
                        default=False, action='store_true')
    parser.add_argument('--enable-ptsc-2-test', help='Add proxy to all-of-us-rdr-ptsc-2-test',
                        default=False, action='store_true')
    parser.add_argument('--enable-ptsc-3-test', help='Add proxy to all-of-us-rdr-ptsc-3-test',
                        default=False, action='store_true')
    parser.add_argument('--enable-test-mysql-8', help='Add proxy to test MySQL 8 instance',
                        default=False, action='store_true')
    parser.add_argument('--enable-test-postgres', help='Add proxy to test PostgreSQL instance',
                        default=False, action='store_true')

    parser.add_argument("action", choices=("start", "stop", "restart"), default="")

    args = parser.parse_args()

    if args.root_only is True and os.getuid() != 0:
        _logger.warning("daemon must be run as root")
        sys.exit(4)

    if is_valid_email(args.account) is False:
        if "RDR_ACCOUNT" not in os.environ:
            _logger.error("account parameter is invalid and RDR_ACCOUNT shell var is not set.")
            return 1
        else:
            args.account = os.environ["RDR_ACCOUNT"]

    if which("cloud_sql_proxy") is None:
        _logger.error(
            "cloud_sql_proxy executable not found, " + "create symlink to cloud_sql_proxy in /usr/local/bin/ directory"
        )

    # --nodaemon only valid with start action
    if args.nodaemon and args.action != "start":
        print(("{0}: error: --nodaemon option not valid with stop or restart action".format(progname)))
        sys.exit(1)

    _logger.info("account:          {0}".format(args.account))

    # Do not fork the daemon process for systemd service or debugging, run in foreground.
    if args.nodaemon is True:
        if args.action == "start":
            _daemon = SQLProxyDaemon(args=args)
            _daemon.print_instances()
            _logger.info("running daemon in foreground.")
            _daemon.run()
    else:

        pidpath = os.path.expanduser("~/.local/run")
        pidfile = os.path.join(pidpath, "{0}.pid".format(progname))

        logpath = os.path.expanduser("~/.local/log")
        logfile = os.path.join(logpath, "{0}.log".format(progname))

        # Setup daemon object
        # make sure PID and Logging path exist
        if not os.path.exists(pidpath):
            os.makedirs(pidpath)
        if not os.path.exists(logpath):
            os.makedirs(logpath)

        _daemon = SQLProxyDaemon(
            procbase="",
            dirmask="0o700",
            pidfile=pidfile,
            uid="root",
            gid="root",
            stdin="/dev/null",
            stdout=logfile,
            stderr=logfile,
            args=args,
        )

        if args.action == "start":
            _daemon.print_instances()
            _logger.info("running daemon in background.")
            _daemon.start()
        elif args.action == "stop":
            _logger.info("Stopping daemon.")
            _daemon.stop()
        elif args.action == "restart":
            _logger.info("Restarting daemon.")
            _daemon.restart()

    return 0


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
