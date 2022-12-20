#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse
from datetime import datetime

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import os
import sys

from rdr_service.services.system_utils import setup_logging, setup_i18n, which, run_external_program
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "alembic"
tool_desc = "alembic migration manager"


class AlembicManagerClass(object):
    """
    A thin wrapper around the Alembic executable.
    """
    alembic_args = None

    def __init__(self, args, gcp_env: GCPEnvConfigObject, alembic_args: list):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.alembic_args = alembic_args
        self.output = ''

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        if not self.gcp_env.activate_sql_proxy(user='alembic', project=self.gcp_env.project):
            return 1

        clr = self.gcp_env.terminal_colors
        _logger.info('\nAlembic Process Information:')
        _logger.info('=' * 90)
        _logger.info('  Target Project : {0}'.format(
                        clr.fmt(self.gcp_env.project)))
        _logger.info('  Alembic Command : {0}\n'.format(
                        clr.fmt(' '.join(self.alembic_args))))

        if not self.args.quiet:
            confirm = input('Run alembic command (Y/n)? : ')
            if confirm and confirm.lower().strip() != 'y':
                _logger.warning('Aborting.')
                return 1

        args = list()
        args.append(which('alembic'))
        args.extend(self.alembic_args)

        env = dict(os.environ)

        code, so, se = run_external_program(args, env=env)

        sys.stdout.write(so)
        sys.stdout.flush()
        if se:
            _logger.error(se)

        self.output = se + f'Migrations complete at {datetime.now()}'
        _logger.info(f'Alembic command finished at {datetime.now()}')
        return code


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="Enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--quiet", help="do not ask for user input", default=False, action="store_true")  # noqa
    parser.add_argument('args', metavar="alembic command", help="alembic command and args", nargs=argparse.REMAINDER)

    parser.epilog = "  Alembic commands: {-h, branches,current,downgrade,edit,heads,history,init,list_templates," + \
                    "merge,revision,show,stamp,upgrade}"

    args = parser.parse_args()
    alembic_args = args.args

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = AlembicManagerClass(args, gcp_env, alembic_args)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
