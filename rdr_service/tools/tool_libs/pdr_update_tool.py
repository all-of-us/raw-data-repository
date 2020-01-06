#! /bin/env python
#
# PDR data tools.
#

import argparse
import os

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys

from werkzeug.exceptions import NotFound

from rdr_service.services.system_utils import setup_logging, setup_i18n, print_progress_bar
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.dao.bq_participant_summary_dao import BQParticipantSummaryGenerator, rebuild_bq_participant
from rdr_service.dao.bq_pdr_participant_summary_dao import BQPDRParticipantSummaryGenerator

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "pdr-tool"
tool_desc = "Tools for updating RDR data in PDR"


class PDRParticipantRebuildClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env


    def update_single_pid(self, pid, ps_bqgen=None, pdr_bqgen=None):
        """
        Update a single pid
        :param pid: participant id
        :return: 0 if successful otherwise 1
        """
        try:
            rebuild_bq_participant(pid, ps_bqgen=ps_bqgen, pdr_bqgen=pdr_bqgen, project_id=self.gcp_env.project)
        except NotFound:
            return 1
        return 0

    def update_many_pids(self, pids):
        """
        Update many pids from a file.
        :return:
        """
        if not pids:
            return 1

        ps_bqgen = BQParticipantSummaryGenerator()
        pdr_bqgen = BQPDRParticipantSummaryGenerator()

        total_pids = len(pids)
        count = 0
        errors = 0

        for pid in pids:
            count += 1

            if self.update_single_pid(pid, ps_bqgen=ps_bqgen, pdr_bqgen=pdr_bqgen) != 0:
                errors += 1
                if self.args.debug:
                    _logger.error(f'PID {pid} not found.')

            if not self.args.debug:
                print_progress_bar(
                    count, total_pids, prefix="{0}/{1}:".format(count, total_pids), suffix="complete"
                )

        if errors > 0:
            _logger.warning(f'\n\nThere were {errors} PIDs not found during processing.')

        return 0


    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        clr = self.gcp_env.terminal_colors
        pids = None

        if not self.args.from_file and not self.args.pid:
            _logger.error('Nothing to do')
            return 1

        _logger.info(clr.fmt('\nRebuild Participant Summaries for PDR:', clr.custom_fg_color(156)))
        _logger.info('')
        _logger.info('=' * 90)
        _logger.info('  Target Project        : {0}'.format(clr.fmt(self.gcp_env.project)))

        if self.args.from_file:
            filename = os.path.expanduser(self.args.from_file)
            if not os.path.exists(filename):
                _logger.error(f"File '{self.args.from_file}' not found.")
                return 1

            # read pids from file.
            pids = open(os.path.expanduser('~/rebuild_pids.txt')).readlines()
            # convert pids from a list of strings to a list of integers.
            pids = [int(i) for i in pids]
            _logger.info('  PIDs File             : {0}'.format(clr.fmt(self.args.from_file)))
            _logger.info('  Total PIDs            : {0}'.format(clr.fmt(len(pids))))
        else:
            _logger.info('  PID                   : {0}'.format(clr.fmt(self.args.pid)))

        _logger.info('=' * 90)
        _logger.info('')

        self.gcp_env.activate_sql_proxy()
        _logger.info('')

        if self.args.from_file:
            return self.update_many_pids(pids)

        if self.args.pid:
            if self.update_single_pid(self.args.pid) == 0:
                _logger.info(f'Participant {self.args.pid} updated.')
            else:
                _logger.error(f'Participant ID {self.args.pid} not found.')

        return 1


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa

    subparser = parser.add_subparsers(help='pdr tools')

    # Rebuild PDR participants
    rebuild_parser = subparser.add_parser("rebuild-pids")
    rebuild_parser.add_argument("--pid", help="rebuild single participant id", type=int, default=None)  # noqa
    rebuild_parser.add_argument("--from-file", help="rebuild participant ids from a file with a list of pids", default=None)  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:

        if hasattr(args, 'pid') and hasattr(args, 'from_file'):
            process = PDRParticipantRebuildClass(args, gcp_env)
            exit_code = process.run()
        else:
            _logger.info('Please select an option to run. For help use "pdr-tool --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
