#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import os
import sys

from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.participant import Participant

from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "fix-dup-pids"
tool_desc = "Fix duplicated participants"


class ProgramTemplateClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

    def get_participant_records(self, dao, pids):
        """
        Return the participant records for each pid in pids.
        :param dao: DAO object to run queries with.
        :param pids: Tuple with 3 pid values.
        :return: Original Participant record, New Participant 1 record, New Participant 2 record.
        """
        with dao.session() as session:
            p = session.query(Participant).filter(Participant.participantId == pids[0]).first()
            p1 = session.query(Participant).filter(Participant.participantId == pids[1]).first()
            p2 = session.query(Participant).filter(Participant.participantId == pids[2]).first()

        return p, p1, p2

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        """
        Example: Enabling colors in terminals.
            Using colors in terminal output is supported by using the self.gcp_env.terminal_colors
            object.  Errors and Warnings are automatically set to Red and Yellow respectively.
            The terminal_colors object has many predefined colors, but custom colors may be used
            as well. See rdr_service/services/TerminalColors for more information.
        """
        # clr = self.gcp_env.terminal_colors
        # _logger.info(clr.fmt('This is a blue info line.', clr.fg_bright_blue))
        # _logger.info(clr.fmt('This is a custom color line', clr.custom_fg_color(156)))

        self.gcp_env.activate_sql_proxy()
        dao = ParticipantDao()

        pids_list = list()

        if self.args.participant:
            pids_list.append(tuple(i.strip() for i in self.args.participant.split(',')))
        else:
            with open(self.args.csv) as h:
                lines = h.readlines()
                for line in lines:
                    pids_list.append(tuple(i.strip() for i in line.split(',')))

        for pids in pids_list:
            p, p1, p2 = self.get_participant_records(dao, pids)

            if not p or not p1 or not p2:
                _logger.error(f'  error {p.participantId}, {p1.participantId}, {p2.participantId}')
                continue

            _logger.warning(f'  splitting {p.participantId} -> {p1.participantId} | {p2.participantId}')

            # TODO: Call methods for each step in process here


        return 0


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
    parser.add_argument("--csv", help="csv file with participant ids", default=None)  # noqa
    parser.add_argument("--participant", help="old pid,new pid 1,new pid 2", default=None)  # noqa
    args = parser.parse_args()

    if not args.participant and not args.csv:
        _logger.error('Either --csv or --participant argument must be provided.')
        return 1

    if args.participant and args.csv:
        _logger.error('Arguments --csv and --participant may not be used together.')
        return 1

    if args.participant:
        # Verify that we have a string with 3 comma delimited values.
        if len(args.participant.split(',')) != 3:
            _logger.error('Invalid participant argument, must be 3 PIDs in comma delimited format.')
            return 1

    if args.csv:
        if not os.path.exists(args.csv):
            _logger.error(f'File {args.csv} was not found.')
            return 1

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ProgramTemplateClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
