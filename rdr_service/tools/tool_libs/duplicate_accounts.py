from datetime import datetime

from dateutil.parser import parse

from rdr_service.dao.duplicate_account_dao import DuplicateAccountDao
from rdr_service.model.duplicate_account import DuplicationSource, DuplicationStatus
from rdr_service.model.utils import from_client_participant_id
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'duplicate-accounts'
tool_desc = 'Script for managing duplicate account data stored in the RDR database'


class DuplicateAccountScript(ToolBase):
    """
    Defines a command-line script for adding and updating duplicate account information.
    RDR team members should use this script to update the database based on duplicate account information
    (newly discover duplicates, or updates to duplicates currently recorded).

    The following sections describe different ways of running this script, and what you might use those for.

    duplicate-accounts --project <rdr-project> add --pids P123123123,P1234567890 --first-is-primary
                        --status APPROVED --timestamp "2023-02-02 20:48"
    """

    def run(self):
        super().run()

        first_participant_id, second_participant_id = self._parse_participant_ids(self.args.pids.split(','))
        # primary_indication = PrimaryParticipantIndication.PARTICIPANT_A if self.args.first_is_primary else None
        duplication_status = DuplicationStatus[self.args.status]
        timestamp = parse(self.args.timestamp)

        with self.get_session() as session:
            DuplicateAccountDao.store_duplication(
                participant_a_id=first_participant_id,
                participant_b_id=second_participant_id,
                authored=timestamp,
                source=DuplicationSource.SUPPORT_TICKET,
                status=duplication_status,
                session=session
            )

    def _parse_participant_ids(self, pids_arg_str):
        return (
            from_client_participant_id(participant_id_str.strip())
            for participant_id_str in pids_arg_str
        )


def add_additional_arguments(arg_parser):
    # arg_parser.add_argument('--file', help='Path to file to import', required=True)
    arg_parser.add_argument('--pids', help='Participant ids that duplicate each other', required=True)
    arg_parser.add_argument(
        '--first-is-primary', help='Set the first id as the primary account', default=False, action='store_true'
    )
    arg_parser.add_argument(
        '--status', help='Set the status of the duplication (eg POTENTIAL, APPROVED, REJECTED)', required=True
    )
    arg_parser.add_argument(
        '--timestamp', help='Set the authored time for the duplication record', default=datetime.now().isoformat()
    )


def run():
    return cli_run(tool_cmd, tool_desc, DuplicateAccountScript, add_additional_arguments)
