from datetime import datetime
from typing import Optional

from dateutil.parser import parse

from rdr_service.dao.duplicate_account_dao import DuplicateAccountDao
from rdr_service.model.duplicate_account import DuplicationSource, DuplicationStatus, PrimaryParticipantIndication
from rdr_service.model.utils import from_client_participant_id
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'duplicate-accounts'
tool_desc = 'Script for managing duplicate account data stored in the RDR database'


class DuplicateAccountScript(ToolBase):
    """
    Defines a command-line script for adding and updating duplicate account information.
    RDR team members should use this script to update the database based on duplicate account information
    (newly discover duplicates, or updates to duplicates currently recorded).

    The following example shows how to run the script, followed up by descriptions of each of the arguments.

    duplicate-accounts --project <rdr-project> --pids P123123123,P1234567890 --first-is-primary
                        --status APPROVED --timestamp "2023-02-02 20:48"

    "--pids P123123123,P1234567890" is used to identify the two participant ids that duplicate each other.

    "--first-is-primary" should be added to specify that the first participant listed (in the "--pids" argument) should
        be marked as the primary account of the duplication. This is an optional argument and will default to not
        identifying either participants as primary.

    "--status APPROVED" will save the duplication with an APPROVED status. Other options for this status are REJECTED
        and POTENTIAL. This needs to be set when creating a new record, but is optional for updating an existing one
        (only needed if you want to change the status to something else).

    "--timestamp 2023-02-02 20:48" will set the authored timestamp of the duplication when a specific time is
        needed/provided. This is an optional argument and will default to the current timestamp.

    Adding the "--update" flag will set all values provided on an existing duplication record (rather than inserting
        a new one.

    Use the "--clear-primary" flag when updating to remove any indication of a primary account for the record.
    """

    def run(self):
        super().run()

        first_participant_id, second_participant_id = self._parse_participant_ids(self.args.pids.split(','))

        with self.get_session() as session:
            if self.args.update:
                self._run_update(session, first_participant_id, second_participant_id)
            else:
                self._run_insert(session, first_participant_id, second_participant_id)

    def _run_insert(self, session, first_participant_id, second_participant_id):
        duplication_status = self._get_status()
        if not duplication_status:
            raise Exception("'status' field required for inserting new record")

        authored_timestamp = self._get_authored_value() or datetime.utcnow().isoformat()

        DuplicateAccountDao.store_duplication(
            participant_a_id=first_participant_id,
            participant_b_id=second_participant_id,
            authored=authored_timestamp,
            source=DuplicationSource.SUPPORT_TICKET,
            status=duplication_status,
            primary_account=PrimaryParticipantIndication.PARTICIPANT_A if self.args.first_is_primary else None,
            session=session
        )

    def _run_update(self, session, first_participant_id, second_participant_id):
        kwargs = {}

        authored_timestamp = self._get_authored_value()
        if authored_timestamp:
            kwargs['authored'] = authored_timestamp

        duplication_status = self._get_status()
        if duplication_status:
            kwargs['status'] = duplication_status

        if self.args.clear_primary:
            kwargs['primary_account'] = None
        elif self.args.first_is_primary:
            kwargs['primary_account'] = PrimaryParticipantIndication.PARTICIPANT_A

        DuplicateAccountDao.update_duplication(
            participant_a_id=first_participant_id,
            participant_b_id=second_participant_id,
            session=session,
            **kwargs
        )

    def _get_status(self) -> Optional[DuplicationStatus]:
        if not self.args.status:
            return None
        return DuplicationStatus[self.args.status]

    def _get_authored_value(self) -> Optional[datetime]:
        if not self.args.timestamp:
            return None
        return parse(self.args.timestamp)

    @classmethod
    def _parse_participant_ids(cls, pids_arg_str):
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
        '--clear-primary', help='Set the first id as the primary account', default=False, action='store_true'
    )
    arg_parser.add_argument(
        '--status', help='Set the status of the duplication (eg POTENTIAL, APPROVED, REJECTED)'
    )
    arg_parser.add_argument(
        '--timestamp', help='Set the authored time for the duplication record'
    )
    arg_parser.add_argument(
        '--update', help='Update an existing record instead.', default=False, action='store_true'
    )


def run():
    return cli_run(tool_cmd, tool_desc, DuplicateAccountScript, add_additional_arguments)
