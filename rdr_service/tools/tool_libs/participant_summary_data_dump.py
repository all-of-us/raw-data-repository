import logging
from datetime import datetime

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.system_utils import list_chunks

from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'ps-data-dump'
tool_desc = 'bulk data dump of participants from the PS API for vibrent'


class ParticipantSummaryDataDump(ToolBase):
    logger_name = None

    def run(self):
        super(ParticipantSummaryDataDump, self).run()
        with self.get_session() as session:
            summary_dao = ParticipantSummaryDao()
            # --id option takes precedence over --from-file option
            if self.args.id:
                participant_id_list = [int(i) for i in self.args.id.split(',')]
            elif self.args.from_file:
                participant_id_list = self.get_int_ids_from_file(self.args.from_file)
            else:
                # Default to all participant_summary ids
                participant_id_list = session.query(
                    ParticipantSummary.participantId
                ).order_by(ParticipantSummary.participantId).all()

            count = 0
            last_id = None

            chunk_size = 50
            for id_list_subset in list_chunks(lst=participant_id_list, chunk_size=chunk_size):
                logging.info(f'{datetime.now()}: {count} of {len(participant_id_list)} (last id: {last_id})')
                count += chunk_size

                summary_list = session.query(
                    ParticipantSummary
                ).filter(
                    ParticipantSummary.participantId.in_(id_list_subset),
                    ParticipantSummary.participantOrigin == 'vibrent'
                ).all()

                for summary in summary_list:
                    summary_dao.to_client_json(summary)
                    print( 'debug')


def add_additional_arguments(parser):
    parser.add_argument('--id', required=False,
                        help="Single participant id or comma-separated list of id integer values to backfill")
    parser.add_argument('--from-file', required=False,
                        help="file of integer participant id values to backfill")
    parser.add_argument('--allow-downgrade', default=False, action="store_true",
                        help='Force recalculation of enrollment status, and allow status to revert to a lower status')


def run():
    return cli_run(tool_cmd, tool_desc, ParticipantSummaryDataDump, add_additional_arguments, replica=True)
