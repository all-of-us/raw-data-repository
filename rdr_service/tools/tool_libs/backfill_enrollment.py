
import logging
from datetime import datetime

from rdr_service.api_util import dispatch_task
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'backfill-enrollment'
tool_desc = 'Backfill enrollment status fields for version 3 of data glossary'


class BackfillEnrollment(ToolBase):
    logger_name = None

    def run(self):
        super(BackfillEnrollment, self).run()

        with self.get_session() as session:
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

            for participant_id in participant_id_list:
                if count % 50 == 0:
                    logging.info(f'{datetime.now()}: {count} of {len(participant_id_list)} (last id: {last_id})')
                count += 1

                dispatch_task(
                    endpoint='update_enrollment_status',
                    payload={
                        'participant_id': participant_id,
                        'allow_downgrade': self.args.allow_downgrade
                    },
                    project_id=self.gcp_env.project
                )
                last_id = participant_id

                session.commit()


def add_additional_arguments(parser):
    parser.add_argument('--id', required=False,
                        help="Single participant id or comma-separated list of id integer values to backfill")
    parser.add_argument('--from-file', required=False,
                        help="file of integer participant id values to backfill")
    parser.add_argument('--allow-downgrade', default=False, action="store_true",
                        help='Force recalculation of enrollment status, and allow status to revert to a lower status')
def run():
    return cli_run(tool_cmd, tool_desc, BackfillEnrollment, add_additional_arguments)
