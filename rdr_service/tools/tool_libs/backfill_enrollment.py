
import logging
from datetime import datetime

import rdr_service.config as config

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.cloud_utils.gcp_google_pubsub import submit_pipeline_pubsub_msg

from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'backfill-enrollment'
tool_desc = 'Backfill enrollment status fields for version 3 of data glossary'


class BackfillEnrollment(ToolBase):
    logger_name = None

    def run(self):
        super(BackfillEnrollment, self).run()
        config.override_setting('pdr_pipeline', { 'allowed_projects': [self.gcp_env.project]})
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

            for participant_id in participant_id_list:
                if count % 50 == 0:
                    logging.info(f'{datetime.now()}: {count} of {len(participant_id_list)} (last id: {last_id})')
                count += 1

                summary = ParticipantSummaryDao.get_for_update_with_linked_data(
                    participant_id=participant_id,
                    session=session
                )
                summary_dao.update_enrollment_status(
                    summary=summary,
                    session=session,
                    allow_downgrade=self.args.allow_downgrade,
                    # Don't trigger pubsub for PDR pipeline until after the commit
                    pdr_pubsub=False
                )
                last_id = summary.participantId

                session.commit()

                submit_pipeline_pubsub_msg(
                    database='rdr', table='participant_summary', action='upsert',
                    pk_columns=['participant_id'], pk_values=[participant_id], project=self.gcp_env.project
                )


def add_additional_arguments(parser):
    parser.add_argument('--id', required=False,
                        help="Single participant id or comma-separated list of id integer values to backfill")
    parser.add_argument('--from-file', required=False,
                        help="file of integer participant id values to backfill")
    parser.add_argument('--allow-downgrade', default=False, action="store_true",
                        help='Force recalculation of enrollment status, and allow status to revert to a lower status')
def run():
    return cli_run(tool_cmd, tool_desc, BackfillEnrollment, add_additional_arguments)
