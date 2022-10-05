import logging
from datetime import datetime

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.system_utils import list_chunks
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'backfill-enrollment'
tool_desc = 'Backfill enrollement status fields for version 3 of data glossary'


class BackfillEnrollment(ToolBase):
    logger_name = None

    def run(self):
        super(BackfillEnrollment, self).run()

        with self.get_session() as session:
            summary_dao = ParticipantSummaryDao()
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
                    ParticipantSummary.participantId.in_(id_list_subset)
                ).with_for_update().all()

                for summary in summary_list:
                    summary_dao.update_enrollment_status(
                        summary=summary,
                        session=session
                    )
                    last_id = summary.participantId

                session.commit()


def run():
    return cli_run(tool_cmd, tool_desc, BackfillEnrollment)
