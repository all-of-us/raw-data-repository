
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.system_utils import list_chunks
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'backfill-enrollment'
tool_desc = 'Backfill enrollement status fields for version 3 of data glossary'


class BackfillEnrollment(ToolBase):
    def run(self):
        with self.get_session() as session:
            summary_dao = ParticipantSummaryDao()
            summary_list = session.query(ParticipantSummary).all()

            chunk_size = 50
            for summary_list_subset in list_chunks(lst=summary_list, chunk_size=chunk_size):
                for summary in summary_list_subset:
                    summary_dao.update_enrollment_status(
                        summary=summary,
                        session=session
                    )
                session.commit()


def run():
    return cli_run(tool_cmd, tool_desc, BackfillEnrollment)
