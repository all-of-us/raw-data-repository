import logging
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.offline.retention_eligible_import import _supplement_with_rdr_calculations
from rdr_service.participant_enums import RetentionStatus
from rdr_service.services.system_utils import list_chunks

from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'retention-qc'
tool_desc = 'QC tool comparing PTSC calculated retention_eligible_metrics data to the RDR supplemental calculations'

class RetentionQC(ToolBase):
    logger_name = None

    def run(self):
        super(RetentionQC, self).run()
        with self.get_session() as session:
            participant_id_list = None
            query = session.query(ParticipantSummary)
            # --id option takes precedence over --from-file option
            if self.args.id:
                participant_id_list = [int(i) for i in self.args.id.split(',')]
            elif self.args.from_file:
                participant_id_list = self.get_int_ids_from_file(self.args.from_file)

            if participant_id_list:
                query = query.filter(ParticipantSummary.participantId.in_(participant_id_list))

            participants = query.order_by(ParticipantSummary.participantId).all()

            if not participants:
                logging.error('No participant summary results to process')
                return 1

            count = 0
            chunk_size = 500
            for summaries in list_chunks(lst=participants, chunk_size=chunk_size):
                for ps in summaries:
                    row = session.query(RetentionEligibleMetrics).filter(
                        RetentionEligibleMetrics.participantId == ps.participantId
                    ).first()
                    if row.retentionEligible:
                        obj = RetentionEligibleMetrics(
                            participantId=row.participantId,
                            retentionEligible=row.retentionEligible,
                            retentionEligibleTime=row.retentionEligibleTime,
                            lastActiveRetentionActivityTime=row.lastActiveRetentionActivityTime,
                            activelyRetained=row.activelyRetained,
                            passivelyRetained=row.passivelyRetained,
                            fileUploadDate=row.fileUploadDate,
                            retentionEligibleStatus=RetentionStatus.ELIGIBLE\
                                if row.retentionEligible else RetentionStatus.NOT_ELIGIBLE,
                            retentionType=row.retentionType
                        )
                        _supplement_with_rdr_calculations(obj, session)
                        if obj.rdr_retention_eligible != obj.retentionEligible:
                            logging.error(f'P{ps.participantId}: undiagnosed eligibility mismatch')
                        elif obj.activelyRetained != obj.rdr_is_actively_retained:
                            logging.error(f'P{ps.participantId}: undiagnosed actively retained mismatch')
                count += 1
                logging.info(f'Processed {min(count * chunk_size, len(participants))} of {len(participants)} pids...')


def add_additional_arguments(parser):
    parser.add_argument('--id', required=False,
                        help="Single participant id or comma-separated list of id integer values to check")
    parser.add_argument('--from-file', required=False,
                        help="file of integer participant id values to check")
def run():
    return cli_run(tool_cmd, tool_desc, RetentionQC, add_additional_arguments)
