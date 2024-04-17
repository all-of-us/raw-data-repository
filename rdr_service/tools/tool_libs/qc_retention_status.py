import logging
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from sqlalchemy import or_

tool_cmd = "qc-retention-status"
tool_desc = "tool to update the participant summary table & remediate retention status mismatches from PTSC"

logger = logging.getLogger("rdr_logger")


class QCRetentionStatus(ToolBase):
    """
    NOTE: KEEPING HERE FOR NOW, TO REMOVE ONCE THIS IS WORKING IN RETENTION_METRICS.PY
    This tool will make the update the retention metrics data in the participant summary table to match the data
    that is sent to us by PTSC.

    We require approvals to run the following query to resolve

    update participant_summary ps
    join retention_eligible_metrics rem
    on ps.participant_id = rem.participant_id
    set ps.retention_eligible_status = rem.retention_eligible_status,
    ps.retention_eligible_time = rem.retention_eligible_time,
    ps.retention_type = rem.retention_type,
    ps.last_active_retention_activity_time = rem.last_active_retention_activity_time,
    ps.last_modified = now()
    where ps.retention_eligible_status != rem.retention_eligible_status
      or ps.retention_eligible_time != rem.retention_eligible_time
      or ps.retention_type != rem.retention_type
      or ps.last_active_retention_activity_time != rem.last_active_retention_activity_time;


    This tool will allow us to run the query without waiting for approvals every time.
    """

    def run(self):
        super().run()
        pid_list = []
        with self.get_session() as session:
            for participant, retention_metric in (
                session.query(ParticipantSummary, RetentionEligibleMetrics)
                .filter(
                    ParticipantSummary.participantId
                    == RetentionEligibleMetrics.participantId,
                    or_(
                        ParticipantSummary.retentionEligibleStatus
                        != RetentionEligibleMetrics.retentionEligibleStatus,
                        ParticipantSummary.retentionEligibleTime
                        != RetentionEligibleMetrics.retentionEligibleTime,
                        ParticipantSummary.retentionType
                        != RetentionEligibleMetrics.retentionType,
                        ParticipantSummary.lastActiveRetentionActivityTime
                        != RetentionEligibleMetrics.lastActiveRetentionActivityTime,
                    ),
                )
                .order_by(ParticipantSummary.participantId)
                .all()
            ):
                pid_list.append(f"P{participant.participantId},\n")
                logger.info(
                    f"remediating QC retention status for P{participant.participantId}"
                )
                participant.retentionEligibleStatus = (
                    retention_metric.retentionEligibleStatus
                )
                participant.retentionEligibleTime = (
                    retention_metric.retentionEligibleTime
                )
                participant.retentionType = retention_metric.retentionType
                participant.lastActiveRetentionActivityTime = (
                    retention_metric.lastActiveRetentionActivityTime
                )
                session.add(participant)

        logger.info(f"REMEDIATED {len(pid_list)} MISMATCHES: \n{''.join(pid_list)}")


def run():
    return cli_run(tool_cmd, tool_desc, QCRetentionStatus)
