#! /bin/env python
#
# Remove an approved deceased report from a given participant
#

import argparse
import logging
import sys

from rdr_service.dao.api_user_dao import ApiUserDao
from rdr_service.model.api_user import ApiUser
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import DeceasedReportDenialReason, DeceasedReportStatus, DeceasedStatus
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "resurrect"
tool_desc = "remove an approved deceased report from a given participant"

PMI_OPS_URL = "https://www.pmi-ops.org/"


class ResurrectClass(object):
    def __init__(self, args, gcp_env):
        self.args = args
        self.gcp_env = gcp_env
        self.dao = ApiUserDao()

    def update_approved_deceased_report(self, session, participant_id):
        report = session.query(DeceasedReport).filter(
            DeceasedReport.participantId == participant_id,
            DeceasedReport.status == DeceasedReportStatus.APPROVED
        ).one_or_none()
        if report is None:
            raise Exception('no approved deceased report found')

        report.status = DeceasedReportStatus.DENIED
        report.reviewer = self.dao.load_from_database(PMI_OPS_URL, self.gcp_env.account)
        if report.reviewer is None:
            report.reviewer = ApiUser(
                system=PMI_OPS_URL,
                username=self.gcp_env.account
            )

        report.denialReason = DeceasedReportDenialReason(self.args.reason)
        if report.denialReason == DeceasedReportDenialReason.OTHER:
            if self.args.reason_desc is None:
                raise Exception('reason description required when denial reason is OTHER')
            report.denialReasonOther = self.args.reason_desc

    @staticmethod
    def update_participant_summary(session, participant_id):
        participant_summary = session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == participant_id
        ).one_or_none()

        if participant_summary is None:
            _logger.warning('WARNING: participant summary not found')
        else:
            participant_summary.deceasedStatus = DeceasedStatus.UNSET
            participant_summary.deceasedAuthored = None
            participant_summary.dateOfDeath = None

    def run(self):
        proxy_pid = self.gcp_env.activate_sql_proxy()
        if not proxy_pid:
            _logger.error("activating google sql proxy failed.")
            return 1

        participant_id = self.args.pid
        with self.dao.session() as session:
            self.update_approved_deceased_report(session, participant_id)
            self.update_participant_summary(session, participant_id)

        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--pid", required=True, help="Id of participant to remove deceased status from")
    parser.add_argument(
        "--reason",
        help="Valid DeceasedReportDenialReason value for setting on the report",
        default='MARKED_IN_ERROR'
    )
    parser.add_argument(
        "--reason-desc",
        help="Text description for setting the deceased report to denied. Required if the reason is set as OTHER"
    )

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ResurrectClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
