
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.storage import GoogleCloudStorageProvider

from rdr_service.services.consent.validation import ConsentValidationController

tool_cmd = 'deceased-sync'
tool_desc = 'Sync deceased reports from Redcap to an environment'


class DeceasedSyncTool(ToolBase):
    def run(self):
        super(DeceasedSyncTool, self).run()

        controller = ConsentValidationController(
            consent_dao=ConsentDao(),
            participant_summary_dao=ParticipantSummaryDao(),
            hpo_dao=HPODao(),
            storage_provider=GoogleCloudStorageProvider()
        )
        controller.validate_recent_uploads()


def run():
    return cli_run(tool_cmd, tool_desc, DeceasedSyncTool)
