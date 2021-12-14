
from rdr_service.services.ghost_check_service import GhostCheckService
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase, logger

tool_cmd = 'ghost-api-check'
tool_desc = 'Check for ghost participants against PTSC API'


class GhostApiCheck(ToolBase):
    def run(self):
        super(GhostApiCheck, self).run()
        server_config = self.get_server_config()

        with self.get_session() as session:
            service = GhostCheckService(
                session=session,
                logger=logger,
                ptsc_config=server_config['ptsc_api_config']
            )
            service.run_ghost_check()


def run():
    return cli_run(tool_cmd, tool_desc, GhostApiCheck)
