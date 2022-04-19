import argparse
from dateutil.parser import parse

from rdr_service.services.ghost_check_service import GhostCheckService
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase


tool_cmd = 'ghost-check'
tool_desc = 'Check for ghost participants'


class GhostCheckScript(ToolBase):
    def run(self):
        super(GhostCheckScript, self).run()
        server_config = self.get_server_config()['ptsc_api_config']

        start_date = parse(self.args.start_date).date()
        end_date = None
        if self.args.end_date:
            end_date = parse(self.args.end_date).date()

        with self.get_session() as session:
            service = GhostCheckService(
                session=session,
                logger=logger,
                ptsc_config=server_config
            )
            service.run_ghost_check(start_date=start_date, end_date=end_date,
                                    project=self.gcp_env.project)


def add_additional_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        '--start-date',
        required=True,
        help='Only checks participants that have signed up after the start date'
    )
    parser.add_argument(
        '--end-date',
        required=False,
        help='Only checks participants that have signed up before the end date'
    )


def run():
    cli_run(tool_cmd, tool_desc, GhostCheckScript, add_additional_arguments)
