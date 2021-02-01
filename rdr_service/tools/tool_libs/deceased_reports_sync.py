from dateutil import parser

from rdr_service.offline.import_deceased_reports import DeceasedReportImporter
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'deceased-sync'
tool_desc = 'Sync deceased reports from Redcap to an environment'


class DeceasedSyncTool(ToolBase):
    def run(self):
        super(DeceasedSyncTool, self).run()

        parsed_since_date = None
        if self.args.since_date:
            parsed_since_date = parser.parse(self.args.since_date)

        server_config = self.get_server_config()
        importer = DeceasedReportImporter(server_config)
        importer.import_reports(parsed_since_date)


def add_additional_arguments(arg_parser):
    arg_parser.add_argument('--since-date', help='Request all records sync the given date/time', default=None)


def run():
    return cli_run(tool_cmd, tool_desc, DeceasedSyncTool, add_additional_arguments)
