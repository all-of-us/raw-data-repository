from dateutil import parser

from rdr_service.offline.import_hpo_lite_pairing import HpoLitePairingImporter
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'hpo-lite-pairing-sync'
tool_desc = 'Sync hpo lite pairing data from Redcap to an environment'


class HpoLitePairingSyncTool(ToolBase):
    def run(self):
        super(HpoLitePairingSyncTool, self).run()

        parsed_since_date = None
        if self.args.since_date:
            parsed_since_date = parser.parse(self.args.since_date)

        importer = HpoLitePairingImporter()
        importer.import_pairing_data(parsed_since_date)


def add_additional_arguments(arg_parser):
    arg_parser.add_argument('--since-date', help='Request all records sync the given date/time', default=None)


def run():
    return cli_run(tool_cmd, tool_desc, HpoLitePairingSyncTool, add_additional_arguments)
