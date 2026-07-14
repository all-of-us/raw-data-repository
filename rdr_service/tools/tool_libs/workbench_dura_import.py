from rdr_service.researchers_offline.import_workbench_dura_data import WorkbenchDuraImporter
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = "dura-test"
tool_desc = "Tool to run the Workbench Institutional DURA Import"


class WorkbenchInstitutionalDuraTool(ToolBase):

    def run(self):
        super(WorkbenchInstitutionalDuraTool, self).run()

        importer = WorkbenchDuraImporter()
        importer.import_reports(self.args.since_date)


def add_additional_arguments(arg_parser):
    arg_parser.add_argument('--since_date', help='Request all records sync the given date/time', default=None)


def run():
    cli_run(tool_cmd, tool_desc, WorkbenchInstitutionalDuraTool, add_additional_arguments)
