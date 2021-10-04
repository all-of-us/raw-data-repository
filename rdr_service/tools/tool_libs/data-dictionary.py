import argparse

from rdr_service.config import DATA_DICTIONARY_DOCUMENT_ID
from rdr_service.services.data_dictionary_updater import DataDictionaryUpdater
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase


tool_cmd = 'data-dictionary'
tool_desc = "Supplemental tool for managing RDR's data-dictionary (for when the deploy fails to update it)."


class DataDictionaryScript(ToolBase):
    def run_process(self):
        with self.initialize_process_context() as gcp_env:
            self.gcp_env = gcp_env
            server_config = self.get_server_config()

        updater = DataDictionaryUpdater(server_config[DATA_DICTIONARY_DOCUMENT_ID], self.args.rdr_version)
        updater.run_update_in_tool(self, logger)


def add_additional_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        '--rdr-version',
        help='Version number of the RDR release to label the changes with in the data dictionary.'
    )


def run():
    cli_run(tool_cmd, tool_desc, DataDictionaryScript, add_additional_arguments)
