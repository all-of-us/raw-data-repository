#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import logging
import sys

from csv import DictReader
from typing import List

from rdr_service.exposomics.exposomics_generate import ExposomicsGenerate
from rdr_service.services.system_utils import setup_logging, setup_i18n

from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.tools.tool_libs.tool_base import ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "exposomics-generate"
tool_desc = "Tool for running initial exposomics generation leading to output of M0 manifest"


class ExposomicsGenrateTool(ToolBase):

    @classmethod
    def parse_csvs(cls, csv_path):
        with open(csv_path, 'r') as f:
            dict_reader = DictReader(f)
            data_list = list(dict_reader)
            cleaned_rows = []
            for row in data_list:
                cleaned_rows.append({k.lower().replace('\ufeff', ''): v for k, v in row.copy().items()})
        return cleaned_rows

    def run(self):
        server_config = self.get_server_config()
        self.gcp_env.activate_sql_proxy()

        logging.info('Starting Exposomics generation workflow')

        sample_list: List[dict] = self.parse_csvs(csv_path=self.args.sample_list)
        form_data: List[dict] = self.parse_csvs(csv_path=self.args.form_data)

        if not sample_list or not form_data:
            logging.warning('Necessary file data not found')
            return

        if len(form_data) > 1:
            logging.warning('Multiple rows are not accepted for form data')
            return

        logging.info(f'Running workflow for {len(sample_list)} participants')

        ExposomicsGenerate.create_exposomics_generate_workflow(
            sample_list=sample_list,
            form_data=form_data[0],
            server_config=server_config
        ).run_generation()


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--sample-list", help="csv containing all affected biobank_ids/sample_ids", default=None,
                        required=True) # noqa
    parser.add_argument("--form-data", help="form data transered in csv format", default=None, required=True)  # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ExposomicsGenrateTool(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
