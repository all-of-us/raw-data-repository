#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import logging
from traceback import print_exc
import sys

from rdr_service.services.system_utils import setup_logging, setup_i18n

from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.tools.tool_libs.tool_base import ToolBase
from rdr_service.offline.study_nph_biobank_file_export import main as study_nph_biobank_file_export_job

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "study_nph_biobank_file_export"
tool_desc = "NPH Study BioBank File Export"


class StudyNphBioBankFileExport(ToolBase):

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super().__init__(args, gcp_env)

    def run(self):
        if self.args.project == 'all-of-us-rdr-prod':
            _logger.error(f'Participant generator cannot be used on project: {self.args.project}')
            return 1
        self.gcp_env.activate_sql_proxy()
        study_nph_biobank_file_export_job()

        return 0


def nph_study_biobank_file_export_for_run(args, gcp_env):
    datagen_map = {
        'study_nph_biobank_file_export': StudyNphBioBankFileExport(args, gcp_env),
    }
    return datagen_map.get(args.process)


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
    parser.add_argument("--account", help="pmi-ops account", default=None)
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa

    subparser = parser.add_subparsers(help='', dest='process')

    participants = subparser.add_parser("study_nph_biobank_file_export")
    participants.add_argument("--spec-path", help="path to the request form", default=None)  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        try:
            # nph_biobank_file_export_job = nph_study_biobank_file_export_for_run(args, gcp_env)
            exit_code = StudyNphBioBankFileExport(args, gcp_env).run()
        # pylint: disable=broad-except
        except Exception as e:
            print_exc()
            _logger.exception(e)
            _logger.info(f'Error has occured, {e}. For help use "study_nph_biobank_file_export --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
