#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys
import os

from rdr_service.dao.genomics_dao import GenomicSetMemberDao
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.participant_enums import GenomicManifestTypes

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "genomic"
tool_desc = "Genomic system utilities"


class ResendSamplesClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.dao = None

    def resend_biobank_manifest(self):
        pass

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        # Check Args
        if not self.args.manifest:
            _logger.error('--manifest must be provided.')
            return 1

        if not self.args.csv and not self.args.samples:
            _logger.error('Either --csv or --samples must be provided.')
            return 1

        if self.args.csv and self.args.samples:
            _logger.error('Arguments --csv and --samples may not be used together.')
            return 1

        # Check Manifest Type
        if self.args.manifest not in [m.name for m in GenomicManifestTypes]:
            _logger.error('Please choose a valid manifest type:')
            _logger.error(f'    {[m.name for m in GenomicManifestTypes]}')
            return 1
        # DRC_BIOBANK

        if self.args.csv:
            if not os.path.exists(self.args.csv):
                _logger.error(f'File {self.args.csv} was not found.')
                return 1

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()

        # Parse samples to resend from CSV or List
        samples_list = list()
        if self.args.samples:
            for i in self.args.samples.split(','):
                samples_list.append(int(i.strip()))
        else:
            with open(self.args.csv, encoding='utf-8-sig') as h:
                lines = h.readlines()
                for line in lines:
                    samples_list.append(int(line.strip()))
        print(samples_list)

        if self.args.manifest == "DRC_BIOBANK":
            self.resend_biobank_manifest()

        return 0


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

    subparser = parser.add_subparsers(help='genomic utilities')

    # Resend to biobank tool
    resend_parser = subparser.add_parser("resend")
    manifest_type_list = [m.name for m in GenomicManifestTypes]
    manifest_help = f"which manifest type to resend: {manifest_type_list}"
    resend_parser.add_argument("--manifest", help=manifest_help, default=None)  # noqa
    resend_parser.add_argument("--csv", help="csv file with multiple sample ids to resend", default=None)  # noqa
    resend_parser.add_argument("--samples", help="a comma-separated list of samples to resend", default=None)  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        if hasattr(args, 'manifest'):
            process = ResendSamplesClass(args, gcp_env)
            exit_code = process.run()
        else:
            _logger.info('Please select a utility option to run. For help use "genomic --help".')
            exit_code = 1
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
