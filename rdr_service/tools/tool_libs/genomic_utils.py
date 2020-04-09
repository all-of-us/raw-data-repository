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

import pytz

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicSetDao
from rdr_service.genomic.genomic_biobank_manifest_handler import (
    create_and_upload_genomic_biobank_manifest_file)
from rdr_service.model.genomics import GenomicSetMember, GenomicSet
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.participant_enums import GenomicManifestTypes, GenomicSetStatus

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "genomic"
tool_desc = "Genomic system utilities"

_US_CENTRAL = pytz.timezone("US/Central")
_UTC = pytz.utc

class ResendSamplesClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        # Tool_lib attributes
        self.args = args
        self.gcp_env = gcp_env
        self.dao = None

        # Genomic attributes
        self.OUTPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
        self.DRC_BIOBANK_PREFIX = "Genomic-Manifest-AoU"

        self.nowts = clock.CLOCK.now()
        self.nowf = _UTC.localize(self.nowts).astimezone(_US_CENTRAL)\
            .replace(tzinfo=None).strftime(self.OUTPUT_CSV_TIME_FORMAT)

    def get_members_for_samples(self, samples):
        """
        returns the genomic set members' data for samples
        :param samples: list of samples to resend
        :return: the members' records for the samples
        """
        with self.dao.session() as session:
            return session.query(GenomicSetMember)\
                .filter(GenomicSetMember.sampleId.in_(samples)).all()

    def update_members_genomic_set(self, members, set_id):
        """
        Updates the genomic set to the new ID
        :param members:
        :param set_id:
        :return: the updated members
        """
        with self.dao.session() as session:
            updated_members = list()
            for member in members:
                member.genomicSetId = set_id
                _logger.warning(f"Updating genomic set for sample: {member.sampleId}")
                updated_members.append(session.merge(member))
        return updated_members

    def create_new_genomic_set(self):
        """
        inserts a new genomic set and returns the genomic set object
        :return: genomic set object
        """
        _logger.info("Creating new Genomic Set...")
        set_dao = GenomicSetDao()
        attributes = {
            'genomicSetName': f'sample_resend_utility_{self.nowf}',
            'genomicSetCriteria': '.',
            'genomicSetVersion': 1,
            'genomicSetStatus': GenomicSetStatus.VALID,
        }
        new_set_obj = GenomicSet(**attributes)
        return set_dao.insert(new_set_obj)

    def export_bb_manifest(self, set_id):
        """
        Runs the manifest handler to export the genomic set
        :param set_id:
        :return:
        """
        _logger.info(f"Exporting samples to manifest...")
        filename = f'{self.DRC_BIOBANK_PREFIX}-{str(set_id)}-{self.nowf}.CSV'
        create_and_upload_genomic_biobank_manifest_file(set_id, self.nowts)
        _logger.warning(f'Manifest exported: {filename}')

    def generate_bb_manifest_from_sample_list(self, samples):
        """
        Executes the methods to create set,
        get the Genomic Set Members, and export the data
        :return:
        """
        genset = self.create_new_genomic_set()
        members = self.get_members_for_samples(samples)
        self.update_members_genomic_set(members, genset.id)
        self.export_bb_manifest(genset.id)

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
            for sample in self.args.samples.split(','):
                samples_list.append(sample.strip())
        else:
            with open(self.args.csv, encoding='utf-8-sig') as h:
                lines = h.readlines()
                for line in lines:
                    samples_list.append(line.strip())

        # Execute manifest resends
        if self.args.manifest == "DRC_BIOBANK":
            self.generate_bb_manifest_from_sample_list(samples_list)

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

    # Genomic Test Datasets

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
