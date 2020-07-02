#! /bin/env python
#
# Utilities for the Genomic System
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import csv
import datetime
import logging
import sys
import os

import pytz

from rdr_service import clock, config
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicSetDao, GenomicJobRunDao
from rdr_service.genomic.genomic_job_components import GenomicBiobankSamplesCoupler
from rdr_service.genomic.genomic_biobank_manifest_handler import (
    create_and_upload_genomic_biobank_manifest_file)
from rdr_service.genomic.genomic_state_handler import GenomicStateHandler
from rdr_service.model.genomics import GenomicSetMember, GenomicSet
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.storage import GoogleCloudStorageProvider, LocalFilesystemStorageProvider
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.participant_enums import GenomicManifestTypes, GenomicSetStatus, GenomicJob, GenomicSubProcessResult, \
    GenomicWorkflowState

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "genomic"
tool_desc = "Genomic system utilities"

_US_CENTRAL = pytz.timezone("US/Central")
_UTC = pytz.utc


class GenomicManifestBase(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        # Tool_lib attributes
        self.args = args
        self.gcp_env = gcp_env
        self.dao = None
        self.gscp = GoogleCloudStorageProvider()
        self.lsp = LocalFilesystemStorageProvider()

        # Genomic attributes
        self.OUTPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
        self.DRC_BIOBANK_PREFIX = "Genomic-Manifest-AoU"

        self.nowts = clock.CLOCK.now()
        self.nowf = _UTC.localize(self.nowts).astimezone(_US_CENTRAL) \
            .replace(tzinfo=None).strftime(self.OUTPUT_CSV_TIME_FORMAT)


class ResendSamplesClass(GenomicManifestBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(ResendSamplesClass, self).__init__(args, gcp_env)

    def get_members_for_collection_tubes(self, samples):
        """
        returns the genomic set members' data for collection tube
        :param samples: list of samples to resend
        :return: the members' records for the samples
        """
        with self.dao.session() as session:
            return session.query(GenomicSetMember)\
                .filter(GenomicSetMember.collectionTubeId.in_(samples)).all()

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
                _logger.warning(f"Updating genomic set for collection tube id: {member.collectionTubeId}")
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
        project_config = self.gcp_env.get_app_config()
        bucket_name = project_config.get(config.BIOBANK_SAMPLES_BUCKET_NAME)[0]
        folder_name = project_config.get(config.GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME)[0]

        # creates local file
        _logger.info(f"Exporting samples to manifest...")
        filename = f'{self.DRC_BIOBANK_PREFIX}-{str(set_id)}-{self.nowf}.CSV'

        create_and_upload_genomic_biobank_manifest_file(set_id, self.nowts, bucket_name=bucket_name)
        local_path = f'{self.lsp.DEFAULT_STORAGE_ROOT}/{bucket_name}/{folder_name}/{filename}'

        # upload file and remove local
        if self.gcp_env.project != "localhost":
            self.gscp.upload_from_file(local_path, f"{bucket_name}/{folder_name}/{filename}")
            os.remove(local_path)

        _logger.info(f'Manifest Exported.')
        _logger.warning(f'  {filename} -> {bucket_name}')

    def generate_bb_manifest_from_sample_list(self, samples):
        """
        Executes the methods to create set,
        get the Genomic Set Members, and export the data
        :return:
        """
        members = self.get_members_for_collection_tubes(samples)
        if len(members) > 0:
            genset = self.create_new_genomic_set()
            self.update_members_genomic_set(members, genset.id)
            self.export_bb_manifest(genset.id)
        else:
            _logger.error("No genomic set members for specified samples!")

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

        # Display resend details is about to be done
        _logger.info('-' * 90)
        _logger.info('Please confirm the following')
        _logger.info(f'    Sample Count: {str(len(samples_list))}')
        _logger.info(f'    Project: {self.args.project}')
        _logger.info(f'    Manifest: {self.args.manifest}')
        _logger.info('-' * 90)
        _logger.info('')

        # Confirm
        if not self.args.quiet:
            confirm = input('\nContinue (Y/n)? : ')
            if confirm and confirm.lower().strip() != 'y':
                _logger.warning('Aborting the resend.')
                return 1

        # Execute manifest resends
        if self.args.manifest == "DRC_BIOBANK":
            self.generate_bb_manifest_from_sample_list(samples_list)

        return 0


class GenerateManifestClass(GenomicManifestBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(GenerateManifestClass, self).__init__(args, gcp_env)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicJobRunDao()

        # Check Args
        if not self.args.manifest:
            _logger.error('--manifest must be provided.')
            return 1

        # Check Manifest Type
        if self.args.manifest not in [m.name for m in GenomicManifestTypes]:
            _logger.error('Please choose a valid manifest type:')
            _logger.error(f'    {[m.name for m in GenomicManifestTypes]}')
            return 1

        # AW0 Manifest
        if self.args.manifest == "DRC_BIOBANK":
            if self.args.cohort not in ['1', '2', '3']:
                _logger.error('--cohort [1, 2, 3] must be provided when generating DRC_BIOBANK manifest')
                return 1

            if int(self.args.cohort) == 2:
                _logger.info('Running c2 workflow')
                return self.generate_local_c2_manifest()

    def generate_local_c2_manifest(self):
        """
        Creates a new C2 Manifest locally
        :return:
        """

        last_run_time = self.dao.get_last_successful_runtime(GenomicJob.C2_PARTICIPANT_WORKFLOW)

        if last_run_time is None:
            last_run_time = datetime.datetime(2020, 6, 29, 0, 0, 0, 0)

        job_run = self.dao.insert_run_record(GenomicJob.C2_PARTICIPANT_WORKFLOW)

        biobank_coupler = GenomicBiobankSamplesCoupler(job_run.id)
        new_set_id = biobank_coupler.create_c2_genomic_participants(last_run_time, local=True)
        if new_set_id == GenomicSubProcessResult.NO_FILES:
            _logger.info("No records to include in manifest.")
            self.dao.update_run_record(job_run.id, GenomicSubProcessResult.NO_FILES, 1)
            return 1

        self.export_c2_manifest_to_local_file(new_set_id)
        self.dao.update_run_record(job_run.id, GenomicSubProcessResult.SUCCESS, 1)

        return 0

    def export_c2_manifest_to_local_file(self, set_id):
        """
        Processes samples into a local AW0, Cohort 2 manifest file
        :param genomic_set_id:
        :param set_id:
        :return:
        """

        project_config = self.gcp_env.get_app_config()
        bucket_name = project_config.get(config.BIOBANK_SAMPLES_BUCKET_NAME)[0]
        folder_name = "genomic_samples_manifests"

        # creates local file
        _logger.info(f"Exporting samples to manifest...")
        _filename = f'{folder_name}/{self.DRC_BIOBANK_PREFIX}-{str(set_id)}-{self.nowf}_C2.CSV'

        create_and_upload_genomic_biobank_manifest_file(set_id, self.nowts,
                                                        bucket_name=bucket_name, filename=_filename)

        # Handle Genomic States for manifests
        member_dao = GenomicSetMemberDao()
        new_members = member_dao.get_members_from_set_id(set_id)

        for member in new_members:
            self.update_member_genomic_state(member, 'manifest-generated')

        local_path = f'{self.lsp.DEFAULT_STORAGE_ROOT}/{bucket_name}/{_filename}'
        print()

        _logger.info(f'Manifest Exported to local file:')
        _logger.warning(f'  {local_path}')

    def update_member_genomic_state(self, member, _signal):
        """
        Updates a genomic member's genomic state after the manifest has been generated.
        :param member:
        :param signal:
        """
        member_dao = GenomicSetMemberDao()
        new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                      signal=_signal)

        if new_state is not None or new_state != member.genomicWorkflowState:
            member_dao.update_member_state(member, new_state)


class IgnoreStateClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        # Tool_lib attributes
        self.args = args
        self.gcp_env = gcp_env
        self.dao = None

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        _logger.info("Running ignore tool")

        # Validate Aruguments
        if self.args.csv is None:
            _logger.error('Argument --csv must be provided.')
            return 1

        if not os.path.exists(self.args.csv):
            _logger.error(f'File {self.args.csv} was not found.')
            return 1

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()

        # Update gsm IDs from file
        with open(self.args.csv, encoding='utf-8-sig') as f:
            lines = f.readlines()
            for _member_id in lines:
                _member = self.dao.get(_member_id)

                if _member is None:
                    _logger.warning(f"Member id {_member_id.rstrip()} does not exist.")
                    continue

                self.update_genomic_set_member_state(_member)

        return 0

    def update_genomic_set_member_state(self, member):
        """
        Sets the member.genomicWorkflowState = IGNORE for member
        :param member:
        :return:
        """
        member.genomicWorkflowState = GenomicWorkflowState.IGNORE

        with self.dao.session() as session:
            _logger.info(f"Updating member id {member.id}")
            session.merge(member)


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

    subparser = parser.add_subparsers(help='genomic utilities', dest='util')

    # Resend to biobank tool
    resend_parser = subparser.add_parser("resend")
    manifest_type_list = [m.name for m in GenomicManifestTypes]
    manifest_help = f"which manifest type to resend: {manifest_type_list}"
    resend_parser.add_argument("--quiet", help="do not ask for user input", default=False, action="store_true")  # noqa
    resend_parser.add_argument("--manifest", help=manifest_help, default=None)  # noqa
    resend_parser.add_argument("--csv", help="csv file with multiple sample ids to resend", default=None)  # noqa
    resend_parser.add_argument("--samples", help="a comma-separated list of samples to resend", default=None)  # noqa

    # Manual Manifest Generation
    new_manifest_parser = subparser.add_parser("generate-manifest")
    manifest_type_list = [m.name for m in GenomicManifestTypes]
    new_manifest_help = f"which manifest type to generate: {manifest_type_list}"
    new_manifest_parser.add_argument("--manifest", help=new_manifest_help, default=None)  # noqa
    new_manifest_parser.add_argument("--cohort", help="Cohort [1, 2, 3]", default=None)  # noqa

    # Set GenomicWorkflowState to IGNORE for provided member IDs
    ignore_state_parser = subparser.add_parser("ignore-state")
    ignore_state_parser.add_argument("--csv", help="csv file with genomic_set_member ids", default=None)  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        if args.util == 'resend':
            process = ResendSamplesClass(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'generate-manifest':
            process = GenerateManifestClass(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'ignore-state':
            process = IgnoreStateClass(args, gcp_env)
            exit_code = process.run()

        else:
            _logger.info('Please select a utility option to run. For help use "genomic --help".')
            exit_code = 1
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
