#! /bin/env python
#
# Utilities for the Genomic System
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import datetime
import logging
import math
import sys
import os
import csv
import pytz
from sqlalchemy import text

from rdr_service import clock, config
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.bq_genomics_dao import bq_genomic_set_member_update, bq_genomic_set_update, \
    bq_genomic_job_run_update, bq_genomic_gc_validation_metrics_update, bq_genomic_file_processed_update
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicSetDao, GenomicJobRunDao, \
    GenomicGCValidationMetricsDao, GenomicFileProcessedDao
from rdr_service.genomic.genomic_job_components import GenomicBiobankSamplesCoupler, GenomicFileIngester
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic.genomic_biobank_manifest_handler import (
    create_and_upload_genomic_biobank_manifest_file)
from rdr_service.genomic.genomic_state_handler import GenomicStateHandler
from rdr_service.model.genomics import GenomicSetMember, GenomicSet, GenomicGCValidationMetrics, GenomicFileProcessed
from rdr_service.offline.genomic_pipeline import reconcile_metrics_vs_genotyping_data
from rdr_service.resource.generators.genomics import genomic_set_member_update, genomic_set_update, \
    genomic_job_run_update, genomic_gc_validation_metrics_update, genomic_file_processed_update
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.storage import GoogleCloudStorageProvider, LocalFilesystemStorageProvider
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.participant_enums import GenomicManifestTypes, GenomicSetStatus, GenomicJob, GenomicSubProcessResult, \
    GenomicWorkflowState, GenomicSetMemberStatus

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
        self.counter = 0
        self.msg = "Updated"  # Output message


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
        filename = f'{self.DRC_BIOBANK_PREFIX}-{self.nowf}_C2-{str(set_id)}.CSV'

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
                _logger.info('Running Cohort 2 workflow')
                return self.generate_local_c2_manifest()

            if int(self.args.cohort) == 1:
                _logger.info('Running Cohort 1 workflow')
                return self.generate_local_c1_manifest()

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
        :param set_id:
        :return:
        """

        project_config = self.gcp_env.get_app_config()
        bucket_name = project_config.get(config.BIOBANK_SAMPLES_BUCKET_NAME)[0]
        folder_name = "genomic_samples_manifests"

        # creates local file
        _logger.info(f"Exporting samples to manifest...")
        _filename = f'{folder_name}/{self.DRC_BIOBANK_PREFIX}-{self.nowf}_C2-{str(set_id)}.CSV'

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

    def generate_local_c1_manifest(self):
        """
        Creates a new C1 Manifest locally
        :return:
        """

        last_run_time = self.dao.get_last_successful_runtime(GenomicJob.C1_PARTICIPANT_WORKFLOW)

        if last_run_time is None:
            last_run_time = datetime.datetime(2020, 7, 27, 0, 0, 0, 0)

        job_run = self.dao.insert_run_record(GenomicJob.C1_PARTICIPANT_WORKFLOW)

        biobank_coupler = GenomicBiobankSamplesCoupler(job_run.id)
        new_set_id = biobank_coupler.create_c1_genomic_participants(last_run_time, local=True)
        if new_set_id == GenomicSubProcessResult.NO_FILES:
            _logger.info("No records to include in manifest.")
            self.dao.update_run_record(job_run.id, GenomicSubProcessResult.NO_FILES, 1)
            return 1

        self.export_c1_manifest_to_local_file(new_set_id)
        self.dao.update_run_record(job_run.id, GenomicSubProcessResult.SUCCESS, 1)

        return 0

    def export_c1_manifest_to_local_file(self, set_id):
        """
        Processes samples into a local AW0, Cohort 1 manifest file
        :param set_id:
        :return:
        """

        project_config = self.gcp_env.get_app_config()
        bucket_name = project_config.get(config.BIOBANK_SAMPLES_BUCKET_NAME)[0]
        folder_name = "genomic_samples_manifests"

        # creates local file
        _logger.info(f"Exporting samples to manifest...")
        _filename = f'{folder_name}/{self.DRC_BIOBANK_PREFIX}-{self.nowf}_C1-{str(set_id)}.CSV'

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


class ControlSampleClass(GenomicManifestBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(ControlSampleClass, self).__init__(args, gcp_env)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

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

            if not self.args.dryrun:
                new_genomic_set = GenomicSet(
                    genomicSetName=f"New Control Sample List-{self.nowf}",
                    genomicSetCriteria=".",
                    genomicSetVersion=1,
                )

                with self.dao.session() as session:
                    inserted_set = session.merge(new_genomic_set)
                    session.flush()

                    # Update state for PDR
                    bq_genomic_set_update(inserted_set.id, project_id=self.gcp_env.project)
                    genomic_set_update(inserted_set.id)

                    for _sample_id in lines:
                        _logger.warning(f'Inserting {_sample_id}')

                        member_to_insert = GenomicSetMember(
                            genomicSetId=inserted_set.id,
                            sampleId=_sample_id,
                            genomicWorkflowState=GenomicWorkflowState.CONTROL_SAMPLE,
                            participantId=0,
                        )

                        inserted_member = session.merge(member_to_insert)
                        session.commit()

                        bq_genomic_set_member_update(inserted_member.id, project_id=self.gcp_env.project)
                        genomic_set_member_update(inserted_member.id)

            else:
                for _sample_id in lines:
                    _logger.warning(f'Would Insert {_sample_id}')

        return 0


class ManualSampleClass(GenomicManifestBase):
    """
    Class for inserting arbitrary genomic samples manually. Used for E2E testing.
    """
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(ManualSampleClass, self).__init__(args, gcp_env)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        # Validate Aruguments
        if not os.path.exists(self.args.csv):
            _logger.error(f'File {self.args.csv} was not found.')
            return 1

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()

        # Update gsm IDs from file
        with open(self.args.csv, encoding='utf-8-sig') as f:
            csvreader = csv.reader(f, delimiter=",")

            if not self.args.dryrun:
                new_genomic_set = GenomicSet(
                    genomicSetName=f"New Manual Sample List-{self.nowf}",
                    genomicSetCriteria=".",
                    genomicSetVersion=1,
                )

                with self.dao.session() as session:
                    inserted_set = session.merge(new_genomic_set)
                    session.flush()

                    # Update state for PDR
                    bq_genomic_set_update(inserted_set.id, project_id=self.gcp_env.project)
                    genomic_set_update(inserted_set.id)

                    for line in csvreader:
                        _pid = line[0]
                        _bid = line[1]
                        _sample_id = line[2]
                        _sab = line[3]
                        _siteId = line[4]
                        _state = line[5]
                        _logger.warning(f'Inserting {_pid}, {_bid}, {_sample_id}, {_sab}, {_siteId}, {_state}')

                        for _genome_type in (config.GENOME_TYPE_WGS, config.GENOME_TYPE_ARRAY):
                            member_to_insert = GenomicSetMember(
                                genomicSetId=inserted_set.id,
                                biobankId=_bid,
                                sampleId=_sample_id,
                                collectionTubeId=_sample_id,
                                genomicWorkflowState=_state,
                                participantId=int(_pid),
                                genomeType=_genome_type,
                                sexAtBirth=_sab,
                                nyFlag=0,
                                validationStatus=GenomicSetMemberStatus.VALID,
                                gcSiteId=_siteId,
                            )

                            inserted_member = session.merge(member_to_insert)
                            session.flush()

                            bq_genomic_set_member_update(inserted_member.id, project_id=self.gcp_env.project)
                            genomic_set_member_update(inserted_member.id)

                    session.commit()

            else:
                for line in csvreader:
                    _logger.warning(f'Would Insert {line}')

        return 0


class JobRunResult(GenomicManifestBase):
    """Class to set a genomic_job_run.run_result to a particular status."""
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(JobRunResult, self).__init__(args, gcp_env)
        self.valid_job_results = GenomicSubProcessResult.names()

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicJobRunDao()

        # Check that supplied result is valid
        if self.args.result not in self.valid_job_results:
            _logger.error(f'Invalid job result. must be one of {self.valid_job_results}')
            return 1

        # Get the result to update to
        new_result = GenomicSubProcessResult.lookup_by_name(self.args.result)

        job_run = self.dao.get(self.args.id)

        if job_run is None:
            _logger.error(f'Job Run ID not found: {self.args.id}')
            return 1

        # Don't actually update the job_run
        if self.args.dryrun:
            _logger.warning(f'Would update Job Run ID {self.args.id}:')
            _logger.warning(f'    {job_run.runResult} -> {self.args.result}')

            if self.args.message:
                _logger.warning(f'    {job_run.resultMessage} -> {self.args.message}')

        else:
            _logger.warning(f'Updating Job Run ID {self.args.id}:')
            _logger.warning(f'    {job_run.runResult} -> {self.args.result}')

            if self.args.message:
                _logger.warning(f'    {job_run.resultMessage} -> {self.args.message}')
                job_run.resultMessage = self.args.message

            job_run.runResult = new_result

            self.dao.update(job_run)

            # Update run for PDR
            bq_genomic_job_run_update(job_run.id, project_id=self.gcp_env.project)
            genomic_job_run_update(job_run.id)

        return 0


class UpdateGenomicMembersState(GenomicManifestBase):
    """Class update a genomic_set_member to a particular state."""
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(UpdateGenomicMembersState, self).__init__(args, gcp_env)
        self.valid_genomic_states = GenomicWorkflowState.names()

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        _logger.info("Running Genomic State tool")
        if self.args.dryrun:
            _logger.info("Running in dryrun mode. No actual updates to data.")
            self.msg = "Would update"

        # Validate Aruguments
        if not os.path.exists(self.args.csv):
            _logger.error(f'File {self.args.csv} was not found.')
            return 1

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()

        # Document these changes in the dev_note field
        dev_note = input("Please enter a developer note for these updates:\n")
        if dev_note in (None, ""):
            _logger.error('A developer note is required for these changes.')
            return 1

        # Update gsm IDs from file
        with open(self.args.csv, encoding='utf-8-sig') as f:
            csvreader = csv.reader(f, delimiter=",")

            for line in csvreader:
                _member_id = line[0]
                _provided_state = line[1]

                # Check that supplied state is valid
                if _provided_state not in self.valid_genomic_states:
                    _logger.error(f'Invalid genomic. must be one of {self.valid_genomic_states}')
                    return 1

                new_state = GenomicWorkflowState.lookup_by_name(_provided_state)

                # lookup member
                member = self.dao.get(_member_id)
                member.devNote = dev_note

                if member is None:
                    _logger.error(f"Member id {_member_id.rstrip()} does not exist.")
                    return 1

                self.update_genomic_set_member_state(member, new_state)

        _logger.info(f'{self.msg} {self.counter} Genomic Set Member records.')

        return 0

    def update_genomic_set_member_state(self, member, state):
        """
        Sets the member.genomicWorkflowState = state
        :param member:
        :param state:
        :return:
        """

        with self.dao.session() as session:
            if not self.args.dryrun:
                member.genomicWorkflowState = state
                member.genomicWorkflowStateModifiedTime = self.nowts
                session.merge(member)

            _logger.warning(f'{self.msg} member id {member.id}')
            _logger.warning(f"    {member.genomicWorkflowState} -> {state}")

            self.counter += 1

        # Update state for PDR
        bq_genomic_set_member_update(member.id, project_id=self.gcp_env.project)
        genomic_set_member_update(member.id)


class ChangeCollectionTube(GenomicManifestBase):
    """Class to update a genomic_set_member with a different collection tube id."""

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(ChangeCollectionTube, self).__init__(args, gcp_env)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        _logger.info("Running Genomic Collection Tube tool")
        if self.args.dryrun:
            _logger.info("Running in dryrun mode. No actual updates to data.")
            self.msg = "Would update"

        # Validate Aruguments
        if not os.path.exists(self.args.file):
            _logger.error(f'File {self.args.file} was not found.')
            return 1

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()

        # Document these changes in the dev_note field
        dev_note = input("Please enter a developer note for these updates:\n")
        if dev_note in (None, ""):
            _logger.error('A developer note is required for these changes.')
            return 1

        # Update gsm IDs from file
        with open(self.args.file, encoding='utf-8-sig') as f:
            csvreader = csv.reader(f, delimiter=",")

            # Groups for output/logging
            nonexistent_bids = []
            invalid_tubes = []
            already_existing_tubes_same_member = []
            already_existing_tubes_diff_member = []

            # Iterate through each bid-tube_id pair
            for line in csvreader:
                _bid = int(line[0])
                _new_tube_id = line[1]

                # Check that supplied new_tube_id is valid and associated to bid
                valid_tube = self._validate_tube_id(_bid, _new_tube_id)
                if not valid_tube:
                    invalid_tubes.append((_bid, _new_tube_id))

                # lookup member for each genome type
                for _genome_type in (config.GENOME_TYPE_WGS, config.GENOME_TYPE_ARRAY):
                    member = self.dao.get_member_from_biobank_id(_bid, _genome_type)

                    if member is None:
                        # Member with BID doesn't exist, skip it and log it.
                        nonexistent_bids.append((_bid, _genome_type))
                        continue

                    else:
                        # Check that collection tube isn't already used
                        new_tube_member = self.dao.get_member_from_collection_tube(_new_tube_id, _genome_type)

                        if new_tube_member is None and valid_tube:
                            # Set the dev note
                            member.devNote = dev_note

                            # Valid tube isn't used, update the record
                            self.update_genomic_set_member_collection_tube(member, _new_tube_id)

                        else:
                            if new_tube_member is not None:
                                if new_tube_member == member:
                                    # collection tube already set for that member
                                    already_existing_tubes_same_member.append((_bid, _new_tube_id, member.id))

                                else:
                                    # collection tube set for a different member
                                    already_existing_tubes_diff_member.append((_bid, _new_tube_id,
                                                                               new_tube_member.id, member.id))

                            else:
                                _logger.error(f'No valid tube for: bid: {_bid}; tube_id: {_new_tube_id}')

        # Output Summary
        _logger.info(f'{self.msg} {self.counter} Genomic Set Member records.')

        self._output_results(nonexistent_bids,
                             invalid_tubes,
                             already_existing_tubes_same_member,
                             already_existing_tubes_diff_member)

        return 0

    def _validate_tube_id(self, bid, new_tube_id):
        """
        Looks up biobank_stored_sample_ID and validates bid is what is expected
        :param bid:
        :param new_tube_id:
        :return: boolean
        """
        ss_dao = BiobankStoredSampleDao()
        sample = ss_dao.get(new_tube_id)

        if self.args.sample_override:
            return True

        if sample is not None:
            return bid == sample.biobankId

        else:
            return False

    def _output_results(self, nonexistent_bids,
                        invalid_tubes,
                        already_existing_tubes_same_member,
                        already_existing_tubes_diff_member):
        """
        Outputs results if any bad BID/tube IDs supplied
        :param nonexistent_bids:
        :param invalid_tubes:
        :param already_existing_tubes_same_member:
        :param already_existing_tubes_diff_member:
        :return:
        """
        # Nonexistent BIDs
        if len(nonexistent_bids) > 0:
            _logger.warning(f'The following (BID, genome_type) could not be found in the genomics system:')
            for bid_type in nonexistent_bids:
                _logger.warning(f'    {bid_type}')

        # Invalid Tubes
        if len(invalid_tubes) > 0:
            _logger.warning(f'The following collection tubes are not valid:')
            for t in invalid_tubes:
                _logger.warning(f'    {t}')

        # Existing tubes same member
        if len(already_existing_tubes_same_member) > 0:
            _logger.warning(f'The following collection tubes are already associated to the genomic member:')
            for t in already_existing_tubes_same_member:
                _logger.warning(f'    {t}')

        # Existing tubes different member
        if len(already_existing_tubes_diff_member) > 0:
            _logger.warning(f'The following collection tubes are already associated a different genomic member:')
            for t in already_existing_tubes_diff_member:
                _logger.warning(f'    {t}')

    def update_genomic_set_member_collection_tube(self, member, new_tube_id):
        """
        Sets the member.collectionTubeId = new_tube_id
        :param member:
        :param new_tube_id:
        :return:
        """
        _logger.warning(f'{self.msg} member id {member.id}')
        _logger.warning(f"    {member.collectionTubeId} -> {new_tube_id}")

        with self.dao.session() as session:
            if not self.args.dryrun:
                member.collectionTubeId = new_tube_id
                session.merge(member)

                # Update new_tube_id for PDR
                bq_genomic_set_member_update(member.id, project_id=self.gcp_env.project)
                genomic_set_member_update(member.id)

            self.counter += 1


class UpdateGcMetricsClass(GenomicManifestBase):
    """
    Class for updating GC Metrics records
    """
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(UpdateGcMetricsClass, self).__init__(args, gcp_env)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        _logger.info("Running Genomic GC Validation Metrics Update tool")

        if self.args.dryrun:
            _logger.warning("Running in dryrun mode. No actual updates to data.")
            self.msg = "Would update"

        # Validate Aruguments
        if not os.path.exists(self.args.csv):
            _logger.error(f'File {self.args.csv} was not found.')
            return 1

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicGCValidationMetricsDao()

        # Update GC Metrics IDs from file
        with open(self.args.csv, encoding='utf-8-sig') as f:
            csvreader = csv.DictReader(f)

            # Check columns
            _logger.info("Checking CSV columns against GenomicGCValidationMetrics model")
            for field_name in csvreader.fieldnames:
                if field_name not in GenomicGCValidationMetrics.__dict__.keys():
                    _logger.error(f"Field not found in model: {field_name}")
                    return 1

            # Document these changes in the dev_note field
            dev_note = input("Please enter a developer note for these updates:\n")
            if dev_note in (None, ""):
                _logger.error('A developer note is required for these changes.')
                return 1

            for line in csvreader:
                _metric_id = line['id']

                # lookup metric
                metric = self.dao.get(_metric_id)

                if metric is None:
                    _logger.error(f"Metric id {_metric_id.rstrip()} does not exist.")
                    return 1

                metric.devNote = dev_note
                self.process_line_for_gc_metric_object(metric, line)

        _logger.info(f'{self.msg} {self.counter} GC Metrics records.')
        return 0

    def process_line_for_gc_metric_object(self, metric, line):
        """
        Maps each field in CSV to metric attribute and updates the metric object
        :param metric:
        :param line:
        :return:
        """
        for v in line:
            try:
                # Skip ID since this doesn't change
                if v == 'id':
                    continue
                setattr(metric, v, line[v])

            except AttributeError:
                _logger.error(f'Invalid attribute {v}')
                return 1

        # Only update the metric if this is not a dryrun
        if not self.args.dryrun:
            self.dao.update(metric)

            # Update for BQ/Resource
            bq_genomic_gc_validation_metrics_update(metric.id, project_id=self.gcp_env.project)
            genomic_gc_validation_metrics_update(metric.id)

        _logger.warning(f'{self.msg} gc_metric ID {metric.id}')

        self.counter += 1


class GenomicProcessRunner(GenomicManifestBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(GenomicProcessRunner, self).__init__(args, gcp_env)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        if self.args.job not in GenomicJob.names():
            _logger.error(f'Job must be a valid GenomicJob: {GenomicJob.names()}')

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicJobRunDao()

        _logger.info(f"Running Genomic Process Runner for: {self.args.job}")

        if self.args.job == 'AW1_MANIFEST':
            if self.args.file:
                _logger.info(f'File Specified: {self.args.file}')
                return self.run_aw1_manifest()

            else:
                _logger.error(f'A file is required for this job.')
                return 1

        if self.args.job == 'RECONCILE_GENOTYPING_DATA':
            try:
                reconcile_metrics_vs_genotyping_data(provider=self.gscp)

            except Exception as e:   # pylint: disable=broad-except
                _logger.error(e)
                return 1

        if self.args.job == 'METRICS_INGESTION':
            try:
                if self.args.file:
                    _logger.info(f'File Specified: {self.args.file}')
                    return self.run_aw2_manifest()

                elif self.args.csv:
                    _logger.info(f'File list Specified: {self.args.csv}')
                    print(f'Multiple File List Specified: {self.args.csv}')

                    # Validate file exists
                    if not os.path.exists(self.args.csv):
                        _logger.error(f'File {self.args.csv} was not found.')
                        return 1

                    return self.process_multiple_aw2_from_file()

                else:
                    _logger.error(f'A file is required for this job.')
                    return 1

            except Exception as e:   # pylint: disable=broad-except
                _logger.error(e)
                return 1

        return 0

    def run_aw1_manifest(self):
        # Get bucket and filename from argument
        bucket_name = self.args.file.split('/')[0]
        file_name = self.args.file.replace(bucket_name + '/', '')

        # Use a Controller to run the job
        try:
            with GenomicJobController(GenomicJob.AW1_MANIFEST,
                                      storage_provider=self.gscp,
                                      bq_project_id=self.gcp_env.project) as controller:
                controller.bucket_name = bucket_name
                controller.ingest_specific_aw1_manifest(file_name)

            return 0

        except Exception as e:  # pylint: disable=broad-except
            _logger.error(e)
            return 1

    def run_aw2_manifest(self):
        # Get bucket and filename from argument
        bucket_name = self.args.file.split('/')[0]
        file_name = self.args.file.replace(bucket_name + '/', '')
        _logger.info(f'Processing: {file_name}')

        # Use a Controller to run the job
        try:
            with GenomicJobController(GenomicJob.METRICS_INGESTION,
                                      storage_provider=self.gscp,
                                      bq_project_id=self.gcp_env.project) as controller:
                controller.bucket_name = bucket_name
                controller.ingest_specific_manifest(file_name)

            return 0

        except Exception as e:  # pylint: disable=broad-except
            _logger.error(e)
            return 1

    def process_multiple_aw2_from_file(self):
        # Open list of files and run_aw2_manifest() for each one individually
        with open(self.args.csv, encoding='utf-8-sig') as f:
            csvreader = csv.reader(f)

            # Run the AW2 manifest ingestion on each file
            for l in csvreader:
                self.args.file = l[0]
                result = self.run_aw2_manifest()
                if result == 1:
                    return 1

        return 0


class FileUploadDateClass(GenomicManifestBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(FileUploadDateClass, self).__init__(args, gcp_env)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicFileProcessedDao()

        files_to_backfill = self._get_files_to_backfill()

        _logger.info(f"Number of files to backfill: {len(files_to_backfill)}")
        counter = 0

        for file in files_to_backfill:
            # Skip if data is incomplete (i.e. test data on lower environments)
            if file.bucketName is None or file.bucketName == "":
                continue

            # Get blob for file from gcs
            _blob = self.gscp.get_blob(file.bucketName,
                                       file.filePath.replace(f'/{file.bucketName}/', ''))

            if _blob is None:
                _logger.error(f'File does not exist: {file.fileName}')
                continue

            _logger.warning(f'{file.fileName}: {_blob.updated}')

            # Don't update if dryrun
            if self.args.dryrun:
                continue

            else:
                try:
                    # Set upload_date
                    file.uploadDate = _blob.updated
                    self.dao.update(file)

                    # For BQ/PDR
                    bq_genomic_file_processed_update(file.id, project_id=self.gcp_env.project)
                    genomic_file_processed_update(file.id)

                    counter += 1

                except Exception as e:   # pylint: disable=broad-except
                    _logger.error(e)
                    return 1

        _logger.info(f'Updated {counter}/{len(files_to_backfill)} genomic_file_processed records')
        return 0

    def _get_files_to_backfill(self):
        """Lookup for all genomic files processed with null upload_date"""
        with self.dao.session() as session:
            return session.query(
                GenomicFileProcessed
            ).filter(
                GenomicFileProcessed.uploadDate == None
            ).all()


class BackfillGenomicSetMemberFileProcessedID(GenomicManifestBase):
    """
    Tool to backfill genomic_set_member.aw1_file_processed_id
    For AW2F contamination workflow.
    For AW1:
    The ongoing AW1 ingestion will write this field as of RDR 1.86.1.
    This is to backfill the field for previously ingested data.
    AW1 files are matched to a genomic_set_member record
    from the genomic_set_member.package_id field and the
    package_id in the file name.
    """
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(BackfillGenomicSetMemberFileProcessedID, self).__init__(args, gcp_env)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()

        # Validate csv file exists
        if not os.path.exists(self.args.csv):
            _logger.error(f'File {self.args.csv} was not found.')
            return 1

        # Open file of package IDs
        with open(self.args.csv, encoding='utf-8-sig') as f:
            csvreader = csv.reader(f)

            # Iterate through each package ID
            for l in csvreader:
                pkg = l[0]

                members_for_pkg = self.get_members_aw1_from_package_id(pkg)

                record_count_for_pkg = len(members_for_pkg)
                records_updated_count = 0

                for m in members_for_pkg:
                    if not self.args.dryrun:
                        self.update_member_aw1_file_processed_id(m.member_id, m.aw1_id)
                        records_updated_count += 1

                _logger.warning(f'{pkg}: updated {records_updated_count}/{record_count_for_pkg}')

        return 0

    def get_members_aw1_from_package_id(self, pkg_id):
        """
        Return query results of AW1 file processed ID for all genomic
        set members associated to a package ID
        :param pkg_id: the package ID from file name to lookup
        :return: result set of (member_id, aw1_id)
        """

        with self.dao.session() as session:
            # Lookup AW1 file_processed_id from package ID
            sql = """
            SELECT m.id as member_id                    
                , max(f.id) as aw1_id
            FROM genomic_set_member m
                JOIN genomic_manifest_file mf ON (
                        # Return only Package from file path
                        SUBSTRING(
                        SUBSTRING_INDEX(
                        SUBSTRING_INDEX(mf.file_path, "_", -1),
                        ".csv",
                        1
                    ),
                    1,15
                    )
                ) = m.package_id
                JOIN genomic_file_processed f ON f.genomic_manifest_file_id = mf.id
                    and f.file_result = 1
                    and f.file_status = 1
            where true 
                and reconcile_gc_manifest_job_run_id is not null
                and m.package_id is not null
                and m.package_id = :pkg_id
            group by m.id, mf.id
            """

            return session.execute(text(sql), {'pkg_id': pkg_id}).fetchall()

    def update_member_aw1_file_processed_id(self, mid, fid):
        """

        :param mid: member ID
        :param fid: file_id
        """
        member = self.dao.get(mid)
        member.aw1FileProcessedId = fid
        with self.dao.session() as s:
            s.merge(member)


class CalculateContaminationCategoryClass(GenomicManifestBase):
    """
    Recalculate contamination category for an arbitrary set of participants
    Provides the option to use a Cloud Task
    """
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(CalculateContaminationCategoryClass, self).__init__(args, gcp_env)

        self.member_ids = []

        self.genomic_ingester = None

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()

        # Validate csv file exists
        if not os.path.exists(self.args.csv):
            _logger.error(f'File {self.args.csv} was not found.')
            return 1

        _logger.info('Contamination Category')
        with open(self.args.csv, encoding='utf-8-sig') as f:
            csvreader = csv.reader(f)

            # Get list of member IDs
            for l in csvreader:
                self.member_ids.append(l[0])

        # Using a cloud task
        if self.args.cloud_task:
            _logger.info('Using Cloud Task...')
            return self.process_contamination_category_using_cloud_task()

        else:
            _logger.info('Processing contamination category locally...')
            return self.process_contamination_category_locally()

    def process_contamination_category_using_cloud_task(self):
        _task = None if self.gcp_env.project == 'localhost' else GCPCloudTask()

        if _task is not None:
            task_queue = 'resource-tasks'

            batch_size = 100

            # Get total number of batches
            batch_total = math.ceil(len(self.member_ids) / batch_size)
            _logger.info(f'Found {batch_total} member_id batches of size {batch_size}.')

            # Setup counts
            count = 0
            batch_count = 0
            batch = list()

            # Add members to batch and submit cloud tasks
            for mid in self.member_ids:
                batch.append(mid)
                count += 1

                if count == batch_size:
                    data = {"member_ids": batch}

                    if self.args.dryrun:
                        _logger.info("In Dryrun mode, skip submitting cloud task.")
                    else:
                        _task.execute('calculate_contamination_category_task',
                                      payload=data,
                                      queue=task_queue,
                                      project_id=self.gcp_env.project)

                    batch_count += 1
                    _logger.info(f'Task created for batch {batch_count}')

                    # Reset counts
                    batch.clear()
                    count = 0

            # Submit remainder in last batch
            if count > 0:
                batch_count += 1
                data = {"member_ids": batch}
                _task.execute('calculate_contamination_category_task',
                              payload=data,
                              queue=task_queue,
                              project_id=self.gcp_env.project)

            _logger.info(f'Submitted {batch_count} tasks.')

        return 0

    def process_contamination_category_locally(self):

        genomic_ingester = GenomicFileIngester(job_id=GenomicJob.RECALCULATE_CONTAMINATION_CATEGORY)

        for mid in self.member_ids:

            # Get genomic_set_member and gc metric objects
            with self.dao.session() as s:
                record = s.query(GenomicSetMember, GenomicGCValidationMetrics).filter(
                    GenomicSetMember.id == mid,
                    GenomicSetMember.collectionTubeId != None,
                    GenomicGCValidationMetrics.genomicSetMemberId == mid
                ).one_or_none()

                if record is not None:
                    # calculate new contamination category
                    contamination_category = genomic_ingester.calculate_contamination_category(
                        record.GenomicSetMember.collectionTubeId,
                        float(record.GenomicGCValidationMetrics.contamination),
                        record.GenomicSetMember
                        )

                    # Update the contamination category
                    if not self.args.dryrun:
                        record.GenomicGCValidationMetrics.contaminationCategory = contamination_category
                        s.merge(record.GenomicGCValidationMetrics)

                        _logger.warning(f"Updated contamination category for member id: {mid}")

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
    parser.add_argument("--dryrun", help="for testing", default=False, action="store_true")  # noqa

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

    # Set GenomicWorkflowState to provided state for provided member IDs
    member_state_parser = subparser.add_parser("member-state")
    member_state_parser.add_argument("--csv", help="csv file with genomic_set_member.id, state",
                                     default=None, required=True)  # noqa

    # Create GenomicSetMembers for provided control sample IDs (provided by Biobank)
    control_sample_parser = subparser.add_parser("control-sample")
    control_sample_parser.add_argument("--csv", help="csv file with control sample ids", default=None)  # noqa

    # Create Arbitrary GenomicSetMembers for manually provided PID and sample IDs
    manual_sample_parser = subparser.add_parser("manual-sample")
    manual_sample_parser.add_argument("--csv", help="csv file with manual sample ids",
                                       default=None, required=True)  # noqa

    # Update GenomicGCValidationMetrics from CSV
    gc_metrics_parser = subparser.add_parser("update-gc-metrics")
    gc_metrics_parser.add_argument("--csv", help="csv file with genomic_cv_validation_metrics.id, additional fields",
                                     default=None, required=True)  # noqa

    # Update Job Run ID to result
    job_run_parser = subparser.add_parser("job-run-result")
    job_run_parser.add_argument("--id", help="genomic_job_run.id to update",
                                      default=None, required=True)  # noqa
    job_run_parser.add_argument("--result", help="genomic_job_run.run_result to update to",
                                      default=None, required=True)  # noqa
    job_run_parser.add_argument("--message", help="genomic_job_run.result_message to update to (optional)",
                                      default=None, required=False)  # noqa

    # Process Runner
    process_runner_parser = subparser.add_parser("process-runner")
    process_runner_parser.add_argument("--job", help="GenomicJob process to run",
                                       default=None, required=True)
    process_runner_parser.add_argument("--file", help="The full 'bucket/subfolder/file.ext to process",
                                       default=None, required=False)
    process_runner_parser.add_argument("--csv", help="A file specifying multiple manifests to process",
                                       default=None, required=False)

    # Backfill GenomicFileProcessed UploadDate
    upload_date_parser = subparser.add_parser("backfill-upload-date")  # pylint: disable=unused-variable

    # Collection tube
    collection_tube_parser = subparser.add_parser("collection-tube")
    collection_tube_parser.add_argument("--file", help="A CSV file with collection-tube, biobank_id",
                                        default=None, required=False)
    collection_tube_parser.add_argument("--sample-override", help="for testing",
                                        default=False, action="store_true")  # noqa

    # Backfill tool for genomic_set_member.aw1_file_processed_ID
    backfill_file_processed_id_parser = subparser.add_parser("file-processed-id-backfill")
    backfill_file_processed_id_parser.add_argument("--csv", help="A CSV file with the package_ids to backfill",
                                        default=None, required=True)

    # Calculate contamination category
    contamination_category_parser = subparser.add_parser('contamination-category')
    contamination_category_parser.add_argument("--csv", help="A CSV file of genomic_set_member_ids",
                                               default=None, required=True)
    contamination_category_parser.add_argument("--cloud-task",
                                               help="Use a cloud task",
                                               default=False, action="store_true")  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        if args.util == 'resend':
            process = ResendSamplesClass(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'generate-manifest':
            process = GenerateManifestClass(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'member-state':
            process = UpdateGenomicMembersState(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'control-sample':
            process = ControlSampleClass(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'manual-sample':
            process = ManualSampleClass(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'job-run-result':
            process = JobRunResult(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'update-gc-metrics':
            process = UpdateGcMetricsClass(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'process-runner':
            process = GenomicProcessRunner(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'backfill-upload-date':
            process = FileUploadDateClass(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'collection-tube':
            process = ChangeCollectionTube(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'file-processed-id-backfill':
            process = BackfillGenomicSetMemberFileProcessedID(args, gcp_env)
            exit_code = process.run()

        elif args.util == 'contamination-category':
            process = CalculateContaminationCategoryClass(args, gcp_env)
            exit_code = process.run()

        else:
            _logger.info('Please select a utility option to run. For help use "genomic --help".')
            exit_code = 1
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
