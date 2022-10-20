#! /bin/env python
#
# Utilities for the Genomic System
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import math
import sys
import os
import csv
import pytz
from sqlalchemy import text
from sqlalchemy.sql import functions

from rdr_service import clock, config
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.code_constants import GENOME_TYPE, GC_SITE_IDs, AW1_BUCKETS, AW2_BUCKETS
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicSetDao, GenomicJobRunDao, \
    GenomicGCValidationMetricsDao, GenomicFileProcessedDao, GenomicManifestFileDao, \
    GenomicAW1RawDao, GenomicAW2RawDao, GenomicManifestFeedbackDao, GemToGpMigrationDao, GenomicInformingLoopDao, \
    GenomicGcDataFileDao
from rdr_service.genomic.genomic_job_components import GenomicBiobankSamplesCoupler, GenomicFileIngester
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic.genomic_biobank_manifest_handler import (
    create_and_upload_genomic_biobank_manifest_file)
from rdr_service.genomic.genomic_state_handler import GenomicStateHandler
from rdr_service.model.genomics import GenomicSetMember, GenomicSet, GenomicGCValidationMetrics, GenomicFileProcessed, \
    GenomicManifestFeedback
from rdr_service.offline import genomic_pipeline
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.storage import GoogleCloudStorageProvider, LocalFilesystemStorageProvider
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.genomic_enums import GenomicSetStatus, GenomicSetMemberStatus, GenomicJob, GenomicWorkflowState, \
    GenomicSubProcessResult, GenomicManifestTypes
from rdr_service.genomic.genomic_mappings import wgs_file_types_attributes, array_file_types_attributes
from rdr_service.tools.tool_libs.tool_base import ToolBase
from rdr_service.services.system_utils import JSONObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "genomic"
tool_desc = "Genomic system utilities"

_US_CENTRAL = pytz.timezone("US/Central")
_UTC = pytz.utc


class GenomicManifestBase(ToolBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        # Tool_lib attributes
        super().__init__(args, gcp_env)
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
        self.genomic_task_queue = 'genomics'
        self.resource_task_queue = 'resource-tasks'
        self.genomic_cloud_tasks = {
            # AW1
            'AW1_MANIFEST': {
                'process': {
                    'endpoint': 'ingest_aw1_manifest_task'
                },
                'samples': {
                    'endpoint': 'ingest_samples_from_raw_task'
                }
            },
            # AW2
            'METRICS_INGESTION': {
                'process': {
                    'endpoint': 'ingest_aw2_manifest_task'
                },
                'samples': {
                    'endpoint': 'ingest_samples_from_raw_task'
                }
            },
            # AW4
            'AW4_ARRAY_WORKFLOW': {
                'process': {
                    'endpoint': 'ingest_aw4_manifest_task'
                },
            },
            'AW4_WGS_WORKFLOW': {
                'process': {
                    'endpoint': 'ingest_aw4_manifest_task'
                },
            },
            # AW5
            'AW5_ARRAY_MANIFEST': {
                'process': {
                    'endpoint': 'ingest_aw5_manifest_task'
                },
            },
            'AW5_WGS_MANIFEST': {
                'process': {
                    'endpoint': 'ingest_aw5_manifest_task'
                },
            }
        }

    def execute_in_cloud_task(self, *, endpoint, payload, queue):
        task = GCPCloudTask()
        task.execute(
            endpoint,
            payload=payload,
            queue=queue,
            project_id=self.gcp_env.project
        )


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
            return session.query(GenomicSetMember) \
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
            _logger.error(f'{[m.name for m in GenomicManifestTypes]}')
            return 1

        if self.args.csv:
            if not os.path.exists(self.args.csv):
                _logger.error(f'File {self.args.csv} was not found.')
                return 1

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()

        # Parse samples to resend from CSV or List
        samples_list = []
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
        if self.args.manifest == "AW0":
            self.generate_bb_manifest_from_sample_list(samples_list)

        return 0


class GenerateManifestClass(GenomicManifestBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(GenerateManifestClass, self).__init__(args, gcp_env)
        self.limit = None

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetDao()
        args = self.args
        self.limit = args.limit

        # AW0 Manifest
        if args.manifest == "AW0":
            if args.cohort and int(args.cohort) == 2:
                _logger.info('Running Cohort 2 workflow')
                return self.generate_local_c2_remainder_manifest()

            if args.saliva:
                _logger.info('Running saliva samples workflow')
                s_dict = {
                    'origin': args.saliva_origin if args.saliva_origin is not None
                                                    and args.saliva_origin >= 0 else None,
                    'ror': args.saliva_ror if args.saliva_ror is not None
                                              and args.saliva_ror >= 0 else None
                }
                return self.generate_local_saliva_manifest(s_dict)

            if args.long_read:
                _logger.info('Running long read pilot workflow')
                return self.generate_long_read_manifest(limit=self.limit or 394)

    def generate_local_c2_remainder_manifest(self):
        """
        Creates a new C2 Manifest locally for the remaining C2 participants
        :return:
        """

        with GenomicJobController(GenomicJob.C2_PARTICIPANT_WORKFLOW,
                                  bq_project_id=self.gcp_env.project) as controller:
            biobank_coupler = GenomicBiobankSamplesCoupler(controller.job_run.id, controller=controller)
            biobank_coupler.create_c2_genomic_participants(local=True)
            new_set_id = self.dao.get_max_set()
            self.export_manifest_to_local_file(new_set_id, str_type='c2')

        return 0

    def generate_local_saliva_manifest(self, s_dict):
        """
        # to do
        """

        with GenomicJobController(GenomicJob.C2_PARTICIPANT_WORKFLOW,
                                  bq_project_id=self.gcp_env.project) as controller:
            biobank_coupler = GenomicBiobankSamplesCoupler(controller.job_run.id, controller=controller)
            biobank_coupler.create_saliva_genomic_participants(local=True, config=s_dict)
            new_set_id = self.dao.get_max_set()
            self.export_manifest_to_local_file(new_set_id, str_type='saliva')

        return 0

    def generate_long_read_manifest(self, limit=None):

        with GenomicJobController(GenomicJob.C2_PARTICIPANT_WORKFLOW,
                                  bq_project_id=self.gcp_env.project) as controller:
            GenomicBiobankSamplesCoupler(controller.job_run.id, controller=controller) \
                .create_long_read_genomic_participants(limit)
            new_set_id = self.dao.get_max_set()
            _type = 'long_read'
            self.export_manifest_to_local_file(
                new_set_id,
                str_type=_type,
                project=_type,
                update=False,
            )

        return 0

    def export_manifest_to_local_file(
        self,
        set_id,
        str_type=None,
        project=None,
        update=True,
    ):
        """
        Processes samples into a local AW0, Cohort 2 manifest file
        :param set_id:
        :param str_type:
        :param project:
        :param update:
        :return:
        """

        project_config = self.gcp_env.get_app_config()
        bucket_name = project_config.get(config.BIOBANK_SAMPLES_BUCKET_NAME)[0]
        prefix = project_config.get(config.BIOBANK_ID_PREFIX)[0]
        folder_name = "genomic_samples_manifests"

        # creates local file
        _logger.info(f"Exporting samples to manifest...")
        output_str_type = '_{}'.format(str_type) if str_type else ""
        _filename = f'{folder_name}/{self.DRC_BIOBANK_PREFIX}-{self.nowf}{output_str_type}-{str(set_id)}.csv'

        create_and_upload_genomic_biobank_manifest_file(
            set_id,
            self.nowts,
            bucket_name=bucket_name,
            filename=_filename,
            prefix=prefix,
            project=project,
        )

        # Handle Genomic States for manifests
        if update:
            member_dao = GenomicSetMemberDao()
            new_members = member_dao.get_members_from_set_id(set_id)

            for member in new_members:
                self.update_member_genomic_state(member, 'manifest-generated')

        local_path = f'{self.lsp.DEFAULT_STORAGE_ROOT}/{bucket_name}/{_filename}'
        print()

        _logger.info(f'Manifest Exported to local file:')
        _logger.warning(f'  {local_path}')

    @staticmethod
    def update_member_genomic_state(member, _signal):
        """
        Updates a genomic member's genomic state after the manifest has been generated.
        :param member:
        :param signal:
        """
        member_dao = GenomicSetMemberDao()
        new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                      signal=_signal)

        if new_state is not None or new_state != member.genomicWorkflowState:
            member_dao.update_member_workflow_state(member, new_state)


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

                    for _sample_id in lines:
                        _logger.warning(f'Inserting {_sample_id}')

                        member_to_insert = GenomicSetMember(
                            genomicSetId=inserted_set.id,
                            sampleId=_sample_id,
                            genomicWorkflowState=GenomicWorkflowState.CONTROL_SAMPLE,
                            genomicWorkflowStateStr=GenomicWorkflowState.CONTROL_SAMPLE.name,
                            participantId=0,
                        )

                        session.merge(member_to_insert)
                        session.commit()

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
                                genomicWorkflowStateStr=_state.name,
                                participantId=int(_pid),
                                genomeType=_genome_type,
                                sexAtBirth=_sab,
                                nyFlag=0,
                                validationStatus=GenomicSetMemberStatus.VALID,
                                gcSiteId=_siteId,
                            )

                            session.merge(member_to_insert)
                            session.flush()

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
        _logger.warning(f"{member.collectionTubeId} -> {new_tube_id}")

        with self.dao.session() as session:
            if not self.args.dryrun:
                member.collectionTubeId = new_tube_id
                session.merge(member)

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

        _logger.warning(f'{self.msg} gc_metric ID {metric.id}')

        self.counter += 1


class GenomicProcessRunner(GenomicManifestBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(GenomicProcessRunner, self).__init__(args, gcp_env)
        self.gen_enum = None
        self.gen_job_name = None

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        self.gen_enum = GenomicJob.__dict__[self.args.job]
        self.gen_job_name = self.gen_enum.name

        if self.args.cloud_task and self.gen_job_name not in self.genomic_cloud_tasks.keys():
            _logger.error(f'{self.gen_job_name} is not able to run in cloud task.')
            return 1

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicJobRunDao()

        _logger.info(f"Running Genomic Process Runner for: {self.gen_job_name}")

        if self.gen_job_name == 'AW1_MANIFEST':
            if self.args.manifest_file:
                _logger.info(f'Manifest File Specified: {self.args.manifest_file}')
                return self.run_aw1_manifest()

            else:
                _logger.error(f'A manifest file is required for this job.')
                return 1

        if self.gen_job_name == 'AW2F_MANIFEST':
            try:
                if not self.args.csv:
                    _logger.info('--csv of genomic_manifest_feedback ids record required for this job.')

                else:
                    _logger.info(f'Feedback record list specified: {self.args.csv}')

                    # Validate csv file exists
                    if not os.path.exists(self.args.csv):
                        _logger.error(f'File {self.args.csv} was not found.')
                        return 1

                    # Open list of feedback records and process AW2F for each
                    with open(self.args.csv, encoding='utf-8-sig') as f:
                        csvreader = csv.reader(f)
                        # Run the AW2/AW4 manifest ingestion on each file
                        for l in csvreader:
                            feedback_id = l[0]
                            result = self.run_aw2f_manifest(feedback_id)
                            if result != 0:
                                return 1

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(e)
                return 1

        if self.gen_job_name == 'AW3_WGS_WORKFLOW':
            self.run_aw3_manifest(job=GenomicJob.AW3_WGS_WORKFLOW,
                                  manifest_type=GenomicManifestTypes.AW3_WGS,
                                  genome_type=config.GENOME_TYPE_WGS)

        if self.gen_job_name == 'AW3_ARRAY_WORKFLOW':
            self.run_aw3_manifest(job=GenomicJob.AW3_ARRAY_WORKFLOW,
                                  manifest_type=GenomicManifestTypes.AW3_ARRAY,
                                  genome_type=config.GENOME_TYPE_ARRAY)

        if self.gen_job_name == 'RESOLVE_MISSING_FILES':
            self.resolve_missing_files(
                job=GenomicJob.RESOLVE_MISSING_FILES,
            )

        if self.gen_job_name in (
            'METRICS_INGESTION',
            'AW4_ARRAY_WORKFLOW',
            'AW4_WGS_WORKFLOW',
            'AW5_ARRAY_MANIFEST',
            'AW5_WGS_MANIFEST'
        ):
            try:
                if self.args.manifest_file or self.args.csv:
                    _logger.info(f'File(s) Specified: {self.args.manifest_file or self.args.csv}')

                    if self.args.csv and not os.path.exists(self.args.csv):
                        _logger.error(f'CSV File {self.args.csv} was not found.')
                        return 1

                    file_paths = self.args.manifest_file if self.args.manifest_file else self.csv_to_list()
                    bucket_name = file_paths.split('/')[0] if type(file_paths) is not list else file_paths[0].split(
                        '/')[0]

                    if self.args.cloud_task and not self.args.csv:
                        if bucket_name in file_paths:
                            file_name = file_paths.replace(bucket_name + '/', '')
                        else:
                            file_name = file_paths

                        # Get blob for file from gcs
                        _blob = self.gscp.get_blob(bucket_name, file_name)
                        payload = {
                            "file_path": file_paths,
                            "bucket_name": bucket_name,
                            "upload_date": _blob.updated,
                        }
                        return self.execute_in_cloud_task(
                            endpoint=self.genomic_cloud_tasks[self.gen_job_name]['process']['endpoint'],
                            payload=payload,
                            queue=self.genomic_task_queue,
                        )

                    if self.args.manifest_file and type(file_paths) is not list:
                        return self.run_manifest_ingestion(
                            bucket_name=bucket_name,
                            file_name=file_paths.replace(bucket_name + '/', '') if bucket_name in file_paths
                            else file_paths
                        )

                    if self.args.csv and type(file_paths) is list:
                        # Run the AW2/AW4/AW5 manifest ingestion on each file
                        for file in file_paths:
                            self.run_manifest_ingestion(
                                bucket_name=bucket_name,
                                file_name=file.replace(bucket_name + '/', '') if bucket_name in file else file
                            )
                else:
                    _logger.error(f'A manifest file or csv is required for this job.')
                    return 1

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(e)
                return 1

        if self.gen_job_name == 'RECONCILE_ARRAY_DATA':
            try:
                server_config = self.get_server_config()

                with GenomicJobController(self.gen_enum,
                                          storage_provider=self.gscp,
                                          bq_project_id=self.gcp_env.project) as controller:

                    controller.bucket_name_list = server_config[config.GENOMIC_CENTER_DATA_BUCKET_NAME]
                    controller.run_reconciliation_to_data(genome_type='aou_array')

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(e)
                return 1

        if self.gen_job_name == 'RECONCILE_WGS_DATA':
            try:
                server_config = self.get_server_config()

                with GenomicJobController(self.gen_enum,
                                          storage_provider=self.gscp,
                                          bq_project_id=self.gcp_env.project) as controller:

                    controller.bucket_name_list = server_config[config.GENOMIC_CENTER_DATA_BUCKET_NAME]
                    controller.run_reconciliation_to_data(genome_type='aou_wgs')

            except Exception as e:  # pylint: disable=broad-except
                _logger.error(e)
                return 1

        if self.gen_job_name == 'CALCULATE_RECORD_COUNT_AW1':
            self.dao = GenomicManifestFileDao()

            if not self.args.id:
                _logger.error("--id as comma-separated list of genomic_manifest_file.id required for record count")
                return 1

            id_list = [i.strip() for i in self.args.id.split(',')]

            while len(id_list) > 0:
                mid = id_list.pop(0)
                try:
                    int(mid)
                except ValueError:
                    _logger.error('ID must be an integer.')
                    return 1

                self.run_calculate_record_counts_aw1(mid)

        if self.gen_job_name == "RECONCILE_INFORMING_LOOP_RESPONSES":
            with GenomicJobController(GenomicJob.RECONCILE_INFORMING_LOOP_RESPONSES,
                                      storage_provider=self.gscp,
                                      bq_project_id=self.gcp_env.project
                                      ) as controller:
                controller.reconcile_informing_loop_responses()

        return 0

    def run_aw1_manifest(self):
        # Get bucket and filename from argument
        bucket_name = self.args.manifest_file.split('/')[0]
        file_name = self.args.manifest_file.replace(bucket_name + '/', '')

        # Get blob for file from gcs
        _blob = self.gscp.get_blob(bucket_name, file_name)

        if self.args.cloud_task:
            payload = {
                "file_path": self.args.manifest_file,
                "bucket_name": bucket_name,
                "upload_date": _blob.updated,
            }
            return self.execute_in_cloud_task(
                endpoint=self.genomic_cloud_tasks[self.gen_job_name]['process']['endpoint'],
                payload=payload,
                queue=self.genomic_task_queue,
            )

        # Set up file/JSON
        task_data = {
            "job": False,
            "bucket": bucket_name,
            "manifest_file": None,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": _blob.updated,
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": self.args.manifest_file,
            }
        }

        # Call pipeline function
        mf = genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data, project_id=self.gcp_env.project)
        task_data['manifest_file'] = mf
        _task_data = JSONObject(task_data)

        # Use a Controller to run the job
        try:
            with GenomicJobController(self.gen_enum,
                                      storage_provider=self.gscp,
                                      task_data=_task_data,
                                      bq_project_id=self.gcp_env.project) as controller:
                controller.bucket_name = bucket_name
                controller.ingest_specific_aw1_manifest(file_name)

            return 0

        except Exception as e:  # pylint: disable=broad-except
            _logger.error(e)
            return 1

    def run_manifest_ingestion(self, *, bucket_name, file_name):
        _logger.info(f'Processing: {file_name}')

        # Use a Controller to run the job
        try:
            with GenomicJobController(self.gen_enum,
                                      storage_provider=self.gscp,
                                      bq_project_id=self.gcp_env.project) as controller:
                controller.bucket_name = bucket_name
                controller.ingest_specific_manifest(file_name)

            return 0

        except Exception as e:  # pylint: disable=broad-except
            _logger.error(e)
            return 1

    def run_aw2f_manifest(self, feedback_id):
        """
        Runs the AW2f manifest generation workflow for a feedback ID
        :param feedback_id:
        :return:
        """
        try:

            server_config = self.get_server_config()

            # Get the feedback record for the ID
            with self.dao.session() as s:
                feedback_record = s.query(
                    GenomicManifestFeedback
                ).filter(GenomicManifestFeedback.id == feedback_id).one_or_none()

            # Run the AW2F Workflow
            with GenomicJobController(self.gen_enum,
                                      bq_project_id=self.gcp_env.project
                                      ) as controller:

                controller.bucket_name = server_config[config.BIOBANK_SAMPLES_BUCKET_NAME][0]

                controller.generate_manifest(GenomicManifestTypes.AW2F,
                                             _genome_type=None,
                                             feedback_record=feedback_record)

            return 0

        except Exception as e:  # pylint: disable=broad-except
            _logger.error(e)
            return 1

    def run_aw3_manifest(self, job, manifest_type, genome_type):
        server_config = self.get_server_config()

        # Run the AW3 Workflow
        with GenomicJobController(job_id=job,
                                  max_num=4000,
                                  bq_project_id=self.gcp_env.project) as controller:
            controller.bucket_name = server_config[config.DRC_BROAD_BUCKET_NAME][0]

            controller.generate_manifest(
                manifest_type=manifest_type,
                _genome_type=genome_type,
            )

    def resolve_missing_files(self, job):
        # Run the resolve_missing_files job
        with GenomicJobController(job_id=job, bq_project_id=self.gcp_env.project) as controller:
            controller.resolve_missing_gc_files()

    def run_calculate_record_counts_aw1(self, manifest_id):
        _logger.info(f"Calculating record count for manifest_id: {manifest_id}")

        manifest = self.dao.get(manifest_id)

        task_data = {
            "job": self.gen_enum,
            "manifest_file": manifest
        }

        task_data = JSONObject(task_data)

        genomic_pipeline.dispatch_genomic_job_from_task(
            task_data,
            project_id=self.gcp_env.project
        )

    def csv_to_list(self):
        files = []
        with open(self.args.csv, encoding='utf-8-sig') as f:
            csvreader = csv.reader(f)
            # Run the AW2/AW4/AW5 manifest ingestion on each file
            for line in csvreader:
                files.append(line[0])
        return files


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

                    counter += 1

                except Exception as e:  # pylint: disable=broad-except
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
                        self.execute_in_cloud_task(
                            endpoint='calculate_contamination_category_task',
                            payload=data,
                            queue=self.resource_task_queue,
                        )

                    batch_count += 1
                    _logger.info(f'Task created for batch {batch_count}')

                    # Reset counts
                    batch.clear()
                    count = 0

            # Submit remainder in last batch
            if count > 0:
                batch_count += 1
                data = {"member_ids": batch}

                if self.args.dryrun:
                    _logger.info("In Dryrun mode, skip submitting cloud task.")

                else:
                    self.execute_in_cloud_task(
                        endpoint='calculate_contamination_category_task',
                        payload=data,
                        queue=self.resource_task_queue,
                    )

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
                    # Don't update the contamination category if dryrun
                    if self.args.dryrun:
                        _logger.info(f"In Dryrun mode, skip updating calculating category for member id: {mid}.")

                    else:
                        # calculate new contamination category
                        contamination_category = genomic_ingester.calculate_contamination_category(
                            record.GenomicSetMember.collectionTubeId,
                            float(record.GenomicGCValidationMetrics.contamination),
                            record.GenomicSetMember
                        )

                        record.GenomicGCValidationMetrics.contaminationCategory = contamination_category
                        s.merge(record.GenomicGCValidationMetrics)

                        _logger.warning(f"Updated contamination category for member id: {mid}")

        return 0


class IngestionClass(GenomicManifestBase):
    """
    Perform a targeted ingestion of AW1 or AW2 data
    for an arbitrary set of participants based on genomic_set_member.id
    """

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(IngestionClass, self).__init__(args, gcp_env)
        self.gen_enum = None
        self.gen_job_name = None

    def run(self):
        self.gen_enum = GenomicJob.__dict__[self.args.job]
        self.gen_job_name = self.gen_enum.name

        # Validate arguments
        if self.args.cloud_task and self.gen_job_name not in self.genomic_cloud_tasks.keys():
            _logger.error(f'{self.gen_job_name} is not able to run in cloud task.')
            return 1

        if not self.args.csv and not self.args.member_ids:
            _logger.error('Either --csv or --member_ids must be provided.')
            return 1

        if self.args.csv and self.args.member_ids:
            _logger.error('Arguments --csv and --member_ids may not be used together.')
            return 1

        if self.args.csv:
            # Validate csv file exists
            if not os.path.exists(self.args.csv):
                _logger.error(f'File {self.args.csv} was not found.')
                return 1

        # Make list of member_ids from CSV or argument
        member_ids = []
        if self.args.member_ids:
            for member_id in self.args.member_ids.split(','):
                member_ids.append(member_id.strip())
        elif self.args.csv:
            with open(self.args.csv, encoding='utf-8-sig') as h:
                lines = h.readlines()
                for line in lines:
                    member_ids.append(line.strip())

        if member_ids:
            # Activate the SQL Proxy
            self.gcp_env.activate_sql_proxy()
            self.dao = GenomicSetMemberDao()
            server_config = self.get_server_config()
            bucket_name = None

            if self.args.manifest_file:
                _logger.info(f'Manifest file supplied: {self.args.manifest_file}')
                # Get bucket and filename from argument
                bucket_name = self.args.manifest_file.split('/')[0]

            message = 'AW1' if self.gen_job_name == 'AW1_MANIFEST' else 'AW2'
            _logger.info(f"Ingesting {message} data for ids.")

            if self.args.cloud_task:
                payload = {
                    "job": self.args.job,
                    "server_config": server_config,
                    "member_ids": member_ids
                }
                return self.execute_in_cloud_task(
                    endpoint=self.genomic_cloud_tasks[self.gen_job_name]['samples']['endpoint'],
                    payload=payload,
                    queue=self.genomic_task_queue,
                )

            with GenomicJobController(self.gen_enum,
                                      bq_project_id=self.gcp_env.project,
                                      server_config=server_config if self.args.use_raw else None,
                                      storage_provider=self.gscp if bucket_name else None
                                      ) as controller:
                controller.bypass_record_count = self.args.bypass_record_count

                if self.args.use_raw:
                    results = controller.ingest_member_ids_from_awn_raw_table(member_ids)
                    logging.info(results)

                if bucket_name:
                    controller.skip_updates = True
                    for member_id in member_ids:
                        self.run_ingestion_for_member_id(controller, member_id, bucket_name)

    def run_ingestion_for_member_id(self, controller, member_id, bucket_name):
        # Use a Controller to run the job
        try:
            controller.bucket_name = bucket_name
            member = self.dao.get(member_id)
            controller.ingest_awn_data_for_member(f"/{self.args.manifest_file}", member)
            return 0

        except Exception as e:  # pylint: disable=broad-except
            _logger.error(e)
            return 1


class CompareIngestionAW2Class(GenomicManifestBase):
    """
    Performs a comparison on AW2 file counts and counts in database
    """

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(CompareIngestionAW2Class, self).__init__(args, gcp_env)
        self.data_objs = []

    def run(self):
        if self.args.genome_type and self.args.genome_type not in GENOME_TYPE:
            _logger.error('Valid genome type must be provided - {}'.format(GENOME_TYPE))
            return 1

        if self.args.gc_site_id and self.args.gc_site_id not in GC_SITE_IDs:
            _logger.error('Valid gc site id type must be provided - {}'.format(GC_SITE_IDs))
            return 1

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()

        _logger.info('Getting file comparison data...')
        return self.get_file_comparison_data()

    def get_file_comparison_data(self):
        ignore_flag = 0
        work_state = 33

        with self.dao.session() as s:
            records = s.query(
                functions.count(GenomicSetMember.id).label('rdr_count'),
                GenomicFileProcessed.filePath
            ).join(
                GenomicGCValidationMetrics,
                GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
            ).join(
                GenomicFileProcessed,
                GenomicFileProcessed.id == GenomicGCValidationMetrics.genomicFileProcessedId
            ).filter(
                GenomicSetMember.gcSiteId == self.args.gc_site_id,
                GenomicSetMember.genomeType == self.args.genome_type,
                GenomicGCValidationMetrics.ignoreFlag == ignore_flag,
                GenomicWorkflowState != work_state,
            ).group_by(
                GenomicFileProcessed.filePath
            ).all()

            if not records:
                _logger.info('No records found.')
                return 1

            _logger.info('{} records found'.format(len(records)))

            for r in records:
                row_count = self.get_row_count_file(r.filePath)
                self.data_objs.append({
                    'rows_in_file': row_count,
                    'rdr_count': r.rdr_count,
                    'delta': row_count - r.rdr_count,
                    'path': r.filePath,
                })
            self.output_records()

    def get_row_count_file(self, file):
        _logger.info('Getting row count for file...')
        return sum(1 for _ in self.gscp.open(file, 'r')) - 1  # accounting for header row

    def output_records(self):
        filename = 'aw2_comparisons_file_data_{}_{}_{}.csv'.format(
            self.args.gc_site_id,
            self.args.genome_type,
            self.nowf,
        )
        output_local_csv(filename=filename, data=self.data_objs)


class CompareRecordsClass(GenomicManifestBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(CompareRecordsClass, self).__init__(args, gcp_env)
        self.aw1_raw_dao = None
        self.aw2_raw_dao = None
        self.feedback_dao = None
        self.member_dao = None
        self.manifest_dao = None
        self.metrics_dao = None
        self.data_rows = []
        self.main_config = {
            'aw1': {
                'prefixes': [
                    'AW1_genotyping_sample_manifests',
                    'AW1_wgs_sample_manifests',
                ],
                'headers': [
                    'aw1_member_count',
                    'aw1_manifest_file_count',
                    'aw1_raw_count'
                ]
            },
            'aw2': {
                'prefixes': [
                    'AW2_genotyping_data_manifests',
                    'AW2_wgs_data_manifests',
                ],
                'headers': [
                    'aw2_val_metric_count',
                    'aw2_manifest_metric_count',
                    'aw2_raw_count'
                ]
            }
        }

    def get_counts_for_path(self, *, path):
        manifest_type = self.args.manifest_type.lower()
        paths = {
            'aw1': {
                0: self.member_dao.get_member_count_from_manifest_path,
                1: self.manifest_dao.get_record_count_from_filepath,
                2: self.aw1_raw_dao.get_record_count_from_filepath,
            },
            'aw2': {
                0: self.metrics_dao.get_metric_record_counts_from_filepath,
                1: self.feedback_dao.get_feedback_record_counts_from_filepath,
                2: self.aw2_raw_dao.get_record_count_from_filepath,
            }
        }
        _dict = {}
        for i, val in paths[manifest_type].items():
            if val(path):
                _dict[self.main_config[manifest_type]['headers'][i]] = val(path)[0]

        _dict['file_path'] = path

        self.data_rows.append(_dict)
        return self.data_rows

    def get_file_paths_in_bucket(self):
        manifest_type = self.args.manifest_type.lower()
        bucket = self.args.bucket.lower()
        all_files = []
        # sigh no wildcards for dir
        # https://github.com/googleapis/google-cloud-python/issues/4154#issuecomment-521316326
        prefixes = self.main_config[manifest_type]['prefixes']
        for i, prefix in enumerate(prefixes):
            if 'data-broad' in bucket and i == 1:
                # self.gscp.list => case sensitive for prefixes
                prefix = prefix.lower()
            files = self.gscp.list(bucket, prefix)
            all_files.extend(['{}/{}'.format(f.bucket.name, f.name) for f in files if files and '.csv' in f.name])
        return all_files

    def output_records(self):
        manifest_type = self.args.manifest_type.lower()
        sec_pos = (self.args.bucket or self.args.csv or self.args.manifest_file) \
            .replace('/', '_') \
            .strip('.csv')
        filename = 'genomic_ingestion_comparisons__{}__{}__{}__.csv'.format(
            manifest_type,
            sec_pos,
            self.nowf,
        )
        output_local_csv(
            filename=filename,
            data=self.data_rows
        )

    def run(self):
        if not any([self.args.bucket, self.args.csv, self.args.manifest_file]):
            _logger.error('You must include at least one optional arg: bucket/csv/manifest_file')
            return

        if self.args.bucket and ('data' in self.args.bucket and self.args.manifest_type == 'AW1' or
                                 self.args.manifest_type == 'AW2' and 'data' not in self.args.bucket):
            _logger.error('Misconfiguration in bucket and manifest type')
            return

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()

        self.aw1_raw_dao = GenomicAW1RawDao()
        self.aw2_raw_dao = GenomicAW2RawDao()
        self.feedback_dao = GenomicManifestFeedbackDao()
        self.manifest_dao = GenomicManifestFileDao()
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()

        if self.args.manifest_file:
            self.get_counts_for_path(path=self.args.manifest_file)

        elif self.args.bucket:
            bucket_paths = self.get_file_paths_in_bucket() or []
            for path in bucket_paths:
                self.get_counts_for_path(path=path)

        elif self.args.csv:
            csv_list = []
            with open(self.args.csv, encoding='utf-8-sig') as h:
                lines = h.readlines()
                for line in lines:
                    csv_list.append(line.strip())

            for path in csv_list:
                self.get_counts_for_path(path=path)

        self.output_records()


def output_local_csv(*, filename, data):
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=[k for k in data[0]])
        writer.writeheader()
        writer.writerows(data)

    _logger.info('Generated csv: {}/{}'.format(os.getcwd(), filename))


class LoadRawManifest(GenomicManifestBase):
    """
    Loads a manifest in GCS to the raw manifest table
    currently only supports AW1/AW2 manifests
    """

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(LoadRawManifest, self).__init__(args, gcp_env)

    def run(self):

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicJobRunDao()

        manifest_list = []

        if not self.args.manifest_file and not self.args.csv:
            _logger.error('Either --manifest-file or --csv required')
            return 1

        if self.args.manifest_file and self.args.csv:
            _logger.error('--manifest-file and --csv required cannot be used together')
            return 1

        if self.args.manifest_file:
            manifest_list.append(self.args.manifest_file)

        else:
            # Validate csv file exists
            if not os.path.exists(self.args.csv):
                _logger.error(f'File {self.args.csv} was not found.')
                return 1

            with open(self.args.csv, encoding='utf-8-sig') as h:
                lines = h.readlines()
                for line in lines:
                    manifest_list.append(line.strip())

        if manifest_list:
            for manifest_path in manifest_list:
                genomic_pipeline.load_awn_manifest_into_raw_table(
                    file_path=manifest_path,
                    manifest_type=self.args.manifest_type.lower(),
                    project_id=self.gcp_env.project,
                    provider=self.gscp
                )

        return 0


class ReconcileGcDataFileBucket(GenomicManifestBase):
    """
    Loads a manifest in GCS to the raw manifest table
    currently only supports AW1/AW2 manifests
    """

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(ReconcileGcDataFileBucket, self).__init__(args, gcp_env)

    def run(self):
        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()

        with GenomicJobController(GenomicJob.RECONCILE_GC_DATA_FILE_TO_TABLE,
                                  storage_provider=self.gscp,
                                  bq_project_id=self.gcp_env.project) as controller:
            controller.reconcile_gc_data_file_to_table()

        return 0


class GemToGpMigrationClass(GenomicManifestBase):
    """
    Load a GEM to GP Migration batch
    """

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(GemToGpMigrationClass, self).__init__(args, gcp_env)

        self.gem_gp_dao = None
        self.il_dao = None

    def run(self):

        # Activate the SQL Proxy
        self.gcp_env.activate_sql_proxy()
        self.gem_gp_dao = GemToGpMigrationDao()
        self.il_dao = GenomicInformingLoopDao()

        pids = None
        if self.args.csv:
            pids = []
            with open(self.args.csv, encoding='utf-8-sig') as h:
                lines = h.readlines()
                for line in lines:
                    pids.append(line.strip())

        with GenomicJobController(GenomicJob.GEM_GP_MIGRATION_EXPORT,
                                  bq_project_id=self.gcp_env.project) as controller:

            results = self.gem_gp_dao.get_data_for_export(controller.job_run.id, limit=self.args.limit, pids=pids)

            if results:
                self.export_to_gem_gp_table(controller.job_run.id, results)
                self.export_to_informing_loop(results)
            else:
                _logger.info('No data to export.')

        return 0

    def export_to_gem_gp_table(self, run_id, results):
        batch = []
        batch_size = 1000

        now_str = clock.CLOCK.now().strftime('%Y%m%d%H%M%S')
        file_path = f"gem_gp_export_{now_str}.csv"

        for row in results:
            obj_dict = self.gem_gp_dao.prepare_obj(row, run_id, file_path)
            batch.append(obj_dict)

            # write to table in batches
            if len(batch) % batch_size == 0:
                if not self.args.dryrun:
                    _logger.info(f'Inserting batch starting with: {batch[0]["participant_id"]}')
                    self.gem_gp_dao.insert_bulk(batch)

                else:
                    _logger.info(f'Would insert batch starting with: {batch[0]["participant_id"]}')
                batch = []

        # Insert remainder
        if batch:
            if not self.args.dryrun:
                print(f'Inserting batch starting with: {batch[0]["participant_id"]}')
                self.gem_gp_dao.insert_bulk(batch)
            else:
                print(f'Would insert batch starting with: {batch[0]["participant_id"]}')

    def export_to_informing_loop(self, results):
        batch = []
        batch_size = 1000

        for row in results:
            obj_dict = self.il_dao.prepare_gem_migration_obj(row)
            batch.append(obj_dict)

            # write to table in batches
            if len(batch) % batch_size == 0:
                if not self.args.dryrun:
                    _logger.info(f'Inserting batch starting with: {batch[0]["participant_id"]}')
                    self.il_dao.insert_bulk(batch)

                else:
                    _logger.info(f'Would insert batch starting with: {batch[0]["participant_id"]}')
                batch = []

        # Insert remainder
        if batch:
            if not self.args.dryrun:
                print(f'Inserting batch starting with: {batch[0]["participant_id"]}')
                self.il_dao.insert_bulk(batch)
            else:
                print(f'Would insert batch starting with: {batch[0]["participant_id"]}')


class BackFillReplates(GenomicManifestBase):
    """
    Inserts new genomic_set_member records for all samples
    that require replating
    """

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(BackFillReplates, self).__init__(args, gcp_env)

    def run(self):
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()
        ingester = GenomicFileIngester()

        existing_records = self.dao.get_all_contamination_reextract()

        for existing_record in existing_records:
            if not self.args.dryrun:
                ingester.insert_member_for_replating(existing_record.GenomicSetMember,
                                                     existing_record.contaminationCategory)
            else:
                _logger.info(f'Would create member based on id: {existing_record.GenomicSetMember.id}')

        return 0


class ArbitraryReplates(GenomicManifestBase):
    """
    Inserts new genomic_set_member records for
    supplied existing genomic_set_member IDs
    """

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super(ArbitraryReplates, self).__init__(args, gcp_env)

    def run(self):
        self.gcp_env.activate_sql_proxy()
        self.dao = GenomicSetMemberDao()
        ingester = GenomicFileIngester()

        member_ids, genome_type, block_research_reason, block_results_reason = [], None, None, None

        if self.args.csv:
            with open(self.args.csv, encoding='utf-8-sig') as h:
                lines = h.readlines()
                for line in lines:
                    member_ids.append(int(line.strip()))
        else:
            _logger.error('Need --csv')

        existing_records = self.dao.get_members_from_member_ids(member_ids)

        if self.args.genome_type:
            genome_type = self.args.genome_type

        if self.args.block_research_reason:
            block_research_reason = self.args.block_research_reason

        if self.args.block_results_reason:
            block_results_reason = self.args.block_results_reason

        if not self.args.dryrun:
            new_set = self.make_new_set()
        else:
            new_set = None

        for existing_record in existing_records:
            if not self.args.dryrun:
                ingester.copy_member_for_replating(existing_record,
                                                   genome_type=genome_type,
                                                   set_id=new_set.id,
                                                   block_research_reason=block_research_reason,
                                                   block_results_reason=block_results_reason)
            else:
                _logger.info(f'Would create {genome_type} member based on id: {existing_record.id}')

        return 0

    @staticmethod
    def make_new_set():
        set_dao = GenomicSetDao()
        new_set = GenomicSet(
            genomicSetName=f"replating_{clock.CLOCK.now().replace(microsecond=0)}",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        return set_dao.insert(new_set)


class UnblockSamples(ToolBase):
    """
    Unblocks a set of samples.
    """

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super().__init__(args, gcp_env)
        self.server_config = self.get_server_config()
        self.set_members = None
        self.aw1_raw_dao = None
        self.aw2_raw_dao = None
        self.metrics_dao = None
        self.member_dao = None

    def run(self):
        self.gcp_env.activate_sql_proxy()

        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.aw1_raw_dao = GenomicAW1RawDao()
        self.aw2_raw_dao = GenomicAW2RawDao()

        if not os.path.exists(self.args.file_path):
            _logger.error(f'File {self.args.file_path} was not found.')
            return 1

        if not any([self.args.results, self.args.research]):
            _logger.error('At least one of --results or --research is required.')
            return 1

        if not self._set_members_from_file():
            return 1

        self._unblock_members()

        if self.args.reingest and not self.args.dryrun:
            self._reingest_aw1_from_raw()
            self._reingest_aw2_from_raw()
            # Skipping gc data files until new process is implemented
            # self._check_gc_data_files()

    def _set_members_from_file(self):
        with open(self.args.file_path, encoding='utf-8-sig') as file:
            csv_reader = csv.reader(file)
            id_list = []
            header = next(csv_reader)[0]
            if header in ('sample_id', 'biobank_id'):
                id_type = header
            else:
                _logger.error('File header must be sample_id or biobank_id')
                return False

            for row in csv_reader:
                id_list.append(row[0])

        if id_type == 'sample_id':
            self.set_members = self.member_dao.get_members_from_sample_ids(id_list)
        else:
            self.set_members = self.member_dao.get_members_from_biobank_ids(id_list)

        return True

    def _unblock_members(self):
        unblock_operations = []
        if self.args.results:
            unblock_operations.append('Results')

        if self.args.research:
            unblock_operations.append('Research')

        for set_member in self.set_members:
            for operation in unblock_operations:
                # Un-block set member record
                setattr(set_member, f"block{operation}", 0)
                current_reason = getattr(set_member, f"block{operation}Reason")
                if current_reason:
                    setattr(set_member, f"block{operation}Reason",
                            f"Formerly blocked due to '{current_reason}'")

            if not self.args.dryrun:
                self.member_dao.update(set_member)
            else:
                _logger.info(f"Will unblock genomic_set_member {set_member.id}")

    def _ingest_member(self, job_type, set_members):
        with GenomicJobController(GenomicJob.__dict__[job_type],
                                  bq_project_id=self.gcp_env.project,
                                  server_config=self.server_config,
                                  storage_provider=None
                                  ) as controller:
            controller.bypass_record_count = True
            results = controller.ingest_member_ids_from_awn_raw_table(set_members)
            _logger.info(results)

    def _reingest_aw1_from_raw(self):
        no_aw1_data = []
        ingest_members = []
        skipped_members = []

        for set_member in self.set_members:

            # exclude any in EXTRACT_REQUESTED state
            if set_member.genomicWorkflowState == GenomicWorkflowState.EXTRACT_REQUESTED:
                skipped_members.append(set_member.id)
                continue

            # attempt aw1 ingestion
            try:
                pre = self.server_config[config.BIOBANK_ID_PREFIX][0]
            except KeyError:
                # Set default for unit tests
                pre = "A"

            bid = f"{pre}{set_member.biobankId}"
            result = self.aw1_raw_dao.get_raw_record_from_identifier_genome_type(identifier=bid,
                                                                                 genome_type=set_member.genomeType)
            if result:
                ingest_members.append(set_member.id)
            else:
                no_aw1_data.append(set_member.id)

        _logger.info(f"No AW1 data:\n {no_aw1_data}\n")
        _logger.info(f"Member IDs Skipped due to workflow state EXTRACT_REQUESTED:\n {skipped_members}")

        if not self.args.dryrun:
            self._ingest_member('AW1_MANIFEST', ingest_members)
        else:
            _logger.info(f"Will try AW1 ingestion for {ingest_members}")

    def _reingest_aw2_from_raw(self):
        ingest_members = []
        no_aw2_data = []
        skipped_members = []

        for set_member in self.set_members:
            # exclude any in EXTRACT_REQUESTED state
            if set_member.genomicWorkflowState == GenomicWorkflowState.EXTRACT_REQUESTED:
                skipped_members.append(set_member.id)
                continue

            vmetric = self.metrics_dao.get_metrics_by_member_id(set_member.id)
            if vmetric is None:
                aw2_record = self.aw2_raw_dao.get_raw_record_from_identifier_genome_type(
                    identifier=set_member.sampleId, genome_type=set_member.genomeType)
                if aw2_record:
                    ingest_members.append(set_member.id)
                else:
                    no_aw2_data.append(set_member.id)

        _logger.info(f"No AW2 data:\n {no_aw2_data}\n")

        # Perform AW2 ingestion
        if not self.args.dryrun:
            self._ingest_member('METRICS_INGESTION', ingest_members)
        else:
            _logger.info(f"Will try AW2 ingestion for {ingest_members}")

    def _check_gc_data_files(self):
        needs_data_file_reconciliation = []
        array_required_file_types = [file_type['file_received_attribute'] for file_type in array_file_types_attributes
                                     if file_type['required']]
        wgs_required_file_types = [file_type['file_received_attribute'] for file_type in wgs_file_types_attributes
                                   if file_type['required']]

        for set_member in self.set_members:
            vmetric = self.metrics_dao.get_metrics_by_member_id(set_member.id)
            if vmetric:
                if set_member.genomeType == 'aou_array':
                    if any(getattr(vmetric, file_type) != 1 for file_type in array_required_file_types):
                        needs_data_file_reconciliation.append(set_member.sampleId)
                elif set_member.genomeType == 'aou_wgs':
                    if any(getattr(vmetric, file_type) != 1 for file_type in wgs_required_file_types):
                        needs_data_file_reconciliation.append(set_member.sampleId)
        if needs_data_file_reconciliation:
            if not self.args.dryrun:
                with GenomicJobController(GenomicJob.RECONCILE_GC_DATA_FILE_TO_TABLE) as controller:
                    controller.reconcile_gc_data_file_to_table(sample_ids=needs_data_file_reconciliation)
            else:
                _logger.info(f"Will try to reconcile sample ids: {needs_data_file_reconciliation}")


class UpdateMissingFiles(ToolBase):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        super().__init__(args, gcp_env)
        self.member_dao = None
        self.data_file_dao = None
        self.metrics_dao = None

    def run(self):
        self.gcp_env.activate_sql_proxy()

        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.data_file_dao = GenomicGcDataFileDao()

        samples_list = []
        updated_count = 0

        if self.args.file_path:
            if os.path.exists(self.args.file_path):
                with open(self.args.file_path, encoding='utf-8-sig') as file:
                    lines = file.readlines()
                    for line in lines:
                        samples_list.append(line.strip())
            else:
                _logger.error(f"Unable to open {self.args.file_path}")
                return 1
        if self.args.update_filepath_scheme and not samples_list:
            _logger.error(f"List of sample ids required to check filepath scheme.")
            return 1

        if self.args.update_filepath_scheme:
            members_to_update = self.member_dao.get_members_from_sample_ids(samples_list)
        else:
            members_to_update = self.member_dao.get_array_members_files_available(samples_list)
            members_to_update.extend(self.member_dao.get_wgs_members_files_available(samples_list))
        if not self.args.dryrun:
            for member in members_to_update:
                updated = self._update_metric(member, self.args.update_filepath_scheme)
                if updated:
                    updated_count += 1
        else:
            _logger.info(f"Will update {len(members_to_update)} samples")
        _logger.info(f"Found {len(members_to_update)} members to update. Updated {updated_count}.")

    def _update_metric(self, member: GenomicSetMember, update_scheme=False) -> bool:
        file_list = {}
        files = None
        file_types_attributes = None

        metrics = self.metrics_dao.get_metrics_by_member_id(member.id)
        if member.genomeType == 'aou_array':
            file_types_attributes = array_file_types_attributes
            files = self.data_file_dao.get_with_chipwellbarcode(metrics.chipwellbarcode)
        elif member.genomeType == 'aou_wgs':
            file_types_attributes = wgs_file_types_attributes
            files = self.data_file_dao.get_with_sample_id(member.sampleId)
        metric_updated = False
        for file in files:
            file_list[file.file_type] = file.file_path
        for file_type in file_types_attributes:
            if not update_scheme:
                if file_type['required'] and file_type['file_type'] in file_list:
                    if not getattr(metrics, file_type['file_path_attribute']):
                        setattr(metrics, file_type['file_path_attribute'], 'gs://' + file_list[file_type['file_type']])
                        metric_updated = True
            else:
                file_path = getattr(metrics, file_type['file_path_attribute'])
                if file_path and file_path[:5] != 'gs://':
                    setattr(metrics, file_type['file_path_attribute'], 'gs://' + file_path)
                    metric_updated = True
        if metric_updated:
            self.metrics_dao.upsert(metrics)
            return True
        return False


def get_process_for_run(args, gcp_env):
    util = args.util

    process_config = {
        'resend': {
            'process': ResendSamplesClass(args, gcp_env)
        },
        'generate-manifest': {
            'process': GenerateManifestClass(args, gcp_env)
        },
        'member-state': {
            'process': UpdateGenomicMembersState(args, gcp_env)
        },
        'control-sample': {
            'process': ControlSampleClass(args, gcp_env)
        },
        'manual-sample': {
            'process': ManualSampleClass(args, gcp_env)
        },
        'job-run-result': {
            'process': JobRunResult(args, gcp_env)
        },
        'update-gc-metrics': {
            'process': UpdateGcMetricsClass(args, gcp_env)
        },
        'process-runner': {
            'process': GenomicProcessRunner(args, gcp_env)
        },
        'backfill-upload-date': {
            'process': FileUploadDateClass(args, gcp_env)
        },
        'collection-tube': {
            'process': ChangeCollectionTube(args, gcp_env)
        },
        'file-processed-id-backfill': {
            'process': BackfillGenomicSetMemberFileProcessedID(args, gcp_env)
        },
        'contamination-category': {
            'process': CalculateContaminationCategoryClass(args, gcp_env)
        },
        'sample-ingestion': {
            'process': IngestionClass(args, gcp_env)
        },
        'compare-ingestion': {
            'process': CompareIngestionAW2Class(args, gcp_env)
        },
        'compare-records': {
            'process': CompareRecordsClass(args, gcp_env)
        },
        'load-raw-manifest': {
            'process': LoadRawManifest(args, gcp_env)
        },
        'reconcile-gc-data-file': {
            'process': ReconcileGcDataFileBucket(args, gcp_env)
        },
        'gem-to-gp': {
            'process': GemToGpMigrationClass(args, gcp_env)
        },
        'backfill-replates': {
            'process': BackFillReplates(args, gcp_env)
        },
        'arbitrary-replates': {
            'process': ArbitraryReplates(args, gcp_env)
        },
        'unblock-samples': {
            'process': UnblockSamples(args, gcp_env)
        },
        'update-missing-files': {
            'process': UpdateMissingFiles(args, gcp_env)
        }
    }

    return process_config[util]['process']


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
    new_manifest_parser.add_argument("--manifest",
                                     default=None,
                                     required=True,
                                     choices=[m.name for m in GenomicManifestTypes],
                                     type=str
                                     )  # noqa
    new_manifest_parser.add_argument("--cohort",
                                     help="Cohort [1, 2, 3]",
                                     default=None,
                                     required=False
                                     )  # noqa
    new_manifest_parser.add_argument("--saliva",
                                     help="bool for denoting if manifest is saliva only",
                                     default=None,
                                     required=False
                                     )  # noqa
    new_manifest_parser.add_argument("--saliva-origin",
                                     help="origin for saliva manifest config",
                                     choices=[1, 2],
                                     default=None,
                                     required=False,
                                     type=int
                                     )  # noqa
    new_manifest_parser.add_argument("--saliva-ror",
                                     help="denotes consent for genomics ror",
                                     choices=[0, 1, 2],
                                     default=None,
                                     required=False,
                                     type=int
                                     )  # noqa
    new_manifest_parser.add_argument("--long-read",
                                     help="denotes if manifest is for long read pilot",
                                     default=None,
                                     required=False,
                                     )  # noqa

    new_manifest_parser.add_argument("--limit",
                                     help="denotes a LIMIT for the query in the manifest",
                                     default=None,
                                     required=False,
                                     type=int
                                     )  # noqa

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
    process_runner_parser.add_argument("--job",
                                       default=None,
                                       required=True,
                                       choices=[
                                           'AW1_MANIFEST',
                                           'METRICS_INGESTION',
                                           'RECONCILE_ARRAY_DATA',
                                           'RECONCILE_WGS_DATA',
                                           'AW4_ARRAY_WORKFLOW',
                                           'AW4_WGS_WORKFLOW',
                                           'AW5_ARRAY_MANIFEST',
                                           'AW5_WGS_MANIFEST',
                                           'AW2F_MANIFEST',
                                           'CALCULATE_RECORD_COUNT_AW1',
                                           'AW3_WGS_WORKFLOW',
                                           'AW3_ARRAY_WORKFLOW',
                                           'RESOLVE_MISSING_FILES',
                                           'RECONCILE_INFORMING_LOOP_RESPONSES'
                                       ],
                                       type=str
                                       )
    process_runner_parser.add_argument("--manifest-file",
                                       help="The full 'bucket/subfolder/file.ext to process",
                                       default=None,
                                       required=False
                                       )
    process_runner_parser.add_argument("--csv",
                                       help="A file specifying multiple manifests to process",
                                       default=None,
                                       required=False
                                       )
    process_runner_parser.add_argument("--id",
                                       help="A comma-separated list of ids",
                                       default=None,
                                       required=False)
    process_runner_parser.add_argument("--cloud-task",
                                       help="Denotes whether to run workflow in Cloud task",
                                       default=False,
                                       required=False)

    # Backfill GenomicFileProcessed UploadDate
    upload_date_parser = subparser.add_parser("backfill-upload-date")  # pylint: disable=unused-variable
    recon_gc_data_file = subparser.add_parser("reconcile-gc-data-file")  # pylint: disable=unused-variable
    backfill_replate_parser = subparser.add_parser("backfill-replates")  # pylint: disable=unused-variable

    arbitrary_replate_parser = subparser.add_parser("arbitrary-replates")  # pylint: disable=unused-variable
    arbitrary_replate_parser.add_argument("--csv", help="csv of member_ids", default=None)  # noqa
    arbitrary_replate_parser.add_argument("--genome-type", help="genome_type for new records",
                                          type=str, default=None)  # noqa
    arbitrary_replate_parser.add_argument("--block_research_reason", help="reason to block from research pipelines",
                                          type=str, default=None)  # noqa
    arbitrary_replate_parser.add_argument("--block_results_reason", help="reason to block from results pipelines",
                                          type=str, default=None)  # noqa

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

    # Targeted ingestion of AW1 or AW2 data for a member ID
    sample_ingestion_parser = subparser.add_parser("sample-ingestion")
    sample_ingestion_parser.add_argument("--csv", help="A CSV file of genomic_st_member IDs",
                                         default=None, required=False)  # noqa
    sample_ingestion_parser.add_argument("--member-ids",
                                         help="a comma-separated list of genomic_set_member IDs to resend",
                                         default=None,
                                         required=False)  # noqa
    sample_ingestion_parser.add_argument("--job",
                                         default=None,
                                         required=True,
                                         choices=[
                                             'AW1_MANIFEST',
                                             'METRICS_INGESTION'
                                         ],
                                         type=str)  # noqa
    sample_ingestion_parser.add_argument("--manifest-file", help="The full 'bucket/subfolder/file.ext to process",
                                         default=None, required=False)  # noqa
    sample_ingestion_parser.add_argument("--bypass-record-count", help="Flag to skip counting ingested records",
                                         default=False, required=False, action="store_true")  # noqa
    sample_ingestion_parser.add_argument("--use-raw", help="Flag to process records using `raw` table",
                                         default=False, required=False, action="store_true")  # noqa
    sample_ingestion_parser.add_argument("--cloud-task", help="Denotes whether to run workflow in cloud task",
                                         default=False, required=False)  # noqa

    gem_to_gp_parser = subparser.add_parser("gem-to-gp")
    gem_to_gp_parser.add_argument("--limit", help="limit for migration query", type=int,
                                  default=None, required=False)  # noqa
    gem_to_gp_parser.add_argument("--csv", help="csv file with list of pids", type=str,
                                  default=None, required=False)  # noqa

    # Tool for calculate descripancies in AW2 ingestion and AW2 files
    compare_ingestion_parser = subparser.add_parser("compare-ingestion")
    compare_ingestion_parser.add_argument(
        "--genome-type",
        help="genome type choice declaration, choose one: {}".format(GENOME_TYPE),
        choices=GENOME_TYPE,
        default=None,
        required=True,
        type=str
    )

    compare_ingestion_parser.add_argument(
        "--gc-site-id",
        help="genomic center site id, choose one: {}".format(GC_SITE_IDs),
        choices=GC_SITE_IDs,
        default=None,
        required=True,
        type=str
    )

    # Compare records
    compare_records = subparser.add_parser("compare-records")
    compare_records.add_argument(
        "--manifest-type",
        default=None,
        required=True,
        choices=['AW1', 'AW2'],  # AW1 => AW1, AW2 => AW2
        type=str,
    )
    compare_records.add_argument(
        "--bucket",
        help="",
        choices=AW1_BUCKETS + AW2_BUCKETS,
        type=str,
        default=None,
        required=False
    )  # noqa

    compare_records.add_argument(
        "--manifest-file",
        help="The full 'bucket/subfolder/file.csv to process'",
        default=None,
        required=False
    )  # noqa

    compare_records.add_argument(
        "--csv",
        help="csv file with multiple paths to manifest files",
        default=None,
        required=False
    )  # noqa

    # Load Raw AW1 Manifest into genomic_aw1_raw
    load_raw_manifest = subparser.add_parser("load-raw-manifest")
    load_raw_manifest.add_argument(
        "--manifest-file",
        help="The full 'bucket/subfolder/file.ext to process'",
        default=None,
        required=False
    )  # noqa

    load_raw_manifest.add_argument(
        "--manifest-type",
        help="The manifest type to load [aw1, aw2]",
        default=None,
        required=True
    )  # noqa

    load_raw_manifest.add_argument(
        "--csv",
        help="A CSV file of manifest file paths: "
             "[bucket/subfolder/file.ext to process]",
        default=None,
        required=False
    )  # noqa

    unblock_samples = subparser.add_parser('unblock-samples')
    unblock_samples.add_argument(
        "--file-path",
        help="A newline separated list of sample ids or biobank ids." \
             "Must have 'sample_id' or 'biobank_id' as the header",
        default=None,
        required=True
    )
    unblock_samples.add_argument(
        "--results",
        default=False,
        required=False,
        action="store_true"
    )
    unblock_samples.add_argument(
        "--research",
        default=False,
        required=False,
        action="store_true"
    )
    unblock_samples.add_argument(
        "--reingest",
        default=False,
        required=False,
        action="store_true"
    )

    update_missing_files = subparser.add_parser('update-missing-files')
    update_missing_files.add_argument(
        "--file-path",
        help="A newline separated list of sample ids.",
        default=None,
        required=False
    )
    update_missing_files.add_argument(
        "--update-filepath-scheme",
        help="Checks and updates metric filepath if gs:// is missing.",
        default=False,
        required=False
    )

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:

        try:
            process = get_process_for_run(args, gcp_env)
            exit_code = process.run()
        except Exception as e:
            _logger.info(f'Error has occured, {e}. For help use "genomic --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
