"""
Component Classes for Genomic Jobs
Components are assembled by the JobController for a particular Genomic Job
"""

import csv
import json
import logging
import re
import pytz
from collections import deque, namedtuple
from copy import deepcopy
from dateutil.parser import parse
import sqlalchemy
from werkzeug.exceptions import NotFound

from rdr_service import clock, config
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.genomic_enums import ResultsModuleType, ResultsWorkflowState
from rdr_service.genomic.genomic_data import GenomicQueryClass
from rdr_service.genomic.genomic_state_handler import GenomicStateHandler
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.code import Code
from rdr_service.model.participant_summary import ParticipantRaceAnswers, ParticipantSummary
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.resource.generators.genomics import genomic_user_event_metrics_batch_update
from rdr_service.api_util import (
    open_cloud_file,
    copy_cloud_file,
    delete_cloud_file,
    list_blobs,
    get_blob)
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
    GenomicGCValidationMetrics,
    GenomicSampleContamination)
from rdr_service.participant_enums import (
    WithdrawalStatus,
    QuestionnaireStatus,
    SampleStatus,
    Race,
    SuspensionStatus,
    ParticipantCohort)
from rdr_service.genomic_enums import GenomicSetStatus, GenomicSetMemberStatus, GenomicValidationFlag, GenomicJob, \
    GenomicWorkflowState, GenomicSubProcessStatus, GenomicSubProcessResult, GenomicManifestTypes, \
    GenomicContaminationCategory, GenomicQcStatus, GenomicIncidentCode
from rdr_service.dao.genomics_dao import (
    GenomicGCValidationMetricsDao,
    GenomicSetMemberDao,
    GenomicFileProcessedDao,
    GenomicSetDao,
    GenomicJobRunDao,
    GenomicManifestFeedbackDao,
    GenomicManifestFileDao,
    GenomicAW1RawDao,
    GenomicAW2RawDao,
    GenomicGcDataFileDao,
    GenomicGcDataFileMissingDao,
    GenomicIncidentDao,
    UserEventMetricsDao,
    GenomicQueriesDao,
    GenomicCVLAnalysisDao, GenomicResultWorkflowStateDao, GenomicCVLSecondSampleDao, GenomicAppointmentEventMetricsDao)
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.genomic.genomic_biobank_manifest_handler import (
    create_and_upload_genomic_biobank_manifest_file,
)
from rdr_service.genomic.validation import (
    GENOMIC_VALID_AGE,
)
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.config import (
    getSetting,
    GENOMIC_CVL_RECONCILIATION_REPORT_SUBFOLDER,
    GENOMIC_GEM_A1_MANIFEST_SUBFOLDER,
    GENOMIC_GEM_A3_MANIFEST_SUBFOLDER,
    GENOME_TYPE_ARRAY,
    GENOME_TYPE_ARRAY_INVESTIGATION,
    GENOME_TYPE_WGS,
    GENOME_TYPE_WGS_INVESTIGATION,
    GENOMIC_AW3_ARRAY_SUBFOLDER,
    GENOMIC_AW3_WGS_SUBFOLDER,
    BIOBANK_AW2F_SUBFOLDER,
    GENOMIC_INVESTIGATION_GENOME_TYPES,
    CVL_W1IL_HDR_MANIFEST_SUBFOLDER,
    CVL_W1IL_PGX_MANIFEST_SUBFOLDER,
    CVL_W2W_MANIFEST_SUBFOLDER,
    CVL_W3SR_MANIFEST_SUBFOLDER
)
from rdr_service.code_constants import COHORT_1_REVIEW_CONSENT_YES_CODE
from rdr_service.genomic.genomic_mappings import wgs_file_types_attributes, array_file_types_attributes, \
    genome_center_datafile_prefix_map, wgs_metrics_manifest_mapping
from sqlalchemy.orm import aliased


class GenomicFileIngester:
    """
    This class ingests a file from a source GC bucket into the destination table
    """

    def __init__(self, job_id=None,
                 job_run_id=None,
                 bucket=None,
                 archive_folder=None,
                 sub_folder=None,
                 _controller=None,
                 target_file=None):

        self.controller = _controller
        self.job_id = job_id
        self.job_run_id = job_run_id
        self.file_obj = None
        self.file_queue = deque()
        self.target_file = target_file
        self.bucket_name = bucket
        self.archive_folder_name = archive_folder
        self.sub_folder_name = sub_folder
        self.investigation_set_id = None
        self.participant_dao = None

        # Sub Components
        self.file_validator = GenomicFileValidator(
            job_id=self.job_id,
            controller=self.controller
        )
        self.file_mover = GenomicFileMover(archive_folder=self.archive_folder_name)
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.member_dao = GenomicSetMemberDao()
        self.job_run_dao = GenomicJobRunDao()
        self.sample_dao = BiobankStoredSampleDao()
        self.feedback_dao = GenomicManifestFeedbackDao()
        self.manifest_dao = GenomicManifestFileDao()
        self.incident_dao = GenomicIncidentDao()
        self.user_metrics_dao = UserEventMetricsDao()
        self.cvl_analysis_dao = GenomicCVLAnalysisDao()
        self.results_workflow_dao = GenomicResultWorkflowStateDao()
        self.analysis_cols = self.cvl_analysis_dao.model_type.__table__.columns.keys()
        self.set_dao = None
        self.cvl_second_sample_dao = None

    def generate_file_processing_queue(self):
        """
        Creates the list of files to be ingested in this run.
        Ordering is currently arbitrary;
        """
        # Check Target file is set.
        # It will not be set in cron job, but will be set by tool when run manually

        _manifest_file_id = None

        try:
            _manifest_file_id = self.controller.task_data.manifest_file.id
        except AttributeError:
            pass

        if self.target_file is not None:
            if self.controller.storage_provider is not None:
                _blob = self.controller.storage_provider.get_blob(self.bucket_name, self.target_file)
            else:
                _blob = get_blob(self.bucket_name, self.target_file)

            files = [(self.target_file, _blob.updated)]

        else:
            files = self._get_new_file_names_and_upload_dates_from_bucket()

        if files == GenomicSubProcessResult.NO_FILES:
            return files
        else:
            for file_data in files:
                new_file_record = self.file_processed_dao.insert_file_record(
                    self.job_run_id,
                    f'{self.bucket_name}/{file_data[0]}',
                    self.bucket_name,
                    file_data[0].split('/')[-1],
                    upload_date=file_data[1],
                    manifest_file_id=_manifest_file_id)

                self.file_queue.append(new_file_record)

    def _get_new_file_names_and_upload_dates_from_bucket(self):
        """
        Searches the bucket for un-processed files.
        :return: list of (filenames, upload_date) or NO_FILES result code
        """
        # Setup date
        timezone = pytz.timezone('Etc/Greenwich')
        date_limit_obj = timezone.localize(self.controller.last_run_time)

        # Look for new files with valid filenames
        bucket = '/' + self.bucket_name
        files = list_blobs(bucket, prefix=self.sub_folder_name)

        files = [(s.name, s.updated) for s in files
                 if s.updated > date_limit_obj
                 and self.file_validator.validate_filename(s.name)]

        if not files:
            logging.info('No files in cloud bucket {}'.format(self.bucket_name))
            return GenomicSubProcessResult.NO_FILES
        return files

    def generate_file_queue_and_do_ingestion(self):
        """
        Main method of the ingestor component,
        generates a queue and processes each file
        :return: result code
        """
        file_queue_result = self.generate_file_processing_queue()
        if file_queue_result == GenomicSubProcessResult.NO_FILES:
            logging.info('No files to process.')
            return file_queue_result
        else:
            logging.info('Processing files in queue.')
            results = []
            current_file = None
            while len(self.file_queue):
                try:
                    current_file = self.file_queue[0]
                    ingestion_result = self._ingest_genomic_file(current_file)

                    file_ingested = self.file_queue.popleft()
                    results.append(ingestion_result == GenomicSubProcessResult.SUCCESS)

                    if ingestion_result:
                        ingestion_message = f'Ingestion attempt for {file_ingested.fileName}: {ingestion_result}'
                        if 'invalid' in ingestion_result.name.lower():
                            logging.warning(ingestion_message)
                        else:
                            logging.info(ingestion_message)

                    self.file_processed_dao.update_file_record(
                        file_ingested.id,
                        GenomicSubProcessStatus.COMPLETED,
                        ingestion_result
                    )

                # pylint: disable=broad-except
                except Exception as e:
                    logging.error(f'Exception occured when ingesting manifest {current_file.filePath}: {e}')
                    self.file_queue.popleft()
                except IndexError:
                    logging.info('No files left in file queue.')

            return GenomicSubProcessResult.SUCCESS if all(results) \
                else GenomicSubProcessResult.ERROR

    @staticmethod
    def _clean_row_keys(val):
        def str_clean(str_val):
            return str_val.lower() \
                .replace(' ', '') \
                .replace('_', '')

        if type(val) is str or 'quoted_name' in val.__class__.__name__.lower():
            return str_clean(val)
        elif 'dict' in val.__class__.__name__.lower():
            return dict(zip([str_clean(key)
                             for key in val], val.values()))

    @staticmethod
    def _clean_alpha_values(value):
        return value[1:] if value[0].isalpha() else value

    def _ingest_genomic_file(self, file_obj):
        """
        Reads a file object from bucket and inserts into DB
        :param: file_obj: A genomic file object
        :return: A GenomicSubProcessResultCode
        """
        self.file_obj = file_obj
        data_to_ingest = self._retrieve_data_from_path(self.file_obj.filePath)

        if data_to_ingest == GenomicSubProcessResult.ERROR:
            return GenomicSubProcessResult.ERROR
        elif data_to_ingest:
            logging.info(f'Ingesting data from {self.file_obj.fileName}')
            logging.info("Validating file.")

            ingestion_map = {
                GenomicJob.AW1_MANIFEST: self._ingest_aw1_manifest,
                GenomicJob.AW1F_MANIFEST: self._ingest_aw1_manifest,
                GenomicJob.METRICS_INGESTION: self._process_gc_metrics_data_for_insert,
                GenomicJob.GEM_A2_MANIFEST: self._ingest_gem_a2_manifest,
                GenomicJob.GEM_METRICS_INGEST: self._ingest_gem_metrics_manifest,
                GenomicJob.AW4_ARRAY_WORKFLOW: self._ingest_aw4_manifest,
                GenomicJob.AW4_WGS_WORKFLOW: self._ingest_aw4_manifest,
                GenomicJob.AW1C_INGEST: self._ingest_aw1c_manifest,
                GenomicJob.AW1CF_INGEST: self._ingest_aw1c_manifest,
                GenomicJob.AW5_ARRAY_MANIFEST: self._ingest_aw5_manifest,
                GenomicJob.AW5_WGS_MANIFEST: self._ingest_aw5_manifest,
                GenomicJob.CVL_W2SC_WORKFLOW: self._ingest_cvl_w2sc_manifest,
                GenomicJob.CVL_W3NS_WORKFLOW: self._ingest_cvl_w3ns_manifest,
                GenomicJob.CVL_W3SS_WORKFLOW: self._ingest_cvl_w3ss_manifest,
                GenomicJob.CVL_W3SC_WORKFLOW: self._ingest_cvl_w3sc_manifest,
                GenomicJob.CVL_W4WR_WORKFLOW: self._ingest_cvl_w4wr_manifest,
                GenomicJob.CVL_W5NF_WORKFLOW: self._ingest_cvl_w5nf_manifest
            }

            self.file_validator.valid_schema = None

            validation_result = self.file_validator.validate_ingestion_file(
                filename=self.file_obj.fileName,
                data_to_validate=data_to_ingest
            )

            if validation_result != GenomicSubProcessResult.SUCCESS:
                # delete raw records
                if self.job_id == GenomicJob.AW1_MANIFEST:
                    raw_dao = GenomicAW1RawDao()
                    raw_dao.delete_from_filepath(file_obj.filePath)

                if self.job_id == GenomicJob.METRICS_INGESTION:
                    raw_dao = GenomicAW2RawDao()
                    raw_dao.delete_from_filepath(file_obj.filePath)

                return validation_result

            try:
                ingestion_type = ingestion_map[self.job_id]
                ingestions = self._set_data_ingest_iterations(data_to_ingest['rows'])

                for row in ingestions:
                    ingestion_type(row)

                self._set_manifest_file_resolved()

                return GenomicSubProcessResult.SUCCESS

            except RuntimeError:
                return GenomicSubProcessResult.ERROR

        else:
            logging.info("No data to ingest.")
            return GenomicSubProcessResult.NO_FILES

    def _set_data_ingest_iterations(self, data_rows):
        all_ingestions = []
        if self.controller.max_num and len(data_rows) > self.controller.max_num:
            current_rows = []
            for row in data_rows:
                current_rows.append(row)
                if len(current_rows) == self.controller.max_num:
                    all_ingestions.append(current_rows.copy())
                    current_rows.clear()

            if current_rows:
                all_ingestions.append(current_rows)

        else:
            all_ingestions.append(data_rows)

        return all_ingestions

    def _set_manifest_file_resolved(self):
        if not self.file_obj:
            return

        has_failed_validation = self.incident_dao.get_open_incident_by_file_name(self.file_obj.fileName)

        if not has_failed_validation:
            return

        self.incident_dao.batch_update_incident_fields(
            [obj.id for obj in has_failed_validation],
            _type='resolved'
        )

    @staticmethod
    def get_aw1_manifest_column_mappings():
        return {
            'packageId': 'packageid',
            'sampleId': 'sampleid',
            'gcManifestBoxStorageUnitId': 'boxstorageunitid',
            'gcManifestBoxPlateId': 'boxid/plateid',
            'gcManifestWellPosition': 'wellposition',
            'gcManifestParentSampleId': 'parentsampleid',
            'collectionTubeId': 'collectiontubeid',
            'gcManifestMatrixId': 'matrixid',
            'gcManifestTreatments': 'treatments',
            'gcManifestQuantity_ul': 'quantity(ul)',
            'gcManifestTotalConcentration_ng_per_ul': 'totalconcentration(ng/ul)',
            'gcManifestTotalDNA_ng': 'totaldna(ng)',
            'gcManifestVisitDescription': 'visitdescription',
            'gcManifestSampleSource': 'samplesource',
            'gcManifestStudy': 'study',
            'gcManifestTrackingNumber': 'trackingnumber',
            'gcManifestContact': 'contact',
            'gcManifestEmail': 'email',
            'gcManifestStudyPI': 'studypi',
            'gcManifestTestName': 'genometype',
            'gcManifestFailureMode': 'failuremode',
            'gcManifestFailureDescription': 'failuremodedesc',
        }

    def _ingest_aw1_manifest(self, rows):
        """
        AW1 ingestion method: Updates the GenomicSetMember with AW1 data
        If the row is determined to be a control sample,
        insert a new GenomicSetMember with AW1 data
        :param rows:
        :return: result code
        """
        _states = [GenomicWorkflowState.AW0, GenomicWorkflowState.EXTRACT_REQUESTED]
        _site = self._get_site_from_aw1()

        for row in rows:
            row_copy = self._clean_row_keys(row)

            row_copy['site_id'] = _site
            # Skip rows if biobank_id is an empty string (row is empty well)
            if row_copy['biobankid'] == "":
                continue

            # Check if this sample has a control sample parent tube
            control_sample_parent = self.member_dao.get_control_sample_parent(
                row_copy['genometype'],
                int(row_copy['parentsampleid'])
            )

            # Create new set member record if the sample
            # has the investigation genome type
            if row_copy['genometype'] in GENOMIC_INVESTIGATION_GENOME_TYPES:
                self.create_investigation_member_record_from_aw1(row_copy)

                # Move to next row in file
                continue

            if control_sample_parent:
                logging.warning(f"Control sample found: {row_copy['parentsampleid']}")

                # Check if the control sample member exists for this GC, BID, collection tube, and sample ID
                # Since the Biobank is reusing the sample and collection tube IDs (which are supposed to be unique)
                cntrl_sample_member = self.member_dao.get_control_sample_for_gc_and_genome_type(
                    _site,
                    row_copy['genometype'],
                    row_copy['biobankid'],
                    row_copy['collectiontubeid'],
                    row_copy['sampleid']
                )

                if not cntrl_sample_member:
                    # Insert new GenomicSetMember record if none exists
                    # for this control sample, genome type, and gc site
                    self.create_new_member_from_aw1_control_sample(row_copy)

                # Skip rest of iteration and go to next row
                continue

            # Find the existing GenomicSetMember
            if self.job_id == GenomicJob.AW1F_MANIFEST:
                # Set the member based on collection tube ID will null sample
                member = self.member_dao.get_member_from_collection_tube(
                    row_copy['collectiontubeid'],
                    row_copy['genometype'],
                    state=GenomicWorkflowState.AW1
                )
            else:
                # Set the member based on collection tube ID will null sample
                member = self.member_dao.get_member_from_collection_tube_with_null_sample_id(
                    row_copy['collectiontubeid'],
                    row_copy['genometype'])

            # Since member not found, and not a control sample,
            # check if collection tube id was swapped by Biobank
            if not member:
                bid = row_copy['biobankid']

                # Strip biobank prefix if it's there
                if bid[0] in [get_biobank_id_prefix(), 'T']:
                    bid = bid[1:]
                member = self.member_dao.get_member_from_biobank_id_in_state(
                    bid,
                    row_copy['genometype'],
                    _states
                )
                # If member found, validate new collection tube ID, set collection tube ID
                if member:
                    if self._validate_collection_tube_id(row_copy['collectiontubeid'], bid):
                        if member.genomeType in [GENOME_TYPE_ARRAY, GENOME_TYPE_WGS]:
                            if member.collectionTubeId:
                                with self.member_dao.session() as session:
                                    self._record_sample_as_contaminated(session, member.collectionTubeId)

                        member.collectionTubeId = row_copy['collectiontubeid']
                else:
                    # Couldn't find genomic set member based on either biobank ID or collection tube
                    _message = f"{self.job_id.name}: Cannot find genomic set member: " \
                               f"collection_tube_id: {row_copy['collectiontubeid']}, " \
                               f"biobank id: {bid}, " \
                               f"genome type: {row_copy['genometype']}"

                    self.controller.create_incident(source_job_run_id=self.job_run_id,
                                                    source_file_processed_id=self.file_obj.id,
                                                    code=GenomicIncidentCode.UNABLE_TO_FIND_MEMBER.name,
                                                    message=_message,
                                                    biobank_id=bid,
                                                    collection_tube_id=row_copy['collectiontubeid'],
                                                    sample_id=row_copy['sampleid'],
                                                    )
                    # Skip rest of iteration and continue processing file
                    continue

            # Check for diversion pouch site
            div_pouch_site_id = self.sample_dao.get_diversion_pouch_site_id(row_copy['collectiontubeid'])
            if div_pouch_site_id:
                member.diversionPouchSiteFlag = 1

            # Process the attribute data
            member_changed, member = self._process_aw1_attribute_data(row_copy, member)
            if member_changed:
                self.member_dao.update(member)

        return GenomicSubProcessResult.SUCCESS

    def create_investigation_member_record_from_aw1(self, aw1_data):
        # Create genomic_set
        if not self.investigation_set_id:
            new_set = self.create_new_genomic_set()
            self.investigation_set_id = new_set.id

        self.participant_dao = ParticipantDao()

        # Get IDs
        biobank_id = aw1_data['biobankid']

        # Strip biobank prefix if it's there
        if biobank_id[0] in [get_biobank_id_prefix(), 'T']:
            biobank_id = biobank_id[1:]

        participant = self.participant_dao.get_by_biobank_id(biobank_id)

        # Create new genomic_set_member
        new_member = GenomicSetMember(
            genomicSetId=self.investigation_set_id,
            biobankId=biobank_id,
            participantId=participant.participantId,
            reconcileGCManifestJobRunId=self.job_run_id,
            genomeType=aw1_data['genometype'],
            sexAtBirth=aw1_data['sexatbirth'],
            blockResearch=1,
            blockResearchReason="Created from AW1 with investigation genome type.",
            blockResults=1,
            blockResultsReason="Created from AW1 with investigation genome type.",
            genomicWorkflowState=GenomicWorkflowState.AW1,
            genomicWorkflowStateStr=GenomicWorkflowState.AW1.name,
        )

        _, member = self._process_aw1_attribute_data(aw1_data, new_member)
        self.member_dao.insert(member)

    def create_new_genomic_set(self):
        new_set = GenomicSet(
            genomicSetName=f"investigation_{self.job_run_id}",
            genomicSetCriteria="investigation genome type",
            genomicSetVersion=1,
        )

        self.set_dao = GenomicSetDao()
        with self.set_dao.session() as session:
            session.add(new_set)
        return new_set

    def load_raw_awn_file(self, raw_dao, **kwargs):
        """
        Loads raw models with raw data from manifests file
        Ex: genomic_aw1_raw => aw1_manifest
        :param raw_dao: Model Dao Class
        :return:
        """

        dao = raw_dao()

        # look up if any rows exist already for the file
        records = dao.get_from_filepath(self.target_file)

        if records:
            logging.warning(f'File already exists in raw table: {self.target_file}')
            return GenomicSubProcessResult.SUCCESS

        file_data = self._retrieve_data_from_path(self.target_file)

        # Return the error status if there is an error in file_data
        if not isinstance(file_data, dict):
            return file_data

        model_columns = dao.model_type.__table__.columns.keys()

        # Processing raw data in batches
        batch_size = 100
        item_count = 0
        batch = list()

        for row in file_data['rows']:
            row_obj = self._set_raw_awn_attributes(row, model_columns)

            if kwargs.get('cvl_site_id'):
                row_obj['cvl_site_id'] = kwargs.get('cvl_site_id')

            row_obj = dao.get_model_obj_from_items(row_obj.items())

            batch.append(row_obj)
            item_count += 1

            if item_count == batch_size:
                # Insert batch into DB
                with dao.session() as session:
                    session.bulk_save_objects(batch)

                # Reset batch
                item_count = 0
                batch = list()

        if item_count:
            # insert last batch if needed
            with dao.session() as session:
                session.bulk_save_objects(batch)

        return GenomicSubProcessResult.SUCCESS

    def ingest_single_aw1_row_for_member(self, member):
        # Open file and pull row based on member.biobankId
        with self.controller.storage_provider.open(self.target_file, 'r') as aw1_file:
            reader = csv.DictReader(aw1_file, delimiter=',')
            row = [r for r in reader if r['BIOBANK_ID'][1:] == str(member.biobankId)][0]

            # Alter field names to remove spaces and change to lower case
            row = self._clean_row_keys(row)

        ingested_before = member.reconcileGCManifestJobRunId is not None

        # Write AW1 data to genomic_set_member table
        gc_manifest_column_mappings = self.get_aw1_manifest_column_mappings()

        # Set attributes from file
        for key in gc_manifest_column_mappings.keys():
            try:
                member.__setattr__(key, row[gc_manifest_column_mappings[key]])
            except KeyError:
                member.__setattr__(key, None)

        # Set other fields not in AW1 file
        member.reconcileGCManifestJobRunId = self.job_run_id
        member.aw1FileProcessedId = self.file_obj.id
        member.gcSite = self._get_site_from_aw1()

        # Only update the member's genomicWorkflowState if it was AW0
        if member.genomicWorkflowState == GenomicWorkflowState.AW0:
            member.genomicWorkflowState = GenomicWorkflowState.AW1
            member.genomicWorkflowStateStr = GenomicWorkflowState.AW1.name
            member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

        # Update member in DB
        self.member_dao.update(member)

        # Update AW1 manifest record count
        if not ingested_before and not self.controller.bypass_record_count:
            self.increment_manifest_file_record_count_from_id()

        return GenomicSubProcessResult.SUCCESS

    def ingest_single_aw2_row_for_member(self, member: GenomicSetMember) -> GenomicSubProcessResult:
        # Open file and pull row based on member.biobankId
        with self.controller.storage_provider.open(self.target_file, 'r') as aw1_file:
            reader = csv.DictReader(aw1_file, delimiter=',')
            row = [r for r in reader if r['Biobank ID'] == str(member.biobankId)][0]

            # Alter field names to remove spaces and change to lower case
            row = self._clean_row_keys(row)

        if row['genometype'] in (GENOME_TYPE_WGS, GENOME_TYPE_WGS_INVESTIGATION):
            row = self._set_metrics_wgs_data_file_paths(row)
        elif row['genometype'] in (GENOME_TYPE_ARRAY, GENOME_TYPE_ARRAY_INVESTIGATION):
            row = self._set_metrics_array_data_file_paths(row)
        row = self.prep_aw2_row_attributes(row, member)

        if row == GenomicSubProcessResult.ERROR:
            return GenomicSubProcessResult.ERROR

        # check whether metrics object exists for that member
        existing_metrics_obj = self.metrics_dao.get_metrics_by_member_id(member.id)

        if existing_metrics_obj is not None:
            metric_id = existing_metrics_obj.id
        else:
            metric_id = None

        self.metrics_dao.upsert_gc_validation_metrics_from_dict(row, metric_id)
        self.update_member_for_aw2(member)

        # Update member in DB
        self.member_dao.update(member)
        self._update_member_state_after_aw2(member)

        # Update AW1 manifest feedback record count
        if existing_metrics_obj is None and not self.controller.bypass_record_count:
            # For feedback manifest loop
            # Get the genomic_manifest_file
            manifest_file = self.file_processed_dao.get(member.aw1FileProcessedId)
            if manifest_file is not None:
                self.feedback_dao.increment_feedback_count(manifest_file.genomicManifestFileId)

        return GenomicSubProcessResult.SUCCESS

    def increment_manifest_file_record_count_from_id(self):
        """
        Increments the manifest record count by 1
        """

        manifest_file = self.manifest_dao.get(self.file_obj.genomicManifestFileId)
        manifest_file.recordCount += 1

        with self.manifest_dao.session() as s:
            s.merge(manifest_file)

    def prep_aw2_row_attributes(self, row: dict, member: GenomicSetMember):
        """
        Set contamination, contamination category,
        call rate, member_id, and file_id on AW2 row dictionary
        :param member:
        :param row:
        :return: row dictionary or ERROR code
        """

        row['member_id'] = member.id
        row['file_id'] = self.file_obj.id

        # handle mapped reads in case they are longer than field length
        if 'mappedreadspct' in row.keys():
            if len(row['mappedreadspct']) > 10:
                row['mappedreadspct'] = row['mappedreadspct'][0:10]

        # Set default values in case they upload "" and processing status of "fail"
        row['contamination_category'] = GenomicContaminationCategory.UNSET
        row['contamination_category_str'] = "UNSET"

        # Truncate call rate
        try:
            row['callrate'] = row['callrate'][:10]
        except KeyError:
            pass
        # Convert blank alignedq30bases to none
        try:
            if row['alignedq30bases'] == '':
                row['alignedq30bases'] = None
        except KeyError:
            pass
        # Validate and clean contamination data
        try:
            row['contamination'] = float(row['contamination'])
            # Percentages shouldn't be less than 0
            if row['contamination'] < 0:
                row['contamination'] = 0
        except ValueError:
            if row['processingstatus'].lower() != 'pass':
                return row
            _message = f'{self.job_id.name}: Contamination must be a number for sample_id: {row["sampleid"]}'
            self.controller.create_incident(source_job_run_id=self.job_run_id,
                                            source_file_processed_id=self.file_obj.id,
                                            code=GenomicIncidentCode.DATA_VALIDATION_FAILED.name,
                                            message=_message,
                                            biobank_id=member.biobankId,
                                            sample_id=row['sampleid'],
                                            )

            return GenomicSubProcessResult.ERROR

        # Calculate contamination_category
        contamination_value = float(row['contamination'])
        category = self.calculate_contamination_category(
            member.collectionTubeId,
            contamination_value,
            member
        )
        row['contamination_category'] = category
        row['contamination_category_str'] = category.name

        return row

    def update_member_for_aw2(self, member: GenomicSetMember):
        """
        Updates the aw2FileProcessedId and possibly the genomicWorkflowState
        of a GenomicSetMember after AW2 data has been ingested
        :param member:
        """

        member.aw2FileProcessedId = self.file_obj.id

        # Only update the state if it was AW1
        if member.genomicWorkflowState == GenomicWorkflowState.AW1:
            member.genomicWorkflowState = GenomicWorkflowState.AW2
            member.genomicWorkflowStateStr = GenomicWorkflowState.AW2.name
            member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

        self.member_dao.update(member)

    def _ingest_gem_a2_manifest(self, rows):
        """
        Processes the GEM A2 manifest file data
        Updates GenomicSetMember object with gem_pass field.
        :param rows:
        :return: Result Code
        """
        try:
            for row in rows:
                sample_id = row['sample_id']
                member = self.member_dao.get_member_from_sample_id_with_state(sample_id,
                                                                              GENOME_TYPE_ARRAY,
                                                                              GenomicWorkflowState.A1)
                if member is None:
                    logging.warning(f'Invalid sample ID: {sample_id}')
                    continue

                member.gemPass = row['success']
                member.gemA2ManifestJobRunId = self.job_run_id
                member.gemDateOfImport = parse(row['date_of_import'])
                _signal = 'a2-gem-pass' if member.gemPass.lower() == 'y' else 'a2-gem-fail'

                # update state and state modifed time only if changed
                if member.genomicWorkflowState != GenomicStateHandler.get_new_state(
                    member.genomicWorkflowState, signal=_signal):
                    member.genomicWorkflowState = GenomicStateHandler.get_new_state(
                        member.genomicWorkflowState,
                        signal=_signal)

                    member.genomicWorkflowStateStr = member.genomicWorkflowState.name
                    member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

                self.member_dao.update(member)

            return GenomicSubProcessResult.SUCCESS
        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_gem_metrics_manifest(self, rows):
        """
        Processes the GEM Metrics manifest file data
        Updates GenomicSetMember object with metrics fields.
        :param rows:
        :return: Result Code
        """
        try:
            for row in rows:
                sample_id = row['sample_id']
                member = self.member_dao.get_member_from_sample_id_with_state(sample_id,
                                                                              GENOME_TYPE_ARRAY,
                                                                              GenomicWorkflowState.GEM_RPT_READY)
                if member is None:
                    logging.warning(f'Invalid sample ID: {sample_id}')
                    continue

                member.gemMetricsAncestryLoopResponse = row['ancestry_loop_response']
                member.gemMetricsAvailableResults = row['available_results']
                member.gemMetricsResultsReleasedAt = row['results_released_at']

                member.colorMetricsJobRunID = self.job_run_id

                self.member_dao.update(member)

            return GenomicSubProcessResult.SUCCESS
        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_aw4_manifest(self, rows):
        """
        Processes the AW4 manifest file data
        :param rows:
        :return:
        """
        try:
            for row in rows:
                row_copy = self._clean_row_keys(row)

                sample_id = row_copy['sampleid']

                member = self.member_dao.get_member_from_aw3_sample(sample_id)
                if member is None:
                    logging.warning(f'Invalid sample ID: {sample_id}')
                    continue

                member.aw4ManifestJobRunID = self.job_run_id
                member.qcStatus = self._get_qc_status_from_value(row_copy['qcstatus'])
                member.qcStatusStr = member.qcStatus.name

                metrics = self.metrics_dao.get_metrics_by_member_id(member.id)

                if metrics:
                    metrics.drcSexConcordance = row_copy['drcsexconcordance']

                    if self.job_id == GenomicJob.AW4_ARRAY_WORKFLOW:
                        metrics.drcCallRate = row_copy['drccallrate']

                    elif self.job_id == GenomicJob.AW4_WGS_WORKFLOW:
                        metrics.drcContamination = row_copy['drccontamination']
                        metrics.drcMeanCoverage = row_copy['drcmeancoverage']
                        metrics.drcFpConcordance = row_copy['drcfpconcordance']

                    self.metrics_dao.upsert(metrics)

                self.member_dao.update(member)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def ingest_metrics_file_from_filepath(self, metric_type, file_path):
        metric_map = {
            'user_events': self.user_metrics_dao
        }

        file_data = self._retrieve_data_from_path(file_path)

        if not isinstance(file_data, dict):
            return file_data

        batch_size, item_count, batch = 100, 0, []

        try:
            metric_dao = metric_map[metric_type]

        except KeyError:
            logging.warning(f'Metric type {metric_type} is invalid for this method')
            return GenomicSubProcessResult.ERROR

        for row in file_data['rows']:

            if row.get('participant_id') and 'P' in row.get('participant_id'):
                participant_id = row['participant_id'].split('P')[-1]
                row['participant_id'] = int(participant_id)

            row['file_path'] = file_path
            row['created'] = clock.CLOCK.now()
            row['modified'] = clock.CLOCK.now()
            row['run_id'] = self.controller.job_run.id

            row_insert_obj = metric_dao.get_model_obj_from_items(row.items())

            batch.append(row_insert_obj)
            item_count += 1

            if item_count == batch_size:
                with metric_dao.session() as session:
                    # Use session add_all() so we can get the newly created primary key id values back.
                    session.add_all(batch)
                    session.commit()
                    # Batch update PDR resource records.
                    genomic_user_event_metrics_batch_update([r.id for r in batch])

                item_count = 0
                batch.clear()

        if item_count:
            with metric_dao.session() as session:
                # Use session add_all() so we can get the newly created primary key id values back.
                session.add_all(batch)
                session.commit()
                # Batch update PDR resource records.
                genomic_user_event_metrics_batch_update([r.id for r in batch])

        return GenomicSubProcessResult.SUCCESS

    @staticmethod
    def ingest_appointment_metrics(file_path):
        try:
            with open_cloud_file(file_path) as json_file:
                json_appointment_data = json.load(json_file)

            if not json_appointment_data:
                logging.warning(f'Appointment metric file {file_path} is empty')
                return GenomicSubProcessResult.NO_RESULTS

            batch_size, item_count, batch = 100, 0, []
            appointment_metric_dao = GenomicAppointmentEventMetricsDao()

            for event in json_appointment_data:
                event_obj = {}
                message_body = event.get('messageBody')

                if event.get('participantId'):
                    participant_id = event.get('participantId')
                    if 'P' in participant_id:
                        participant_id = participant_id.split('P')[-1]

                    event_obj['participant_id'] = int(participant_id)

                event_obj['event_authored_time'] = event.get('eventAuthoredTime')
                event_obj['event_type'] = event.get('event')
                event_obj['module_type'] = message_body.get('module_type')
                event_obj['appointment_event'] = json.dumps(event)
                event_obj['file_path'] = file_path
                event_obj['created'] = clock.CLOCK.now()
                event_obj['modified'] = clock.CLOCK.now()

                batch.append(event_obj)
                item_count += 1

                if item_count == batch_size:
                    appointment_metric_dao.insert_bulk(batch)
                    item_count = 0
                    batch.clear()

            if item_count:
                appointment_metric_dao.insert_bulk(batch)

        except ValueError:
            logging.warning('Appointment metric file must be valid json')
            return GenomicSubProcessResult.ERROR

        return GenomicSubProcessResult.SUCCESS

    def _retrieve_data_from_path(self, path):
        """
        Retrieves the last genomic data file from a bucket
        :param path: The source file to ingest
        :return: CSV data as a dictionary
        """
        try:
            filename = path.split('/')[1]
            logging.info(
                'Opening CSV file from queue {}: {}.'
                            .format(path.split('/')[1], filename)
            )
            if self.controller.storage_provider:
                with self.controller.storage_provider.open(path, 'r') as csv_file:
                    return self._read_data_to_ingest(csv_file)
            else:
                with open_cloud_file(path) as csv_file:
                    return self._read_data_to_ingest(csv_file)

        except FileNotFoundError:
            logging.error(f"File path '{path}' not found")
            return GenomicSubProcessResult.ERROR

    @staticmethod
    def _read_data_to_ingest(csv_file):
        data_to_ingest = {'rows': []}
        csv_reader = csv.DictReader(csv_file, delimiter=",")
        data_to_ingest['fieldnames'] = csv_reader.fieldnames
        for row in csv_reader:
            for key in row.copy():
                if not key:
                    del row[key]
            data_to_ingest['rows'].append(row)
        return data_to_ingest

    def _process_aw1_attribute_data(self, aw1_data, member):
        """
        Checks a GenomicSetMember object for changes provided by AW1 data
        And mutates the GenomicSetMember object if necessary
        :param aw1_data: dict
        :param member: GenomicSetMember
        :return: (boolean, GenomicSetMember)
        """
        # Check if the member needs updating
        if self._test_aw1_data_for_member_updates(aw1_data, member):
            member = self._set_member_attributes_from_aw1(aw1_data, member)
            member = self._set_rdr_member_attributes_for_aw1(aw1_data, member)
            return True, member
        return False, member

    def _test_aw1_data_for_member_updates(self, aw1_data, member):
        """
        Checks each attribute provided by Biobank
        for changes to GenomicSetMember Object
        :param aw1_data: dict
        :param member: GenomicSetMember
        :return: boolean (true if member requires updating)
        """
        gc_manifest_column_mappings = self.get_aw1_manifest_column_mappings()
        member_needs_updating = False

        # Iterate each value and test whether the strings for each field correspond
        for key in gc_manifest_column_mappings.keys():
            if str(member.__getattribute__(key)) != str(aw1_data.get(gc_manifest_column_mappings[key])):
                member_needs_updating = True

        return member_needs_updating

    def _set_member_attributes_from_aw1(self, aw1_data, member):
        """
        Mutates the GenomicSetMember attributes provided by the Biobank
        :param aw1_data: dict
        :param member: GenomicSetMember
        :return: GenomicSetMember
        """
        gc_manifest_column_mappings = self.get_aw1_manifest_column_mappings()

        for key in gc_manifest_column_mappings.keys():
            member.__setattr__(key, aw1_data.get(gc_manifest_column_mappings[key]))

        return member

    def _set_rdr_member_attributes_for_aw1(self, aw1_data, member):
        """
        Mutates the GenomicSetMember RDR attributes not provided by the Biobank
        :param aw1_data: dict
        :param member: GenomicSetMember
        :return: GenomicSetMember
        """
        # Set job run and file processed IDs
        member.reconcileGCManifestJobRunId = self.job_run_id

        # Don't overwrite aw1_file_processed_id when ingesting an AW1F
        if self.job_id == GenomicJob.AW1_MANIFEST:
            member.aw1FileProcessedId = self.file_obj.id

        # Set the GC site ID (sourced from file-name)
        member.gcSiteId = aw1_data['site_id']

        # Only update the state if it was AW0 or AW1 (if in failure manifest workflow)
        # We do not want to regress a state for reingested data
        states_to_update = [GenomicWorkflowState.AW0, GenomicWorkflowState.EXTRACT_REQUESTED]

        if self.controller.job_id == GenomicJob.AW1F_MANIFEST:
            states_to_update = [GenomicWorkflowState.AW1]

        if member.genomicWorkflowState in states_to_update:
            _signal = "aw1-reconciled"

            # Set the signal for a failed sample
            if aw1_data['failuremode'] is not None and aw1_data['failuremode'] != '':
                _signal = 'aw1-failed'

            member.genomicWorkflowState = GenomicStateHandler.get_new_state(
                member.genomicWorkflowState,
                signal=_signal)

            member.genomicWorkflowStateStr = member.genomicWorkflowState.name
            member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

        return member

    def _set_raw_awn_attributes(self, row_data, model_columns):
        """
        Builds dict from row_data and model_columns
        :param row_data: dict
        :param model_columns: Current obj model attribute keys
        :return: dict object
        """
        row_obj = {}
        row = self._clean_row_keys(row_data)

        if self.controller.job_id in [
            GenomicJob.LOAD_AW1_TO_RAW_TABLE,
            GenomicJob.LOAD_CVL_W3SS_TO_RAW_TABLE
        ]:
            # adjusting for biobank fieldnames
            row = dict(zip([re.sub(r'\([^)]*\)', '', key)for key in row], row.values()))
            row = dict(zip([key.replace('/', '') for key in row], row.values()))

        genome_type = row.get('genometype', "")

        if not genome_type and row.get('sampleid'):
            member = self.member_dao.get_member_from_sample_id(row.get('sampleid'))
            genome_type = member.genomeType if member else ""

        row_obj['genome_type'] = genome_type
        row_obj['test_name'] = genome_type

        for column in model_columns:
            clean_column = self._clean_row_keys(column)
            row_value = row.get(clean_column)
            if row_value or row_value == "":
                row_obj[column] = row_value[0:512]

        row_obj['file_path'] = self.target_file
        row_obj['created'] = clock.CLOCK.now()
        row_obj['modified'] = clock.CLOCK.now()

        return row_obj

    def _process_gc_metrics_data_for_insert(self, rows):
        """ Since input files vary in column names,
        this standardizes the field-names before passing to the bulk inserter
        :param rows:
        :return result code
        """
        members_to_update = []
        for row in rows:
            row_copy = self._clean_row_keys(row)
            member = self.member_dao.get_member_from_sample_id(
                int(row_copy['sampleid']),
            )

            if not member:
                bid = row_copy['biobankid']
                if bid[0] in [get_biobank_id_prefix(), 'T']:
                    bid = bid[1:]
                _message = f"{self.job_id.name}: Cannot find genomic set member for bid, sample_id: " \
                           f"{row_copy['biobankid']}, {row_copy['sampleid']}"
                self.controller.create_incident(source_job_run_id=self.job_run_id,
                                                source_file_processed_id=self.file_obj.id,
                                                code=GenomicIncidentCode.UNABLE_TO_FIND_MEMBER.name,
                                                message=_message,
                                                biobank_id=bid,
                                                sample_id=row_copy['sampleid'],
                                                )
                continue

            row_copy = self.prep_aw2_row_attributes(row_copy, member)
            if row_copy == GenomicSubProcessResult.ERROR:
                continue

            # METRICS actions
            pipeline_id = None
            if row_copy['genometype'] in (GENOME_TYPE_ARRAY, GENOME_TYPE_ARRAY_INVESTIGATION):
                row_copy = self._set_metrics_array_data_file_paths(row_copy)
                pipeline_id = row_copy.get('pipelineid')

            elif row_copy['genometype'] in (GENOME_TYPE_WGS, GENOME_TYPE_WGS_INVESTIGATION):
                row_copy = self._set_metrics_wgs_data_file_paths(row_copy)
                pipeline_id = row_copy.get('pipelineid')
                # default and add to row dict deprecated version if no pipeline_id in manifest row
                if not pipeline_id:
                    row_copy['pipelineid'] = pipeline_id = config.GENOMIC_DEPRECATED_WGS_DRAGEN

            existing_metrics_obj = self.metrics_dao.get_metrics_by_member_id(
                member_id=member.id,
                pipeline_id=pipeline_id
            )
            metric_id = None if not existing_metrics_obj else existing_metrics_obj.id

            # Member Replating (conditional) based on existing metric record
            if not metric_id:
                if member.genomeType in [GENOME_TYPE_ARRAY, GENOME_TYPE_WGS] and row_copy['contamination_category'] in [
                    GenomicContaminationCategory.EXTRACT_WGS,
                        GenomicContaminationCategory.EXTRACT_BOTH]:
                    self.insert_member_for_replating(member, row_copy['contamination_category'])

            # convert enum to int for json payload
            row_copy['contamination_category'] = int(row_copy['contamination_category'])
            self.controller.execute_cloud_task({
                'metric_id': metric_id,
                'payload_dict': row_copy,
            }, 'genomic_gc_metrics_upsert')

            # MANIFEST/FEEDBACK actions
            manifest_file = self.file_processed_dao.get(member.aw1FileProcessedId)
            if manifest_file is not None and metric_id is None:
                self.feedback_dao.increment_feedback_count(manifest_file.genomicManifestFileId)

            # MEMBER actions
            self.update_member_for_aw2(member)
            member_dict = {
                'id': member.id
            }
            if row_copy['genometype'] == GENOME_TYPE_ARRAY:
                member_dict['genomicWorkflowState'] = int(GenomicWorkflowState.GEM_READY)
                member_dict['genomicWorkflowStateStr'] = str(GenomicWorkflowState.GEM_READY)
                member_dict['genomicWorkflowStateModifiedTime'] = clock.CLOCK.now()
            elif row_copy['genometype'] == GENOME_TYPE_WGS:
                member_dict['genomicWorkflowState'] = int(GenomicWorkflowState.CVL_READY)
                member_dict['genomicWorkflowStateStr'] = str(GenomicWorkflowState.CVL_READY)
                member_dict['genomicWorkflowStateModifiedTime'] = clock.CLOCK.now()
            members_to_update.append(member_dict)

        if members_to_update:
            self.member_dao.bulk_update(members_to_update)

        return GenomicSubProcessResult.SUCCESS

    def copy_member_for_replating(
        self,
        member,
        genome_type=None,
        set_id=None,
        block_research_reason=None,
        block_results_reason=None
    ):
        """
        Inserts a new member record for replating.
        :param member: GenomicSetMember
        :param genome_type:
        :param set_id:
        :param block_research_reason:
        :param block_results_reason:
        :return:
        """
        new_member = GenomicSetMember(
            biobankId=member.biobankId,
            genomicSetId=set_id if set_id else member.genomicSetId,
            participantId=member.participantId,
            nyFlag=member.nyFlag,
            sexAtBirth=member.sexAtBirth,
            validationStatus=member.validationStatus,
            validationFlags=member.validationFlags,
            ai_an=member.ai_an,
            genomeType=genome_type if genome_type else member.genomeType,
            collectionTubeId=f'replated_{member.id}',
            genomicWorkflowState=GenomicWorkflowState.EXTRACT_REQUESTED,
            replatedMemberId=member.id,
            participantOrigin=member.participantOrigin,
            blockResearch=1 if block_research_reason else 0,
            blockResearchReason=block_research_reason if block_research_reason else None,
            blockResults=1 if block_results_reason else 0,
            blockResultsReason=block_results_reason if block_results_reason else None
        )

        self.member_dao.insert(new_member)

    def insert_member_for_replating(self, member, category):
        """
        Inserts a new member record for replating.
        :param member: GenomicSetMember
        :param category: GenomicContaminationCategory
        :return:
        """
        new_member_wgs = GenomicSetMember(
            biobankId=member.biobankId,
            genomicSetId=member.genomicSetId,
            participantId=member.participantId,
            nyFlag=member.nyFlag,
            sexAtBirth=member.sexAtBirth,
            validationStatus=member.validationStatus,
            validationFlags=member.validationFlags,
            collectionTubeId=f'replated_{member.id}',
            ai_an=member.ai_an,
            genomeType=GENOME_TYPE_WGS,
            genomicWorkflowState=GenomicWorkflowState.EXTRACT_REQUESTED,
            genomicWorkflowStateStr=GenomicWorkflowState.EXTRACT_REQUESTED.name,
            participantOrigin=member.participantOrigin,
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            replatedMemberId=member.id,
        )

        if category == GenomicContaminationCategory.EXTRACT_BOTH:
            new_member_array = deepcopy(new_member_wgs)
            new_member_array.genomeType = GENOME_TYPE_ARRAY
            self.member_dao.insert(new_member_array)

        self.member_dao.insert(new_member_wgs)

    @staticmethod
    def get_result_module(module_str):
        results_attr_mapping = {
            'hdrv1': ResultsModuleType.HDRV1,
            'pgxv1': ResultsModuleType.PGXV1,
        }
        return results_attr_mapping.get(module_str)

    def _base_cvl_ingestion(self, **kwargs):
        row_copy = self._clean_row_keys(kwargs.get('row'))
        biobank_id = row_copy.get('biobankid')
        sample_id = row_copy.get('sampleid')

        if not (biobank_id and sample_id):
            return row_copy, None

        biobank_id = self._clean_alpha_values(biobank_id)

        member = self.member_dao.get_member_from_biobank_id_and_sample_id(
            biobank_id,
            sample_id
        )

        if not member:
            logging.warning(f'Can not find genomic member record for biobank_id: '
                            f'{biobank_id} and sample_id: {sample_id}, skipping...')
            return row_copy, None

        setattr(member, kwargs.get('run_attr'), self.job_run_id)
        self.member_dao.update(member)

        # result workflow state
        if kwargs.get('result_state') and kwargs.get('module_type'):
            self.results_workflow_dao.insert_new_result_record(
                member_id=member.id,
                module_type=kwargs.get('module_type'),
                state=kwargs.get('result_state')
            )

        return row_copy, member

    def _base_cvl_analysis_ingestion(self, row_copy, member):
        # cvl analysis
        analysis_cols_mapping = {}
        for column in self.analysis_cols:
            col_matched = row_copy.get(self._clean_row_keys(column))
            if col_matched:
                analysis_cols_mapping[column] = self._clean_row_keys(column)

        analysis_obj = self.cvl_analysis_dao.model_type()
        setattr(analysis_obj, 'genomic_set_member_id', member.id)

        for key, val in analysis_cols_mapping.items():
            setattr(analysis_obj, key, row_copy[val])
        self.cvl_analysis_dao.insert(analysis_obj)

    def _ingest_cvl_w2sc_manifest(self, rows):
        """
        Processes the CVL W2SC manifest file data
        :param rows:
        :return: Result Code
        """
        try:
            for row in rows:
                self._base_cvl_ingestion(
                    row=row,
                    run_attr='cvlW2scManifestJobRunID',
                    result_state=ResultsWorkflowState.CVL_W2SC,
                    module_type=ResultsModuleType.HDRV1
                )

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_cvl_w3ns_manifest(self, rows):
        """
        Processes the CVL W3NS manifest file data
        :param rows:
        :return: Result Code
        """
        try:
            for row in rows:
                self._base_cvl_ingestion(
                    row=row,
                    run_attr='cvlW3nsManifestJobRunID',
                    result_state=ResultsWorkflowState.CVL_W3NS,
                    module_type=ResultsModuleType.HDRV1
                )

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_cvl_w3sc_manifest(self, rows):
        """
        Processes the CVL W3SC manifest file data
        :param rows:
        :return: Result Code
        """
        try:
            for row in rows:
                row_copy, member = self._base_cvl_ingestion(
                    row=row,
                    run_attr='cvlW3scManifestJobRunID',
                    result_state=ResultsWorkflowState.CVL_W3SC,
                    module_type=ResultsModuleType.HDRV1
                )
                if not (row_copy and member):
                    continue

                member.cvlSecondaryConfFailure = row_copy['cvlsecondaryconffailure']
                # allows for sample to be resent in subsequent W3SR
                # https://docs.google.com/presentation/d/1QqXCzwz6MGLMhNwuXlV6ieoMLaJYuYai8csxagF_2-E/edit#slide=id.g10f369a487f_0_0
                member.cvlW3srManifestJobRunID = None
                self.member_dao.update(member)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_cvl_w3ss_manifest(self, rows):
        """
        Processes the CVL W3SS manifest file data
        :param rows:
        :return: Result Code
        """
        self.cvl_second_sample_dao = GenomicCVLSecondSampleDao()
        sample_cols = self.cvl_second_sample_dao.model_type.__table__.columns.keys()
        try:
            for row in rows:
                row_copy, member = self._base_cvl_ingestion(
                    row=row,
                    run_attr='cvlW3ssManifestJobRunID',
                    result_state=ResultsWorkflowState.CVL_W3SS,
                    module_type=ResultsModuleType.HDRV1
                )
                if not (row_copy and member):
                    continue

                row_copy = dict(zip([key.replace('/', '').split('(')[0] for key in row_copy],
                                    row_copy.values()))

                # cvl second sample
                second_sample_obj = self.cvl_second_sample_dao.model_type()
                setattr(second_sample_obj, 'genomic_set_member_id', member.id)
                for col in sample_cols:
                    cleaned_col = self._clean_row_keys(col)
                    col_value = row_copy.get(cleaned_col)
                    if col_value:
                        setattr(second_sample_obj, col, col_value)

                self.cvl_second_sample_dao.insert(second_sample_obj)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_cvl_w4wr_manifest(self, rows):
        """
        Processes the CVL W4WR manifest file data
        :param rows:
        :return: Result Code
        """
        run_attr_mapping = {
            'hdrv1': 'cvlW4wrHdrManifestJobRunID',
            'pgxv1': 'cvlW4wrPgxManifestJobRunID'
        }
        run_id, module = None, None
        for result_key in run_attr_mapping.keys():
            if result_key in self.file_obj.fileName.lower():
                run_id = run_attr_mapping[result_key]
                module = self.get_result_module(result_key)
                break
        try:
            for row in rows:
                row_copy, member = self._base_cvl_ingestion(
                                        row=row,
                                        run_attr=run_id,
                                        result_state=ResultsWorkflowState.CVL_W4WR,
                                        module_type=module
                                    )
                if not (row_copy and member):
                    continue

                self._base_cvl_analysis_ingestion(row_copy, member)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_cvl_w5nf_manifest(self, rows):
        run_attr_mapping = {
            'hdrv1': 'cvlW5nfHdrManifestJobRunID',
            'pgxv1': 'cvlW5nfPgxManifestJobRunID'
        }
        run_id, module = None, None
        for result_key in run_attr_mapping.keys():
            if result_key in self.file_obj.fileName.lower():
                run_id = run_attr_mapping[result_key]
                module = self.get_result_module(result_key)
                break
        try:
            for row in rows:
                row_copy, member = self._base_cvl_ingestion(
                                        row=row,
                                        run_attr=run_id,
                                        result_state=ResultsWorkflowState.CVL_W5NF,
                                        module_type=module,
                                    )
                if not (row_copy and member):
                    continue

                current_analysis = self.cvl_analysis_dao.get_passed_analysis_member_module(
                    member.id,
                    module
                )
                # should have initial record
                if current_analysis:
                    current_analysis.failed = 1
                    current_analysis.failed_request_reason = row_copy['requestreason']
                    current_analysis.failed_request_reason_free = row_copy['requestreasonfree'][0:512]
                    self.cvl_analysis_dao.update(current_analysis)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_aw5_manifest(self, rows):
        try:
            for row in rows:
                row_copy = self._clean_row_keys(row)

                biobank_id = row_copy['biobankid']
                biobank_id = self._clean_alpha_values(biobank_id)
                sample_id = row_copy['sampleid']

                member = self.member_dao.get_member_from_biobank_id_and_sample_id(biobank_id, sample_id)
                if not member:
                    logging.warning(f'Can not find genomic member record for biobank_id: '
                                    f'{biobank_id} and sample_id: {sample_id}, skipping...')
                    continue

                existing_metrics_obj = self.metrics_dao.get_metrics_by_member_id(member.id)
                if existing_metrics_obj is not None:
                    metric_id = existing_metrics_obj.id
                else:
                    logging.warning(f'Can not find metrics record for member id: '
                                    f'{member.id}, skipping...')
                    continue

                self.metrics_dao.update_gc_validation_metrics_deleted_flags_from_dict(row_copy, metric_id)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_aw1c_manifest(self, rows):
        """
        Processes the CVL AW1C manifest file data
        :param rows:
        :return: Result Code
        """
        try:
            for row in rows:
                row_copy = self._clean_row_keys(row)

                collection_tube_id = row_copy['collectiontubeid']
                member = self.member_dao.get_member_from_collection_tube(collection_tube_id, GENOME_TYPE_WGS)

                if member is None:
                    # Currently ignoring invalid cases
                    logging.warning(f'Invalid collection tube ID: {collection_tube_id}')
                    continue

                # Update the AW1C job run ID and genome_type
                member.cvlAW1CManifestJobRunID = self.job_run_id
                member.genomeType = row_copy['genometype']

                # Handle genomic state
                _signal = "aw1c-reconciled"

                if row_copy['failuremode'] not in (None, ''):
                    member.gcManifestFailureMode = row_copy['failuremode']
                    member.gcManifestFailureDescription = row_copy['failuremodedesc']
                    _signal = 'aw1c-failed'

                # update state and state modifed time only if changed
                if member.genomicWorkflowState != GenomicStateHandler.get_new_state(
                    member.genomicWorkflowState, signal=_signal):
                    member.genomicWorkflowState = GenomicStateHandler.get_new_state(
                        member.genomicWorkflowState,
                        signal=_signal)

                    member.genomicWorkflowStateStr = member.genomicWorkflowState.name
                    member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

                self.member_dao.update(member)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _get_site_from_aw1(self):
        """
        Returns the Genomic Center's site ID from the AW1 filename
        :return: GC site ID string
        """
        return self.file_obj.fileName.split('/')[-1].split("_")[0].lower()

    def _validate_collection_tube_id(self, collection_tube_id, bid):
        """
        Returns true if biobank_ID is associated to biobank_stored_sample_id
        (collection_tube_id)
        :param collection_tube_id:
        :param bid:
        :return: boolean
        """
        sample = self.sample_dao.get(collection_tube_id)

        if sample:
            return int(sample.biobankId) == int(bid)

        return False

    @staticmethod
    def _get_qc_status_from_value(aw4_value):
        """
        Returns the GenomicQcStatus enum value for
        :param aw4_value: string from AW4 file (PASS/FAIL)
        :return: GenomicQcStatus
        """
        if aw4_value.strip().lower() == 'pass':
            return GenomicQcStatus.PASS
        elif aw4_value.strip().lower() == 'fail':
            return GenomicQcStatus.FAIL
        else:
            logging.warning(f'Value from AW4 "{aw4_value}" is not PASS/FAIL.')
            return GenomicQcStatus.UNSET

    def create_new_member_from_aw1_control_sample(self, aw1_data: dict) -> GenomicSetMember:
        """
        Creates a new control sample GenomicSetMember in RDR based on AW1 data
        These will look like regular GenomicSetMember samples
        :param aw1_data: dict from aw1 row
        :return:  GenomicSetMember
        """

        # Writing new genomic_set_member based on AW1 data
        max_set_id = self.member_dao.get_collection_tube_max_set_id()[0]
        # Insert new member with biobank_id and collection tube ID from AW1
        new_member_obj = GenomicSetMember(
            genomicSetId=max_set_id,
            participantId=0,
            biobankId=aw1_data['biobankid'],
            collectionTubeId=aw1_data['collectiontubeid'],
            validationStatus=GenomicSetMemberStatus.VALID,
            genomeType=aw1_data['genometype'],
            genomicWorkflowState=GenomicWorkflowState.AW1,
            genomicWorkflowStateStr=GenomicWorkflowState.AW1.name
        )

        # Set member attribures from AW1
        new_member_obj = self._set_member_attributes_from_aw1(aw1_data, new_member_obj)
        new_member_obj = self._set_rdr_member_attributes_for_aw1(aw1_data, new_member_obj)

        return self.member_dao.insert(new_member_obj)

    @staticmethod
    def _participant_has_potentially_clean_samples(session, biobank_id):
        """Check for any stored sample for the participant that is not contaminated
        and is a 1ED04, 1ED10, or 1SAL2 test"""
        query = session.query(BiobankStoredSample).filter(
            BiobankStoredSample.biobankId == biobank_id,
            BiobankStoredSample.status < SampleStatus.SAMPLE_NOT_RECEIVED
        ).outerjoin(GenomicSampleContamination).filter(
            GenomicSampleContamination.id.is_(None),
            BiobankStoredSample.test.in_(['1ED04', '1ED10', '1SAL2'])
        )

        exists_query = session.query(query.exists())
        return exists_query.scalar()

    def _record_sample_as_contaminated(self, session, sample_id):
        session.add(GenomicSampleContamination(
            sampleId=sample_id,
            failedInJob=self.job_id
        ))

    def calculate_contamination_category(self, sample_id, raw_contamination, member: GenomicSetMember):
        """
        Takes contamination value from AW2 and calculates GenomicContaminationCategory
        :param sample_id:
        :param raw_contamination:
        :param member:
        :return: GenomicContaminationCategory
        """
        ps_dao = ParticipantSummaryDao()
        ps = ps_dao.get(member.participantId)

        contamination_category = GenomicContaminationCategory.UNSET

        # No Extract if contamination <1%
        if raw_contamination < 0.01:
            contamination_category = GenomicContaminationCategory.NO_EXTRACT

        # Only extract WGS if contamination between 1 and 3 % inclusive AND ROR
        elif (0.01 <= raw_contamination <= 0.03) and ps.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED:
            contamination_category = GenomicContaminationCategory.EXTRACT_WGS

        # No Extract if contamination between 1 and 3 % inclusive and GROR is not Yes
        elif (0.01 <= raw_contamination <= 0.03) and ps.consentForGenomicsROR != QuestionnaireStatus.SUBMITTED:
            contamination_category = GenomicContaminationCategory.NO_EXTRACT

        # Extract Both if contamination > 3%
        elif raw_contamination > 0.03:
            contamination_category = GenomicContaminationCategory.EXTRACT_BOTH

        with ps_dao.session() as session:
            if raw_contamination >= 0.01:
                # Record in the contamination table, regardless of GROR consent
                self._record_sample_as_contaminated(session, sample_id)

            if contamination_category != GenomicContaminationCategory.NO_EXTRACT and \
                not self._participant_has_potentially_clean_samples(session, member.biobankId):
                contamination_category = GenomicContaminationCategory.TERMINAL_NO_EXTRACT

        return contamination_category

    def _set_metrics_array_data_file_paths(self, row: dict) -> dict:
        gc_site_bucket_map = config.getSettingJson(config.GENOMIC_GC_SITE_BUCKET_MAP, {})
        site_id = self.file_obj.fileName.split('_')[0].lower()
        gc_bucket_name = gc_site_bucket_map.get(site_id)
        gc_bucket = config.getSetting(gc_bucket_name, None)
        if not gc_bucket:
            return row

        for file_def in array_file_types_attributes:
            if file_def['required']:
                if 'idat' in file_def["file_type"]:
                    file_path = f'gs://{gc_bucket}/Genotyping_sample_raw_data/{row["chipwellbarcode"]}' + \
                                f'_{file_def["file_type"]}'
                else:
                    file_path = f'gs://{gc_bucket}/Genotyping_sample_raw_data/{row["chipwellbarcode"]}.' + \
                                f'{file_def["file_type"]}'
                row[file_def['file_path_attribute']] = file_path

        return row

    def _set_metrics_wgs_data_file_paths(self, row: dict) -> dict:
        # IF file_paths in manifest ELSE move on
        required_wgs_file_paths = list(filter(lambda x: x['required'] is True, wgs_file_types_attributes))

        row_paths = {k: v for k, v in row.items() if 'path' in k and v is not None}
        if len(required_wgs_file_paths) == len(row_paths):
            # model attributes are different that manifest columns for certain values in map
            for manifest_file_key, model_attribute in wgs_metrics_manifest_mapping.items():
                path_value = row_paths.get(self._clean_row_keys(manifest_file_key))
                row[model_attribute] = f'gs://{path_value}' if 'gs://' not in path_value else path_value
            return row

        gc_site_bucket_map = config.getSettingJson(config.GENOMIC_GC_SITE_BUCKET_MAP, {})
        site_id = self.file_obj.fileName.split('_')[0].lower()
        gc_bucket_name = gc_site_bucket_map.get(site_id)
        gc_bucket = config.getSetting(gc_bucket_name, None)
        if not gc_bucket:
            return row

        for file_def in required_wgs_file_paths:
            if file_def['required']:
                file_path = f'gs://{gc_bucket}/{genome_center_datafile_prefix_map[site_id][file_def["file_type"]]}/' + \
                            f'{site_id.upper()}_{row["biobankid"]}_{row["sampleid"]}_{row["limsid"]}_1.' + \
                            f'{file_def["file_type"]}'
                row[file_def['file_path_attribute']] = file_path
        return row

    def _update_member_state_after_aw2(self, member: GenomicSetMember):
        if member.genomeType == 'aou_array':
            ready_signal = 'gem-ready'
        elif member.genomeType == 'aou_wgs':
            ready_signal = 'cvl-ready'
        else:
            # Don't update state for investigation genome types
            return
        next_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState, signal=ready_signal)
        if next_state and next_state != member.genomicWorkflowState:
            self.member_dao.update_member_workflow_state(member, next_state)


class GenomicFileValidator:
    """
    This class validates the Genomic Centers files
    """
    GENOME_TYPE_MAPPINGS = {
        'gen': GENOME_TYPE_ARRAY,
        'seq': GENOME_TYPE_WGS,
    }

    def __init__(self, filename=None, data=None, schema=None, job_id=None, controller=None):
        self.filename = filename
        self.data_to_validate = data
        self.valid_schema = schema
        self.job_id = job_id
        self.genome_type = None
        self.controller = controller
        self.gc_site_id = None

        self.GC_METRICS_SCHEMAS = {
            GENOME_TYPE_WGS: (
                "biobankid",
                "sampleid",
                "biobankidsampleid",
                "limsid",
                "meancoverage",
                "genomecoverage",
                "aouhdrcoverage",
                "contamination",
                'samplesource',
                'mappedreadspct',
                "sexconcordance",
                "sexploidy",
                "alignedq30bases",
                "arrayconcordance",
                "processingstatus",
                "notes",
                "genometype"
            ),
            GENOME_TYPE_ARRAY: (
                "biobankid",
                "sampleid",
                "biobankidsampleid",
                "limsid",
                "chipwellbarcode",
                "callrate",
                "sexconcordance",
                "contamination",
                'samplesource',
                "processingstatus",
                "notes",
                "pipelineid",
                "genometype"
            ),
        }

        self.VALID_CVL_FACILITIES = ('rdr', 'co', 'uw', 'bcm')
        self.CVL_ANALYSIS_TYPES = ('hdrv1', 'pgxv1')
        self.VALID_GENOME_CENTERS = ('uw', 'bam', 'bcm', 'bi', 'jh', 'rdr')
        self.DRC_BROAD = 'drc_broad'

        self.AW1_MANIFEST_SCHEMA = (
            "packageid",
            "biobankidsampleid",
            "boxstorageunitid",
            "boxid/plateid",
            "wellposition",
            "sampleid",
            "parentsampleid",
            "collectiontubeid",
            "matrixid",
            "collectiondate",
            "biobankid",
            "sexatbirth",
            "age",
            "nystate(y/n)",
            "sampletype",
            "treatments",
            "quantity(ul)",
            "totalconcentration(ng/ul)",
            "totaldna(ng)",
            "visitdescription",
            "samplesource",
            "study",
            "trackingnumber",
            "contact",
            "email",
            "studypi",
            "sitename",
            "genometype",
            "failuremode",
            "failuremodedesc"
        )

        self.GEM_A2_SCHEMA = (
            "biobankid",
            "sampleid",
            "success",
            "dateofimport",
        )

        self.GEM_METRICS_SCHEMA = (
            "biobankid",
            "sampleid",
            "ancestryloopresponse",
            "availableresults",
            "resultsreleasedat",
        )

        self.CVL_W2_SCHEMA = (
            "genomicsetname",
            "biobankid",
            "sexatbirth",
            "nyflag",
            "siteid",
            "secondaryvalidation",
            "datesubmitted",
            "testname",
        )

        self.CVL_W2SC_SCHEMA = (
            "biobankid",
            "sampleid",
        )

        self.CVL_W3NS_SCHEMA = (
            "biobankid",
            "sampleid",
            "unavailablereason"
        )

        self.CVL_W3SC_SCHEMA = (
            "biobankid",
            "sampleid",
            "cvlsecondaryconffailure"
        )

        self.CVL_W3SS_SCHEMA = (
            "biobankid",
            "sampleid",
            "packageid",
            "version",
            "boxstorageunitid",
            "boxid/plateid",
            "wellposition",
            "cvlsampleid",
            "parentsampleid",
            "collectiontubeid",
            "matrixid",
            "collectiondate",
            "sexatbirth",
            "age",
            "nystate(y/n)",
            "sampletype",
            "treatments",
            "quantity(ul)",
            "totalconcentration(ng/ul)",
            "totaldna(ng)",
            "visitdescription",
            "samplesource",
            "study",
            "trackingnumber",
            "contact",
            "email",
            "studypi",
            "sitename",
            "genometype",
            "failuremode",
            "failuremodedesc"
        )

        self.CVL_W4WR_SCHEMA = (
            "biobankid",
            "sampleid",
            "healthrelateddatafilename",
            "clinicalanalysistype"
        )

        self.CVL_W5NF_SCHEMA = (
            "biobankid",
            "sampleid",
            "requestreason",
            "requestreasonfree",
            "healthrelateddatafilename",
            "clinicalanalysistype"
        )

        self.AW4_ARRAY_SCHEMA = (
            "biobankid",
            "sampleid",
            "sexatbirth",
            "siteid",
            "redidatpath",
            "redidatmd5path",
            "greenidatpath",
            "greenidatmd5path",
            "vcfpath",
            "vcfindexpath",
            "researchid",
            "qcstatus",
            "drcsexconcordance",
            "drccallrate",
            "passtoresearchpipeline"
        )

        self.AW4_WGS_SCHEMA = (
            "biobankid",
            "sampleid",
            "sexatbirth",
            "siteid",
            "vcfhfpath",
            "vcfhfmd5path",
            "vcfhfindexpath",
            "vcfrawpath",
            "vcfrawmd5path",
            "vcfrawindexpath",
            "crampath",
            "crammd5path",
            "craipath",
            "gvcfpath",
            "gvcfmd5path",
            "researchid",
            "qcstatus",
            "drcsexconcordance",
            "drccontamination",
            "drcmeancoverage",
            "drcfpconcordance",
            "passtoresearchpipeline",
            "pipelineid",
            "processingcount"
        )

        self.AW5_WGS_SCHEMA = {
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "sexatbirth",
            "siteid",
            "vcfhf",
            "vcfhfindex",
            "vcfhfmd5",
            "vcfhfbasename",
            "vcfhfmd5hash",
            "vcfraw",
            "vcfrawindex",
            "vcfrawmd5",
            "vcfrawbasename",
            "vcfrawmd5hash",
            "cram",
            "crammd5",
            "crai",
            "crambasename",
            "crammd5hash",
            "gvcf",
            "gvcfmd5",
            "gvcfbasename",
            "gvcfmd5hash",
        }

        self.AW5_ARRAY_SCHEMA = {
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "sexatbirth",
            "siteid",
            "redidat",
            "redidatmd5",
            "redidatbasename",
            "redidatmd5hash",
            "greenidat",
            "greenidatbasename",
            "greenidatmd5hash",
            "greenidatmd5",
            "vcf",
            "vcfindex",
            "vcfmd5",
            "vcfbasename",
            "vcfmd5hash",
        }

        self.values_for_validation = {
            GenomicJob.METRICS_INGESTION: {
                GENOME_TYPE_ARRAY: {
                    'pipelineid': [
                        'cidr_egt_1',
                        'original_egt'
                    ]
                },
            },
        }

    def set_genome_type(self):
        if self.job_id in [GenomicJob.METRICS_INGESTION] and self.filename:
            file_type = self.filename.lower().split("_")[2]
            self.genome_type = self.GENOME_TYPE_MAPPINGS[file_type]

    def set_gc_site_id(self, fn_component):
        if fn_component and \
            fn_component.lower() in self.VALID_GENOME_CENTERS and \
            self.job_id in [
                GenomicJob.METRICS_INGESTION,
                GenomicJob.AW1_MANIFEST,
                GenomicJob.AW1C_INGEST,
                GenomicJob.AW1CF_INGEST,
                GenomicJob.AW1F_MANIFEST
        ]:
            self.gc_site_id = fn_component
        elif self.job_id in [
            GenomicJob.AW4_ARRAY_WORKFLOW,
            GenomicJob.AW4_WGS_WORKFLOW,
            GenomicJob.AW5_ARRAY_MANIFEST,
            GenomicJob.AW5_WGS_MANIFEST
        ]:
            self.gc_site_id = self.DRC_BROAD

    def validate_ingestion_file(self, *, filename, data_to_validate):
        """
        Procedure to validate an ingestion file
        :param filename:
        :param data_to_validate:
        :return: result code
        """
        self.filename = filename
        self.set_genome_type()

        file_processed = self.controller. \
            file_processed_dao.get_record_from_filename(filename)

        # validates filenames for each job
        validated_filename = self.validate_filename(filename)
        if not validated_filename:
            self.controller.create_incident(
                source_job_run_id=self.controller.job_run.id,
                source_file_processed_id=file_processed.id,
                code=GenomicIncidentCode.FILE_VALIDATION_INVALID_FILE_NAME.name,
                message=f"{self.job_id.name}: File name {filename.split('/')[1]} has failed validation due to an"
                        f"incorrect file name.",
                slack=True,
                submitted_gc_site_id=self.gc_site_id,
                manifest_file_name=self.filename
            )
            return GenomicSubProcessResult.INVALID_FILE_NAME

        # validates values in fields if specified for job
        values_validation_failed, message = self.validate_values(data_to_validate)
        if values_validation_failed:
            self.controller.create_incident(
                source_job_run_id=self.controller.job_run.id,
                source_file_processed_id=file_processed.id,
                code=GenomicIncidentCode.FILE_VALIDATION_FAILED_VALUES.name,
                message=message,
                slack=True,
                submitted_gc_site_id=self.gc_site_id,
                manifest_file_name=self.filename
            )
            return GenomicSubProcessResult.ERROR

        # validates file structure rules
        struct_valid_result, missing_fields, extra_fields, expected = self._check_file_structure_valid(
            data_to_validate['fieldnames'])

        if not struct_valid_result:
            slack = True
            invalid_message = f"{self.job_id.name}: File structure of {filename} is not valid."
            if extra_fields:
                invalid_message += f" Extra fields: {', '.join(extra_fields)}"
            if missing_fields:
                invalid_message += f" Missing fields: {', '.join(missing_fields)}"
                if len(missing_fields) == len(expected):
                    slack = False
            self.controller.create_incident(
                source_job_run_id=self.controller.job_run.id,
                source_file_processed_id=file_processed.id,
                code=GenomicIncidentCode.FILE_VALIDATION_FAILED_STRUCTURE.name,
                message=invalid_message,
                slack=slack,
                submitted_gc_site_id=self.gc_site_id,
                manifest_file_name=self.filename
            )
            return GenomicSubProcessResult.INVALID_FILE_STRUCTURE

        return GenomicSubProcessResult.SUCCESS

    def validate_filename(self, filename):
        """
        Applies a naming rule to an arbitrary filename
        Naming rules are defined as local functions and
        Mapped to a Genomic Job ID in naming_rules dict.
        :param filename: passed to each name rule as 'fn'
        :return: boolean
        """
        filename_components = [x.lower() for x in filename.split('/')[-1].split("_")]
        self.set_gc_site_id(filename_components[0])

        # Naming Rule Definitions
        def gc_validation_metrics_name_rule():
            """GC metrics file name rule"""
            return (
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in ('seq', 'gen') and
                filename.lower().endswith('csv')
            )

        def bb_to_gc_manifest_name_rule():
            """Biobank to GCs manifest name rule"""
            return (
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in ('seq', 'gen') and
                filename.lower().endswith('csv')
            )

        def aw1f_manifest_name_rule():
            """Biobank to GCs Failure (AW1F) manifest name rule"""
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in ('seq', 'gen') and
                re.search(r"pkg-[0-9]{4}-[0-9]{5,}$",
                          filename_components[3]) is not None and
                filename_components[4] == 'failure.csv' and
                filename.lower().endswith('csv')
            )

        def cvl_w2sc_manifest_name_rule():
            """
            CVL W2SC (secondary confirmation) manifest name rule
            """
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_CVL_FACILITIES and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl' and
                filename_components[3] == 'w2sc' and
                filename.lower().endswith('csv')
            )

        def cvl_w3ns_manifest_name_rule():
            """
            CVL W3NS manifest name rule
            """
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_CVL_FACILITIES and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl' and
                filename_components[3] == 'w3ns' and
                filename.lower().endswith('csv')
            )

        def cvl_w3sc_manifest_name_rule():
            """
            CVL W3SC manifest name rule
            """
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_CVL_FACILITIES and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl' and
                filename_components[3] == 'w3sc' and
                filename.lower().endswith('csv')
            )

        def cvl_w3ss_manifest_name_rule():
            """
            CVL W3SS manifest name rule
            """
            return (
                len(filename_components) == 4 and
                filename_components[0] in self.VALID_CVL_FACILITIES and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl' and
                'pkg' in filename_components[3] and
                filename.lower().endswith('csv')
            )

        def cvl_w4wr_manifest_name_rule():
            """
            CVL W4WR manifest name rule
            """
            return (
                len(filename_components) == 6 and
                filename_components[0] in self.VALID_CVL_FACILITIES and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl' and
                filename_components[3] == 'w4wr' and
                filename_components[4] in
                [k.lower() for k in ResultsModuleType.to_dict().keys()]
                and filename.lower().endswith('csv')
            )

        def cvl_w5nf_manifest_name_rule():
            """
            CVL W5NF manifest name rule
            """
            return (
                len(filename_components) == 7 and
                filename_components[0] in self.VALID_CVL_FACILITIES and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl' and
                filename_components[3] == 'w5nf' and
                filename_components[4] in
                [k.lower() for k in ResultsModuleType.to_dict().keys()]
                and filename.lower().endswith('csv')
            )

        def gem_a2_manifest_name_rule():
            """GEM A2 manifest name rule: i.e. AoU_GEM_A2_manifest_2020-07-11-00-00-00.csv"""
            return (
                len(filename_components) == 5 and
                filename_components[0] == 'aou' and
                filename_components[1] == 'gem' and
                filename_components[2] == 'a2' and
                filename.lower().endswith('csv')
            )

        def gem_metrics_name_rule():
            """GEM Metrics name rule: i.e. AoU_GEM_metrics_aggregate_2020-07-11-00-00-00.csv"""
            return (
                filename_components[0] == 'aou' and
                filename_components[1] == 'gem' and
                filename_components[2] == 'metrics' and
                filename.lower().endswith('csv')
            )

        def aw4_arr_manifest_name_rule():
            """DRC Broad AW4 Array manifest name rule: i.e. AoU_DRCB_GEN_2020-07-11-00-00-00.csv"""
            return (
                filename_components[0] == 'aou' and
                filename_components[1] == 'drcb' and
                filename_components[2] == 'gen' and
                filename.lower().endswith('csv')
            )

        def aw4_wgs_manifest_name_rule():
            """DRC Broad AW4 WGS manifest name rule: i.e. AoU_DRCB_SEQ_2020-07-11-00-00-00.csv"""
            return (
                filename_components[0] == 'aou' and
                filename_components[1] == 'drcb' and
                filename_components[2] == 'seq' and
                filename.lower().endswith('csv')
            )

        def aw5_wgs_manifest_name_rule():
            # don't have name convention right now, if have in the future, add here
            return filename.lower().endswith('csv')

        def aw5_array_manifest_name_rule():
            # don't have name convention right now, if have in the future, add here
            return filename.lower().endswith('csv')

        ingestion_name_rules = {
            GenomicJob.METRICS_INGESTION: gc_validation_metrics_name_rule,
            GenomicJob.AW1_MANIFEST: bb_to_gc_manifest_name_rule,
            GenomicJob.AW1F_MANIFEST: aw1f_manifest_name_rule,
            GenomicJob.GEM_A2_MANIFEST: gem_a2_manifest_name_rule,
            GenomicJob.AW4_ARRAY_WORKFLOW: aw4_arr_manifest_name_rule,
            GenomicJob.AW4_WGS_WORKFLOW: aw4_wgs_manifest_name_rule,
            GenomicJob.GEM_METRICS_INGEST: gem_metrics_name_rule,
            GenomicJob.AW5_WGS_MANIFEST: aw5_wgs_manifest_name_rule,
            GenomicJob.AW5_ARRAY_MANIFEST: aw5_array_manifest_name_rule,
            GenomicJob.CVL_W2SC_WORKFLOW: cvl_w2sc_manifest_name_rule,
            GenomicJob.CVL_W3NS_WORKFLOW: cvl_w3ns_manifest_name_rule,
            GenomicJob.CVL_W3SC_WORKFLOW: cvl_w3sc_manifest_name_rule,
            GenomicJob.CVL_W3SS_WORKFLOW: cvl_w3ss_manifest_name_rule,
            GenomicJob.CVL_W4WR_WORKFLOW: cvl_w4wr_manifest_name_rule,
            GenomicJob.CVL_W5NF_WORKFLOW: cvl_w5nf_manifest_name_rule
        }

        try:
            is_valid_filename = ingestion_name_rules[self.job_id]()
            return is_valid_filename

        except KeyError:
            return GenomicSubProcessResult.ERROR

    def validate_values(self, data):
        is_invalid, message = False, None
        cleaned_fieldnames = [self._clean_field_name(fieldname) for fieldname in data['fieldnames']]

        try:
            if self.genome_type:
                values_to_check = self.values_for_validation[self.job_id][self.genome_type]
            else:
                values_to_check = self.values_for_validation[self.job_id]
        except KeyError:
            return is_invalid, message

        for field_name, field_values in values_to_check.items():
            if field_name not in cleaned_fieldnames:
                continue

            pos = cleaned_fieldnames.index(field_name)
            for row in data['rows']:
                value_check = list(row.values())[pos]
                if value_check not in field_values:
                    message = f"{self.job_id.name}: Value for {data['fieldnames'][pos]} is invalid: {value_check}"
                    is_invalid = True
                    return is_invalid, message

        return is_invalid, message

    @staticmethod
    def _clean_field_name(fieldname):
        return fieldname.lower().replace('\ufeff', '').replace(' ', '').replace('_', '')

    def _check_file_structure_valid(self, fields):
        """
        Validates the structure of the CSV against a defined set of columns.
        :param fields: the data from the CSV file; dictionary per row.
        :return: boolean; True if valid structure, False if not.
        """

        # Adding temporary bypass rule for manifest ingestion validation DA-3072
        if self.job_id in [GenomicJob.METRICS_INGESTION]:
            return True, None, None, self.valid_schema

        missing_fields, extra_fields = None, None

        if not self.valid_schema:
            self.valid_schema = self._set_schema()

        cases = tuple([self._clean_field_name(field) for field in fields])

        all_file_columns_valid = all([c in self.valid_schema for c in cases])
        all_expected_columns_in_file = all([c in cases for c in self.valid_schema])

        if not all_file_columns_valid:
            extra_fields = list(set(cases) - set(self.valid_schema))

        if not all_expected_columns_in_file:
            missing_fields = list(set(self.valid_schema) - set(cases))

        return \
            all([all_file_columns_valid, all_expected_columns_in_file]), \
            missing_fields, \
            extra_fields, \
            self.valid_schema

    def _set_schema(self):
        """
        Sets schema via the job id
        :return: schema_to_validate,
        (tuple from the CSV_SCHEMA or result code of INVALID_FILE_NAME).
        """
        try:
            if self.job_id == GenomicJob.METRICS_INGESTION:
                return self.GC_METRICS_SCHEMAS[self.genome_type]
            if self.job_id == GenomicJob.AW1_MANIFEST:
                return self.AW1_MANIFEST_SCHEMA
            if self.job_id == GenomicJob.GEM_A2_MANIFEST:
                return self.GEM_A2_SCHEMA
            if self.job_id == GenomicJob.AW1F_MANIFEST:
                return self.AW1_MANIFEST_SCHEMA  # AW1F and AW1 use same schema
            if self.job_id == GenomicJob.GEM_METRICS_INGEST:
                return self.GEM_METRICS_SCHEMA
            if self.job_id == GenomicJob.AW4_ARRAY_WORKFLOW:
                return self.AW4_ARRAY_SCHEMA
            if self.job_id == GenomicJob.AW4_WGS_WORKFLOW:
                return self.AW4_WGS_SCHEMA
            if self.job_id in (GenomicJob.AW1C_INGEST, GenomicJob.AW1CF_INGEST):
                return self.AW1_MANIFEST_SCHEMA
            if self.job_id == GenomicJob.AW5_WGS_MANIFEST:
                self.genome_type = self.GENOME_TYPE_MAPPINGS['seq']
                return self.AW5_WGS_SCHEMA
            if self.job_id == GenomicJob.AW5_ARRAY_MANIFEST:
                self.genome_type = self.GENOME_TYPE_MAPPINGS['gen']
                return self.AW5_ARRAY_SCHEMA
            if self.job_id == GenomicJob.CVL_W2SC_WORKFLOW:
                return self.CVL_W2SC_SCHEMA
            if self.job_id == GenomicJob.CVL_W3NS_WORKFLOW:
                return self.CVL_W3NS_SCHEMA
            if self.job_id == GenomicJob.CVL_W3SC_WORKFLOW:
                return self.CVL_W3SC_SCHEMA
            if self.job_id == GenomicJob.CVL_W3SS_WORKFLOW:
                return self.CVL_W3SS_SCHEMA
            if self.job_id == GenomicJob.CVL_W4WR_WORKFLOW:
                return self.CVL_W4WR_SCHEMA
            if self.job_id == GenomicJob.CVL_W5NF_WORKFLOW:
                return self.CVL_W5NF_SCHEMA

        except (IndexError, KeyError):
            return GenomicSubProcessResult.ERROR


class GenomicFileMover:
    """
    This utility class moves files in the bucket by copying into an archive folder
    and deleting the old instance.
    """

    def __init__(self, archive_folder=None):
        self.archive_folder = archive_folder

    def archive_file(self, file_obj=None, file_path=None):
        """
        This method moves a file to an archive
        by copy and delete
        :param file_obj: a genomic_file_processed object to move
        :return:
        """
        source_path = file_obj.filePath if file_obj else file_path
        file_name = source_path.split('/')[-1]
        archive_path = source_path.replace(file_name,
                                           f"{self.archive_folder}/"
                                           f"{file_name}")
        try:
            copy_cloud_file(source_path, archive_path)
            delete_cloud_file(source_path)
        except FileNotFoundError:
            logging.error(f"No file found at '{file_obj.filePath}'")


class GenomicReconciler:
    """ This component handles reconciliation between genomic datasets """

    def __init__(self, run_id, job_id, archive_folder=None, file_mover=None,
                 bucket_name=None, storage_provider=None, controller=None):

        self.run_id = run_id
        self.job_id = job_id
        self.bucket_name = bucket_name
        self.archive_folder = archive_folder
        self.cvl_file_name = None
        self.file_list = None
        self.ready_signal = None

        # Dao components
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.file_dao = GenomicFileProcessedDao()
        self.data_file_dao = GenomicGcDataFileDao()
        self.data_file_missing_dao = GenomicGcDataFileMissingDao()

        # Other components
        self.file_mover = file_mover
        self.storage_provider = storage_provider
        self.controller = controller

    def process_missing_data(self, metric, missing_data_files, genome_type):
        missing_files_config = config.getSettingJson(config.GENOMIC_SKIP_MISSING_FILETYPES, {})
        missing_files_config = missing_files_config.get(genome_type)

        if missing_files_config:
            missing_files_config = list(missing_files_config) if not type(missing_files_config) \
                                                                     is list else missing_files_config

            missing_data_files = [
                x for x in list(missing_data_files) if x not in missing_files_config
            ]

        if missing_data_files:
            file = self.file_dao.get(metric.genomicFileProcessedId)
            member = self.member_dao.get(metric.genomicSetMemberId)

            description = f"{self.job_id.name}: The following AW2 manifests are missing data files."
            description += f"\nGenomic Job Run ID: {self.run_id}"
            file_list = '\n'.join([mf for mf in missing_data_files])
            description += f"\nManifest File: {file.fileName}"
            description += "\nMissing Data File(s):"
            description += f"\n{file_list}"

            self.controller.create_incident(
                source_job_run_id=self.run_id,
                source_file_processed_id=file.id,
                code=GenomicIncidentCode.MISSING_FILES.name,
                message=description,
                genomic_set_member_id=member.id,
                biobank_id=member.biobankId,
                sample_id=member.sampleId if member.sampleId else "",
                collection_tube_id=member.collectionTubeId if member.collectionTubeId else "",
                slack=True
            )

    def generate_cvl_reconciliation_report(self):
        """
        The main method for the CVL Reconciliation report,
        outputs report file to the cvl subfolder and updates
        genomic_set_member
        :return: result code
        """
        members = self.member_dao.get_members_for_cvl_reconciliation()
        if members:
            cvl_subfolder = getSetting(GENOMIC_CVL_RECONCILIATION_REPORT_SUBFOLDER)
            self.cvl_file_name = f"{cvl_subfolder}/cvl_report_{self.run_id}.csv"
            self._write_cvl_report_to_file(members)

            self.controller.execute_cloud_task({
                'member_ids': [m.id for m in members],
                'field': 'reconcileCvlJobRunId',
                'value': self.run_id,
                'is_job_run': True,
            }, 'genomic_set_member_update_task')

            return GenomicSubProcessResult.SUCCESS

        return GenomicSubProcessResult.NO_FILES

    def update_report_states_for_consent_removal(self, workflow_states):
        """
        Updates report states if gror or primary consent is not yes
        :param workflow_states: list of GenomicWorkflowStates
        """
        # Get unconsented members to update
        unconsented_gror_members = self.member_dao.get_unconsented_gror_or_primary(workflow_states)

        # update each member with the new state and withdrawal time
        for member in unconsented_gror_members:
            new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                          signal='unconsented')

            if new_state is not None or new_state != member.genomicWorkflowState:
                self.member_dao.update_member_workflow_state(member, new_state)

                # Handle withdrawal (gror/primary consent) for reportConsentRemovalDate
                removal_date = self.member_dao.get_consent_removal_date(member)
                if removal_date:
                    self.member_dao.update_report_consent_removal_date(member, removal_date)

    def update_report_state_for_reconsent(self, last_run_time):
        """
        This code is not currently executed, the reconsent has been deferred.
        :param last_run_time:
        :return:
        """
        # Get reconsented members to update (consent > last run time of job_id)
        reconsented_gror_members = self.member_dao.get_reconsented_gror_since_date(last_run_time)

        # update each member with the new state
        for member in reconsented_gror_members:
            new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                          signal='reconsented')

            if new_state is not None or new_state != member.genomicWorkflowState:
                self.member_dao.update_member_workflow_state(member, new_state)
                self.member_dao.update_report_consent_removal_date(member, None)

    def _write_cvl_report_to_file(self, members):
        """
        writes data to csv file in bucket
        :param members:
        :return: result code
        """
        try:
            # extract only columns we need
            cvl_columns = ('biobank_id', 'sample_id', 'member_id')
            report_data = ((m.biobankId, m.sampleId, m.id) for m in members)

            # Use SQL exporter
            exporter = SqlExporter(self.bucket_name)
            with exporter.open_cloud_writer(self.cvl_file_name) as writer:
                writer.write_header(cvl_columns)
                writer.write_rows(report_data)
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR


class GenomicBiobankSamplesCoupler:
    """This component creates the source data for Cohot 3:
    new genomic set and members from the biobank samples pipeline.
    Class uses the manifest handler to create and upload a manifest"""

    _SEX_AT_BIRTH_CODES = {
        'male': 'M',
        'female': 'F',
        'none_intersex': 'NA'
    }
    _VALIDATION_FLAGS = (GenomicValidationFlag.INVALID_WITHDRAW_STATUS,
                         GenomicValidationFlag.INVALID_SUSPENSION_STATUS,
                         GenomicValidationFlag.INVALID_CONSENT,
                         GenomicValidationFlag.INVALID_AGE,
                         GenomicValidationFlag.INVALID_SEX_AT_BIRTH)

    _ARRAY_GENOME_TYPE = "aou_array"
    _WGS_GENOME_TYPE = "aou_wgs"
    _LR_GENOME_TYPE = "long_read"
    COHORT_1_ID = "C1"
    COHORT_2_ID = "C2"
    COHORT_3_ID = "C3"

    GenomicSampleMeta = namedtuple("GenomicSampleMeta", ["bids",
                                                         "pids",
                                                         "order_ids",
                                                         "site_ids",
                                                         "state_ids",
                                                         "sample_ids",
                                                         "valid_withdrawal_status",
                                                         "valid_suspension_status",
                                                         "gen_consents",
                                                         "valid_ages",
                                                         "sabs",
                                                         "gror",
                                                         "is_ai_an",
                                                         "origins"])

    def __init__(self, run_id, controller=None):
        self.samples_dao = BiobankStoredSampleDao()
        self.set_dao = GenomicSetDao()
        self.member_dao = GenomicSetMemberDao()
        self.site_dao = SiteDao()
        self.ps_dao = ParticipantSummaryDao()
        self.code_dao = CodeDao()
        self.run_id = run_id
        self.controller = controller
        self.query = GenomicQueryClass()

    def create_new_genomic_participants(self, from_date):
        """
        This method determines which samples to enter into the genomic system
        from Cohort 3 (New Participants).
        Validation is handled in the query that retrieves the newly consented
        participants' samples to process.
        :param: from_date : the date from which to lookup new biobank_ids
        :return: result
        """
        samples = self._get_new_biobank_samples(from_date)

        if samples:
            samples_meta = self.GenomicSampleMeta(*samples)
            return self.process_samples_into_manifest(samples_meta, cohort=self.COHORT_3_ID)

        else:
            logging.info(f'New Participant Workflow: No new samples to process.')
            return GenomicSubProcessResult.NO_FILES

    def create_saliva_genomic_participants(self, local=False, _config=None):
        """
        This method determines which samples to enter into
        the genomic system that are saliva only, via the
        config obj passed in the argument.

        :param: config : options for ror consent type and denoting if sample was generated in-home or in-clinic
        :return: result
        """
        participants = self._get_remaining_saliva_participants(_config)

        if len(participants) > 0:
            return self.create_matrix_and_process_samples(participants, cohort=None, local=local, saliva=True)

        else:
            logging.info(
                f'Saliva Participant Workflow: No participants to process.')
            return GenomicSubProcessResult.NO_FILES

    def create_c2_genomic_participants(self, local=False):
        """
        Creates Cohort 2 Participants in the genomic system.
        Validation is handled in the query.
        Refactored to first pull valid participants, then pull their samples,
        applying the new business logic of prioritizing
        collection date & blood over saliva.

        :return: result
        """

        samples = self._get_remaining_c2_samples()

        if len(samples) > 0:
            samples_meta = self.GenomicSampleMeta(*samples)
            return self.process_samples_into_manifest(samples_meta, cohort=self.COHORT_2_ID, local=local)

        else:
            logging.info(f'Cohort 2 Participant Workflow: No participants to process.')
            return GenomicSubProcessResult.NO_FILES

    def create_c1_genomic_participants(self):
        """
        Creates Cohort 1 Participants in the genomic system using reconsent.
        Validation is handled in the query that retrieves the newly consented
        participants. Only valid participants are currently sent.

        :param: from_date : the date from which to lookup new participants
        :return: result
        """

        samples = self._get_remaining_c1_samples()

        if len(samples) > 0:
            samples_meta = self.GenomicSampleMeta(*samples)
            return self.process_samples_into_manifest(samples_meta, cohort=self.COHORT_1_ID)

        else:
            logging.info(f'Cohort 1 Participant Workflow: No participants to process.')
            return GenomicSubProcessResult.NO_FILES

    def create_long_read_genomic_participants(self, limit=None):
        """
        Create long_read participants that are already in the genomic system,
        based on downstream filters.
        :return:
        """
        participants = self._get_long_read_participants(limit)

        if len(participants) > 0:
            return self.process_genomic_members_into_manifest(
                participants=participants,
                genome_type=self._LR_GENOME_TYPE
            )

        logging.info(f'Long Read Participant Workflow: No participants to process.')
        return GenomicSubProcessResult.NO_FILES

    def process_genomic_members_into_manifest(self, *, participants, genome_type):
        """
        Compiles AW0 Manifest from already submitted genomic members.
        :param participants:
        :param genome_type
        :return:
        """

        new_genomic_set = self._create_new_genomic_set()
        processed_members = []
        count = 0
        # duplicate genomic set members
        with self.member_dao.session() as session:
            for i, participant in enumerate(participants):
                dup_member_obj = GenomicSetMember(
                    biobankId=participant.biobankId,
                    genomicSetId=new_genomic_set.id,
                    participantId=participant.participantId,
                    nyFlag=participant.nyFlag,
                    sexAtBirth=participant.sexAtBirth,
                    collectionTubeId=participant.collectionTubeId,
                    validationStatus=participant.validationStatus,
                    validationFlags=participant.validationFlags,
                    ai_an=participant.ai_an,
                    genomeType=genome_type,
                    genomicWorkflowState=GenomicWorkflowState.LR_PENDING,
                    genomicWorkflowStateStr=GenomicWorkflowState.LR_PENDING.name,
                    participantOrigin=participant.participantOrigin,
                    created=clock.CLOCK.now(),
                    modified=clock.CLOCK.now(),
                )

                processed_members.append(dup_member_obj)
                count = i + 1

                if count % 100 == 0:
                    self.genomic_members_insert(
                        members=processed_members,
                        session=session,
                    )
                    processed_members.clear()

            if count and processed_members:
                self.genomic_members_insert(
                    members=processed_members,
                    session=session,
                )

        return new_genomic_set.id

    def process_samples_into_manifest(self, samples_meta, cohort, saliva=False, local=False):
        """
        Compiles AW0 Manifest from samples list.
        :param samples_meta:
        :param cohort:
        :param saliva:
        :param local: overrides automatic push to bucket
        :return: job result code
        """

        logging.info(f'{self.__class__.__name__}: Processing new biobank_ids {samples_meta.bids}')
        new_genomic_set = self._create_new_genomic_set()

        processed_array_wgs = []
        count = 0
        bids = []
        # Create genomic set members
        with self.member_dao.session() as session:
            for i, bid in enumerate(samples_meta.bids):
                # Don't write participant to table if no sample
                if samples_meta.sample_ids[i] == 0:
                    continue

                logging.info(f'Validating sample: {samples_meta.sample_ids[i]}')
                validation_criteria = (
                    samples_meta.valid_withdrawal_status[i],
                    samples_meta.valid_suspension_status[i],
                    samples_meta.gen_consents[i],
                    samples_meta.valid_ages[i],
                    samples_meta.sabs[i] in self._SEX_AT_BIRTH_CODES.values()
                )
                valid_flags = self._calculate_validation_flags(validation_criteria)
                logging.info(f'Creating genomic set members for PID: {samples_meta.pids[i]}')

                # Get NY flag for collected-site
                if samples_meta.site_ids[i]:
                    _ny_flag = self._get_new_york_flag_from_site(samples_meta.site_ids[i])

                # Get NY flag for mail-kit
                elif samples_meta.state_ids[i]:
                    _ny_flag = self._get_new_york_flag_from_state_id(samples_meta.state_ids[i])

                # default ny flag if no state id
                elif not samples_meta.state_ids[i]:
                    _ny_flag = 0

                else:
                    logging.warning(f'No collection site or mail kit state. Skipping biobank_id: {bid}')
                    continue

                new_array_member_obj = GenomicSetMember(
                    biobankId=bid,
                    genomicSetId=new_genomic_set.id,
                    participantId=samples_meta.pids[i],
                    nyFlag=_ny_flag,
                    sexAtBirth=samples_meta.sabs[i],
                    collectionTubeId=samples_meta.sample_ids[i],
                    validationStatus=(GenomicSetMemberStatus.INVALID if len(valid_flags) > 0
                                      else GenomicSetMemberStatus.VALID),
                    validationFlags=valid_flags,
                    ai_an='Y' if samples_meta.is_ai_an[i] else 'N',
                    genomeType=self._ARRAY_GENOME_TYPE,
                    genomicWorkflowState=GenomicWorkflowState.AW0_READY,
                    genomicWorkflowStateStr=GenomicWorkflowState.AW0_READY.name,
                    participantOrigin=samples_meta.origins[i],
                    created=clock.CLOCK.now(),
                    modified=clock.CLOCK.now(),
                )

                # Also create a WGS member
                new_wgs_member_obj = deepcopy(new_array_member_obj)
                new_wgs_member_obj.genomeType = self._WGS_GENOME_TYPE

                bids.append(bid)
                processed_array_wgs.extend([new_array_member_obj, new_wgs_member_obj])
                count = i + 1

                if count % 1000 == 0:
                    self.genomic_members_insert(
                        members=processed_array_wgs,
                        session=session
                    )
                    processed_array_wgs.clear()
                    bids.clear()

            if count and processed_array_wgs:
                self.genomic_members_insert(
                    members=processed_array_wgs,
                    session=session
                )

        # Create & transfer the Biobank Manifest based on the new genomic set
        try:
            if local:
                return new_genomic_set.id
            else:
                create_and_upload_genomic_biobank_manifest_file(new_genomic_set.id,
                                                                cohort_id=cohort,
                                                                saliva=saliva)

            # Handle Genomic States for manifests
            for member in self.member_dao.get_members_from_set_id(new_genomic_set.id):
                new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                              signal='manifest-generated')

                if new_state is not None or new_state != member.genomicWorkflowState:
                    self.member_dao.update_member_workflow_state(member, new_state)

            logging.info(f'{self.__class__.__name__}: Genomic set members created ')
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR

    def create_matrix_and_process_samples(self, participants, cohort, local, saliva=False):
        """
        Wrapper method for processing participants for C1 and C2 manifests
        :param cohort:
        :param participants:
        :param local:
        :param saliva:
        :return:
        """

        participant_matrix = self.GenomicSampleMeta(*participants)

        for i, _bid in enumerate(participant_matrix.bids):
            logging.info(f'Retrieving samples for PID: f{participant_matrix.pids[i]}')

            blood_sample_data = None
            if not saliva:
                blood_sample_data = self._get_usable_blood_sample(pid=participant_matrix.pids[i],
                                                                  bid=_bid)

            saliva_sample_data = self._get_usable_saliva_sample(pid=participant_matrix.pids[i],
                                                                bid=_bid)

            # Determine which sample ID to use
            sample_data = self._determine_best_sample(blood_sample_data, saliva_sample_data)

            # update the sample id, collected site, and biobank order
            if sample_data is not None:
                participant_matrix.sample_ids[i] = sample_data[0]
                participant_matrix.site_ids[i] = sample_data[1]
                participant_matrix.order_ids[i] = sample_data[2]

            else:
                logging.info(f'No valid samples for pid {participant_matrix.pids[i]}.')

        # insert new members and make the manifest
        return self.process_samples_into_manifest(
            participant_matrix,
            cohort=cohort,
            saliva=saliva,
            local=local
        )

    @staticmethod
    def genomic_members_insert(*, members, session):
        """
        Bulk save of member for genomic_set_member
        batch updating of members
        :param: members
        :param: session
        """
        try:
            session.bulk_save_objects(members)
            session.commit()
        except Exception as e:
            raise Exception("Error occurred on genomic member insert: {0}".format(e))

    def _get_new_biobank_samples(self, from_date):
        """
        Retrieves BiobankStoredSample objects with `rdr_created`
        after the last run of the new participant workflow job.
        The query filters out participants that do not match the
        genomic validation requirements.
        :param: from_date
        :return: list of tuples (bid, pid, biobank_identifier.value, collected_site_id)
        """

        _new_samples_sql = self.query.new_biobank_samples()

        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "ai_param": Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE.__int__(),
            "from_date_param": from_date.strftime("%Y-%m-%d"),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
            "suspension_param": SuspensionStatus.NOT_SUSPENDED.__int__(),
            "cohort_3_param": ParticipantCohort.COHORT_3.__int__(),
            "ignore_param": GenomicWorkflowState.IGNORE.__int__(),
        }

        with self.samples_dao.session() as session:
            result = session.execute(_new_samples_sql, params).fetchall()

        result = self._prioritize_samples_by_participant(result)

        return list(zip(*result))[:-2]  # Slicing to remove the last two columns retrieved for prioritization

    def _prioritize_samples_by_participant(self, sample_results):
        preferred_samples = {}

        for sample in sample_results:
            preferred_sample = sample
            previously_found_sample = preferred_samples.get(sample.participant_id, None)
            if previously_found_sample is not None:
                preferred_sample = self._determine_best_sample(previously_found_sample, sample)

            preferred_samples[sample.participant_id] = preferred_sample

        return [x for x in preferred_samples.values() if x is not None]

    @staticmethod
    def _determine_best_sample(sample_one, sample_two):
        if sample_one is None:
            return sample_two
        if sample_two is None:
            return sample_one

        # Return the usable sample (status less than NOT_RECEIVED) if one is usable and the other isn't
        if sample_one.status < int(SampleStatus.SAMPLE_NOT_RECEIVED) <= sample_two.status:
            return sample_one
        elif sample_two.status < int(SampleStatus.SAMPLE_NOT_RECEIVED) <= sample_two.status:
            return sample_two
        elif sample_one.status >= int(SampleStatus.SAMPLE_NOT_RECEIVED) \
            and sample_two.status >= int(SampleStatus.SAMPLE_NOT_RECEIVED):
            return None

        # Both are usable
        # Return the sample by the priority of the code: 1ED04, then 1ED10, and 1SAL2 last
        test_codes_by_preference = ['1ED04', '1ED10', '1SAL2']  # most desirable first
        samples_by_code = {}
        for sample in [sample_one, sample_two]:
            samples_by_code[sample.test] = sample

        for test_code in test_codes_by_preference:
            if samples_by_code.get(test_code):
                return samples_by_code[test_code]

        logging.error(f'Should have been able to select between '
                      f'{sample_one.biobank_stored_sample_id} and {sample_two.biobank_stored_sample_id}')

    def _get_remaining_c2_samples(self):

        _c2_participant_sql = self.query.remaining_c2_participants()

        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
            "suspension_param": SuspensionStatus.NOT_SUSPENDED.__int__(),
            "cohort_param": ParticipantCohort.COHORT_2.__int__(),
            "ignore_param": GenomicWorkflowState.IGNORE.__int__(),
        }

        with self.samples_dao.session() as session:
            result = session.execute(_c2_participant_sql, params).fetchall()

        result2 = self._prioritize_samples_by_participant(result)

        return list(zip(*result2))[:-2]

    def _get_remaining_c1_samples(self):
        """
        Retrieves C1 participants and validation data.
        """
        _c1_participant_sql = self.query.remaining_c1_samples()

        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
            "suspension_param": SuspensionStatus.NOT_SUSPENDED.__int__(),
            "cohort_param": ParticipantCohort.COHORT_1.__int__(),
            "c1_reconsent_param": COHORT_1_REVIEW_CONSENT_YES_CODE,
            "ignore_param": GenomicWorkflowState.IGNORE.__int__(),
        }

        with self.samples_dao.session() as session:
            result = session.execute(_c1_participant_sql, params).fetchall()

        result = self._prioritize_samples_by_participant(result)

        return list(zip(*result))[:-2]

    def _get_long_read_participants(self, limit=None):
        """
        Retrieves participants based on filters that have
        been denoted to use in the long read pilot program
        """
        with self.member_dao.session() as session:
            gsm_alias = aliased(GenomicSetMember)
            result = session.query(GenomicSetMember).join(
                ParticipantSummary,
                GenomicSetMember.participantId == ParticipantSummary.participantId,
            ).join(
                ParticipantRaceAnswers,
                ParticipantRaceAnswers.participantId == ParticipantSummary.participantId,
            ).join(
                Code,
                ParticipantRaceAnswers.codeId == Code.codeId,
            ).join(
                GenomicGCValidationMetrics,
                GenomicSetMember.id == GenomicGCValidationMetrics.genomicSetMemberId,
            ).outerjoin(
                gsm_alias,
                sqlalchemy.and_(
                    gsm_alias.participantId == ParticipantSummary.participantId,
                    gsm_alias.genomeType == 'long_read'
                )
            ).filter(
                Code.value == 'WhatRaceEthnicity_Black',
                GenomicSetMember.genomeType.in_(['aou_wgs']),
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
                GenomicGCValidationMetrics.ignoreFlag == 0,
                GenomicGCValidationMetrics.contamination <= 0.01,
                ParticipantSummary.participantOrigin == 'vibrent',
                ParticipantSummary.ehrUpdateTime.isnot(None),
                gsm_alias.id.is_(None),
            ).distinct(gsm_alias.biobankId)

            if limit:
                result = result.limit(limit)

        return result.all()

    def _get_usable_blood_sample(self, pid, bid):
        """
        Select 1ED04 or 1ED10 based on max collected date
        :param pid: participant_id
        :param bid: biobank_id
        :return: tuple(blood_collected date, blood sample, blood site, blood order)
        """
        _samples_sql = self.query.usable_blood_sample()

        params = {
            "pid_param": pid,
            "bid_param": bid,
        }

        with self.samples_dao.session() as session:
            result = session.execute(_samples_sql, params).first()

        return result

    def _get_usable_saliva_sample(self, pid, bid):
        """
        Select 1SAL2 based on max collected date
        :param pid: participant_id
        :param bid: biobank_id
        :return: tuple(saliva date, saliva sample, saliva site, saliva order)
        """
        _samples_sql = self.query.usable_saliva_sample()

        params = {
            "pid_param": pid,
            "bid_param": bid,
        }

        with self.samples_dao.session() as session:
            result = session.execute(_samples_sql, params).first()

        return result

    def _get_remaining_saliva_participants(self, _config):

        _saliva_sql = self.query.remaining_saliva_participants(_config)

        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "ai_param": Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE.__int__(),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
            "suspension_param": SuspensionStatus.NOT_SUSPENDED.__int__(),
            "ignore_param": GenomicWorkflowState.IGNORE.__int__(),
        }

        with self.samples_dao.session() as session:
            result = session.execute(_saliva_sql, params).fetchall()

        return list([list(r) for r in zip(*result)])

    def _create_new_genomic_set(self):
        """Inserts a new genomic set for this run"""
        attributes = {
            'genomicSetName': f'new_participant_workflow_{self.run_id}',
            'genomicSetCriteria': '.',
            'genomicSetVersion': 1,
            'genomicSetStatus': GenomicSetStatus.VALID,
        }
        new_set_obj = GenomicSet(**attributes)
        inserted_set = self.set_dao.insert(new_set_obj)

        return inserted_set

    def _create_new_set_member(self, **kwargs):
        """Inserts new GenomicSetMember object"""
        new_member_obj = GenomicSetMember(**kwargs)
        return self.member_dao.insert(new_member_obj)

    def _get_new_york_flag_from_site(self, collected_site_id):
        """
        Looks up whether a collected site's state is NY
        :param collected_site_id: the id of the site
        :return: int (1 or 0 for NY or Not)
        """
        return int(self.site_dao.get(collected_site_id).state == 'NY')

    def _get_new_york_flag_from_state_id(self, state_id):
        """
        Looks up whether a collected site's state is NY
        :param state_id: the code ID for the state
        :return: int (1 or 0 for NY or Not)
        """
        return int(self.code_dao.get(state_id).value.split('_')[1] == 'NY')

    def _calculate_validation_flags(self, validation_criteria):
        """
        Determines validation and flags for genomic sample
        :param validation_criteria:
        :return: list of validation flags
        """
        # Process validation flags for inserting into genomic_set_member
        flags = [flag for (passing, flag) in
                 zip(validation_criteria, self._VALIDATION_FLAGS)
                 if not passing]
        return flags


class ManifestDefinitionProvider:
    """
    Helper class to produce the definitions for each manifest
    """
    # Metadata for the various manifests
    ManifestDef = namedtuple('ManifestDef',
                             ["job_run_field",
                              "source_data",
                              "destination_bucket",
                              "output_filename",
                              "columns",
                              "signal",
                              "query",
                              "params"])

    def __init__(
        self,
        job_run_id=None,
        bucket_name=None,
        genome_type=None,
        cvl_site_id='rdr',
        **kwargs
    ):
        # Attributes
        self.job_run_id = job_run_id
        self.bucket_name = bucket_name
        self.cvl_site_id = cvl_site_id
        self.genome_type = genome_type
        self.kwargs = kwargs.get('kwargs')
        self.query = GenomicQueryClass(
            input_manifest=self.kwargs.get('input_manifest'),
            genome_type=self.genome_type
        )
        self.query_dao = GenomicQueriesDao()

        self.manifest_columns_config = {
            GenomicManifestTypes.GEM_A1: (
                'biobank_id',
                'sample_id',
                "sex_at_birth",
                "consent_for_ror",
                "date_of_consent_for_ror",
                "chipwellbarcode",
                "genome_center",
            ),
            GenomicManifestTypes.GEM_A3: (
                'biobank_id',
                'sample_id',
                'date_of_consent_removal',
            ),
            GenomicManifestTypes.CVL_W1IL_PGX: (
                'biobank_id',
                'sample_id',
                'vcf_raw_path',
                'vcf_raw_index_path',
                'vcf_raw_md5_path',
                'gvcf_path',
                'gvcf_md5_path',
                'cram_name',
                'sex_at_birth',
                'ny_flag',
                'genome_center',
                'consent_for_gror',
                'genome_type',
                'informing_loop_pgx',
                'aou_hdr_coverage',
                'contamination',
                'sex_ploidy'
            ),
            GenomicManifestTypes.CVL_W1IL_HDR: (
                'biobank_id',
                'sample_id',
                'vcf_raw_path',
                'vcf_raw_index_path',
                'vcf_raw_md5_path',
                'gvcf_path',
                'gvcf_md5_path',
                'cram_name',
                'sex_at_birth',
                'ny_flag',
                'genome_center',
                'consent_for_gror',
                'genome_type',
                'informing_loop_hdr',
                'aou_hdr_coverage',
                'contamination',
                'sex_ploidy'
            ),
            GenomicManifestTypes.CVL_W2W: (
                'biobank_id',
                'sample_id',
                'date_of_consent_removal'
            ),
            GenomicManifestTypes.CVL_W3SR: (
                "biobank_id",
                "sample_id",
                "parent_sample_id",
                "collection_tubeid",
                "sex_at_birth",
                "ny_flag",
                "genome_type",
                "site_name",
                "ai_an"
            ),
            GenomicManifestTypes.AW3_ARRAY: (
                "chipwellbarcode",
                "biobank_id",
                "sample_id",
                "biobankidsampleid",
                "sex_at_birth",
                "site_id",
                "red_idat_path",
                "red_idat_md5_path",
                "green_idat_path",
                "green_idat_md5_path",
                "vcf_path",
                "vcf_index_path",
                "vcf_md5_path",
                "callrate",
                "sex_concordance",
                "contamination",
                "processing_status",
                "research_id",
                "sample_source",
                "pipeline_id",
                "ai_an",
                "blocklisted",
                "blocklisted_reason"
            ),
            GenomicManifestTypes.AW3_WGS: (
                "biobank_id",
                "sample_id",
                "biobankidsampleid",
                "sex_at_birth",
                "site_id",
                "vcf_hf_path",
                "vcf_hf_index_path",
                "vcf_hf_md5_path",
                "cram_path",
                "cram_md5_path",
                "crai_path",
                "gvcf_path",
                "gvcf_md5_path",
                "contamination",
                "sex_concordance",
                "processing_status",
                "mean_coverage",
                "research_id",
                "sample_source",
                "mapped_reads_pct",
                "sex_ploidy",
                "ai_an",
                "blocklisted",
                "blocklisted_reason",
                "pipeline_id",
                "processing_count"
            ),
            GenomicManifestTypes.AW2F: (
                "PACKAGE_ID",
                "BIOBANKID_SAMPLEID",
                "BOX_STORAGEUNIT_ID",
                "BOX_ID/PLATE_ID",
                "WELL_POSITION",
                "SAMPLE_ID",
                "PARENT_SAMPLE_ID",
                "COLLECTION_TUBE_ID",
                "MATRIX_ID",
                "COLLECTION_DATE",
                "BIOBANK_ID",
                "SEX_AT_BIRTH",
                "AGE",
                "NY_STATE_(Y/N)",
                "SAMPLE_TYPE",
                "TREATMENTS",
                "QUANTITY_(uL)",
                "TOTAL_CONCENTRATION_(ng/uL)",
                "TOTAL_DNA(ng)",
                "VISIT_DESCRIPTION",
                "SAMPLE_SOURCE",
                "STUDY",
                "TRACKING_NUMBER",
                "CONTACT",
                "EMAIL",
                "STUDY_PI",
                "TEST_NAME",
                "FAILURE_MODE",
                "FAILURE_MODE_DESC",
                "PROCESSING_STATUS",
                "CONTAMINATION",
                "CONTAMINATION_CATEGORY",
                "CONSENT_FOR_ROR",
            ),
        }

    def _get_source_data_query(self, manifest_type):
        """
        Returns the query to use for manifest's source data
        :param manifest_type:
        :return: query object
        """
        return self.query.genomic_data_config.get(manifest_type)

    def get_def(self, manifest_type):
        """
        Returns the manifest definition based on manifest_type
        :param manifest_type:
        :return: ManifestDef()
        """
        now_formatted = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")
        def_config = {
            GenomicManifestTypes.GEM_A1: {
                'job_run_field': 'gemA1ManifestJobRunId',
                'output_filename': f'{GENOMIC_GEM_A1_MANIFEST_SUBFOLDER}/AoU_GEM_A1_manifest_{now_formatted}.csv',
                'signal': 'manifest-generated'
            },
            GenomicManifestTypes.GEM_A3: {
                'job_run_field': 'gemA3ManifestJobRunId',
                'output_filename': f'{GENOMIC_GEM_A3_MANIFEST_SUBFOLDER}/AoU_GEM_A3_manifest_{now_formatted}.csv',
                'signal': 'manifest-generated'
            },
            GenomicManifestTypes.CVL_W1IL_PGX: {
                'job_run_field': 'cvlW1ilPgxJobRunId',
                'output_filename':
                    f'{CVL_W1IL_PGX_MANIFEST_SUBFOLDER}/{self.cvl_site_id.upper()}_AoU_CVL_W1IL_'
                    f'{ResultsModuleType.PGXV1.name}_{now_formatted}.csv',
                'signal': 'manifest-generated',
                'query': self.query_dao.get_data_ready_for_w1il_manifest,
                'params': {
                    'module': 'pgx',
                    'cvl_id': self.cvl_site_id
                }
            },
            GenomicManifestTypes.CVL_W1IL_HDR: {
                'job_run_field': 'cvlW1ilHdrJobRunId',
                'output_filename':
                    f'{CVL_W1IL_HDR_MANIFEST_SUBFOLDER}/{self.cvl_site_id.upper()}_AoU_CVL_W1IL_'
                    f'{ResultsModuleType.HDRV1.name}_{now_formatted}.csv',
                'query': self.query_dao.get_data_ready_for_w1il_manifest,
                'params': {
                    'module': 'hdr',
                    'cvl_id': self.cvl_site_id
                }
            },
            GenomicManifestTypes.CVL_W2W: {
                'job_run_field': 'cvlW2wJobRunId',
                'output_filename':
                    f'{CVL_W2W_MANIFEST_SUBFOLDER}/{self.cvl_site_id.upper()}_AoU_CVL_W2W_{now_formatted}.csv',
                'query': self.query_dao.get_data_ready_for_w2w_manifest,
                'params': {
                    'cvl_id': self.cvl_site_id
                }
            },
            GenomicManifestTypes.CVL_W3SR: {
                'job_run_field': 'cvlW3srManifestJobRunID',
                'output_filename': f'{CVL_W3SR_MANIFEST_SUBFOLDER}/{self.cvl_site_id.upper()}_AoU_CVL_W3SR'
                                   f'_{now_formatted}.csv',
                'query': self.query_dao.get_w3sr_records,
                'params': {
                    'site_id': self.cvl_site_id
                }
            },
            GenomicManifestTypes.AW3_ARRAY: {
                'job_run_field': 'aw3ManifestJobRunID',
                'output_filename': f'{GENOMIC_AW3_ARRAY_SUBFOLDER}/AoU_DRCV_GEN_{now_formatted}.csv',
                'signal': 'bypass',
                'query': self.query_dao.get_aw3_array_records,
                'params': {
                    'genome_type': self.genome_type
                }
            },
            GenomicManifestTypes.AW3_WGS: {
                'job_run_field': 'aw3ManifestJobRunID',
                'output_filename': f'{GENOMIC_AW3_WGS_SUBFOLDER}/AoU_DRCV_SEQ_{now_formatted}.csv',
                'signal': 'bypass',
                'query': self.query_dao.get_aw3_wgs_records,
                'params': {
                    'genome_type': self.genome_type,
                    'pipeline_id': self.kwargs.get('pipeline_id')
                }
            },
            GenomicManifestTypes.AW2F: {
                'job_run_field': 'aw2fManifestJobRunID',
                'output_filename': f'{BIOBANK_AW2F_SUBFOLDER}/GC_AoU_DataType_PKG-YYMM-xxxxxx_contamination.csv',
                'signal': 'bypass'
            }
        }
        def_config = def_config[manifest_type]
        return self.ManifestDef(
            job_run_field=def_config.get('job_run_field'),
            source_data=self._get_source_data_query(manifest_type),
            destination_bucket=f'{self.bucket_name}',
            output_filename=def_config.get('output_filename'),
            columns=self.manifest_columns_config[manifest_type],
            signal=def_config.get('signal'),
            query=def_config.get('query'),
            params=def_config.get('params')
        )


class ManifestCompiler:
    """
    This component compiles Genomic manifests
    based on definitions provided by ManifestDefinitionProvider
    """
    def __init__(
        self,
        run_id=None,
        bucket_name=None,
        max_num=None,
        controller=None
    ):
        self.run_id = run_id
        self.bucket_name = bucket_name
        self.max_num = max_num
        self.controller = controller
        self.output_file_name = None
        self.manifest_def = None
        self.def_provider = None

        # Dao components
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.results_workflow_dao = GenomicResultWorkflowStateDao()

    def generate_and_transfer_manifest(self, manifest_type, genome_type, version=None, **kwargs):
        """
        Main execution method for ManifestCompiler
        :return: result dict:
            "code": (i.e. SUCCESS)
            "feedback_file": None or feedback file record to update,
            "record_count": integer
        """
        self.def_provider = ManifestDefinitionProvider(
            job_run_id=self.run_id,
            bucket_name=self.bucket_name,
            genome_type=genome_type,
            cvl_site_id=self.controller.cvl_site_id,
            kwargs=kwargs
        )

        self.manifest_def = self.def_provider.get_def(manifest_type)
        source_data = self.pull_source_data()

        if not source_data:
            logging.info(f'No records found for manifest type: {manifest_type}.')
            return {
                "code": GenomicSubProcessResult.NO_FILES,
                "record_count": 0,
            }

        validation_failed, message = self._validate_source_data(source_data, manifest_type)
        if validation_failed:
            message = f'{self.controller.job_id.name}: {message}'
            self.controller.create_incident(
                source_job_run_id=self.run_id,
                code=GenomicIncidentCode.MANIFEST_GENERATE_DATA_VALIDATION_FAILED.name,
                slack=True,
                message=message
            )
            raise RuntimeError

        if self.max_num and len(source_data) > self.max_num:
            current_list, count = [], 0

            for obj in source_data:
                current_list.append(obj)
                if len(current_list) == self.max_num:
                    count += 1
                    self.output_file_name = self.manifest_def.output_filename
                    self.output_file_name = f'{self.output_file_name.split(".csv")[0]}_{count}.csv'
                    file_path = f'{self.manifest_def.destination_bucket}/{self.output_file_name}'

                    logging.info(
                        f'Preparing manifest of type {manifest_type}...'
                        f'{file_path}'
                    )

                    self._write_and_upload_manifest(current_list)
                    self.controller.manifests_generated.append({
                        'file_path': file_path,
                        'record_count': len(current_list)
                    })
                    current_list.clear()

            if current_list:
                count += 1
                self.output_file_name = self.manifest_def.output_filename
                self.output_file_name = f'{self.output_file_name.split(".csv")[0]}_{count}.csv'
                file_path = f'{self.manifest_def.destination_bucket}/{self.output_file_name}'

                logging.info(
                    f'Preparing manifest of type {manifest_type}...'
                    f'{file_path}'
                )

                self._write_and_upload_manifest(current_list)
                self.controller.manifests_generated.append({
                    'file_path': file_path,
                    'record_count': len(current_list)
                })

        else:
            self.output_file_name = self.manifest_def.output_filename
            # If the new manifest is a feedback manifest,
            # it will have an input manifest
            if "input_manifest" in kwargs.keys():
                # AW2F manifest file name is based of of AW1
                if manifest_type == GenomicManifestTypes.AW2F:
                    new_name = kwargs['input_manifest'].filePath.split('/')[-1]
                    new_name = new_name.replace('.csv', f'_contamination_{version}.csv')
                    self.output_file_name = self.manifest_def.output_filename.replace(
                        "GC_AoU_DataType_PKG-YYMM-xxxxxx_contamination.csv",
                        f"{new_name}"
                    )

            file_path = f'{self.manifest_def.destination_bucket}/{self.output_file_name}'

            logging.info(
                f'Preparing manifest of type {manifest_type}...'
                f'{file_path}'
            )

            self._write_and_upload_manifest(source_data)
            self.controller.manifests_generated.append({
                'file_path': file_path,
                'record_count': len(source_data)
            })

        for row in source_data:
            sample_id = row.sampleId if hasattr(row, 'sampleId') else row.sample_id
            member = self.member_dao.get_member_from_sample_id(sample_id, genome_type)

            if not member:
                raise NotFound(f"Cannot find genomic set member with sample ID {sample_id}")

            if self.manifest_def.job_run_field:
                self.controller.member_ids_for_update.append(member.id)

            # Handle Genomic States for manifests
            if self.manifest_def.signal != "bypass":
                # genomic workflow state
                new_wf_state = GenomicStateHandler.get_new_state(
                    member.genomicWorkflowState,
                    signal=self.manifest_def.signal
                )
                if new_wf_state or new_wf_state != member.genomicWorkflowState:
                    self.member_dao.update_member_workflow_state(member, new_wf_state)

            # result workflow state
            cvl_manifest_data = CVLManifestData(manifest_type)
            if cvl_manifest_data.is_cvl_manifest:
                self.results_workflow_dao.insert_new_result_record(
                    member_id=member.id,
                    module_type=cvl_manifest_data.module_type,
                    state=cvl_manifest_data.result_state
                )

        # Updates job run field on set member
        if self.controller.member_ids_for_update:
            self.controller.execute_cloud_task({
                'member_ids': list(set(self.controller.member_ids_for_update)),
                'field': self.manifest_def.job_run_field,
                'value': self.run_id,
                'is_job_run': True
            }, 'genomic_set_member_update_task')

        return {
            "code": GenomicSubProcessResult.SUCCESS,
        }

    def pull_source_data(self):
        """
        Runs the source data query
        :return: result set
        """
        if self.manifest_def.query:
            params = self.manifest_def.params or {}
            return self.manifest_def.query(**params)

        with self.member_dao.session() as session:
            return session.execute(self.manifest_def.source_data).fetchall()

    def _validate_source_data(self, data, manifest_type):
        invalid = False
        message = None

        if manifest_type in [
            GenomicManifestTypes.AW3_ARRAY,
            GenomicManifestTypes.AW3_WGS
        ]:
            prefix = get_biobank_id_prefix()
            path_positions = []
            biobank_ids, sample_ids, sex_at_birth = [], [], []

            for i, col in enumerate(self.manifest_def.columns):
                if 'sample_id' in col:
                    sample_ids = [row[i] for row in data]
                if 'biobank_id' in col:
                    biobank_ids = [row[i] for row in data]
                if 'sex_at_birth' in col:
                    sex_at_birth = [row[i] for row in data]
                if '_path' in col:
                    path_positions.append(i)

            needs_prefixes = any(bid for bid in biobank_ids if prefix not in bid)
            if needs_prefixes:
                message = 'Biobank IDs are missing correct prefix'
                invalid = True
                return invalid, message

            biobank_ids.clear()

            dup_sample_ids = {sample_id for sample_id in sample_ids if sample_ids.count(sample_id) > 1}
            if dup_sample_ids:
                message = f'Sample IDs {list(dup_sample_ids)} are not distinct'
                invalid = True
                return invalid, message

            sample_ids.clear()

            invalid_sex_values = any(val for val in sex_at_birth if val not in ['M', 'F', 'NA'])
            if invalid_sex_values:
                message = 'Invalid Sex at Birth values'
                invalid = True
                return invalid, message

            sex_at_birth.clear()

            for row in data:
                for i, val in enumerate(row):
                    if i in path_positions and val:
                        if not val.startswith('gs://') \
                            or (val.startswith('gs://')
                                and len(val.split('gs://')[1].split('/')) < 3):
                            message = f'Path {val} is invalid formatting'
                            invalid = True
                            return invalid, message

        return invalid, message

    def _write_and_upload_manifest(self, source_data):
        """
        writes data to csv file in bucket
        :return: result code
        """
        try:
            # Use SQL exporter
            exporter = SqlExporter(self.bucket_name)
            with exporter.open_cloud_writer(self.output_file_name) as writer:
                writer.write_header(self.manifest_def.columns)
                writer.write_rows(source_data)
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR


class CVLManifestData:
    result_state = None
    module_type = ResultsModuleType.HDRV1
    is_cvl_manifest = True

    def __init__(self, manifest_type: GenomicManifestTypes):
        self.manifest_type = manifest_type
        self.get_is_cvl_manifest()

    def get_is_cvl_manifest(self):
        if 'cvl' not in self.manifest_type.name.lower():
            self.is_cvl_manifest = False
            return

        self.get_module_type()
        self.get_result_state()

    def get_module_type(self) -> ResultsModuleType:
        if 'pgx' in self.manifest_type.name.lower():
            self.module_type = ResultsModuleType.PGXV1
        return self.module_type

    def get_result_state(self) -> ResultsWorkflowState:
        manifest_name = self.manifest_type.name.rsplit('_', 1)[0] \
            if self.manifest_type.name.count('_') > 1 else \
            self.manifest_type.name
        self.result_state = ResultsWorkflowState.lookup_by_name(manifest_name)
        return self.result_state
