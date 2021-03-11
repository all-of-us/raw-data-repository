"""
Component Classes for Genomic Jobs
Components are assembled by the JobController for a particular Genomic Job
"""

import csv
import logging
import re
import pytz
from collections import deque, namedtuple
from copy import deepcopy
from dateutil.parser import parse
import sqlalchemy

from rdr_service import clock
from rdr_service.dao.bq_genomics_dao import bq_genomic_set_member_update, bq_genomic_gc_validation_metrics_update, \
    bq_genomic_set_update, bq_genomic_file_processed_update, \
    bq_genomic_manifest_file_update, bq_genomic_set_member_batch_update
from rdr_service.dao.code_dao import CodeDao
from rdr_service.genomic.genomic_queries import GenomicQueryClass
from rdr_service.genomic.genomic_state_handler import GenomicStateHandler
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.code import Code
from rdr_service.model.participant_summary import ParticipantRaceAnswers, ParticipantSummary
from rdr_service.model.participant import Participant
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.resource.generators.genomics import genomic_set_member_update, genomic_gc_validation_metrics_update, \
    genomic_set_update, genomic_file_processed_update, genomic_manifest_file_update, genomic_set_member_batch_update
from rdr_service.services.jira_utils import JiraTicketHandler
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
    GenomicSampleContamination,
    GenomicFileProcessed,
    GenomicAW1Raw,
    GenomicAW2Raw)
from rdr_service.participant_enums import (
    GenomicSubProcessResult,
    WithdrawalStatus,
    QuestionnaireStatus,
    SampleStatus,
    GenomicSetStatus,
    GenomicManifestTypes,
    GenomicJob,
    GenomicSubProcessStatus,
    Race,
    GenomicValidationFlag,
    GenomicSetMemberStatus,
    SuspensionStatus,
    GenomicWorkflowState,
    ParticipantCohort, GenomicQcStatus, GenomicContaminationCategory, GenomicIncidentCode)
from rdr_service.dao.genomics_dao import (
    GenomicGCValidationMetricsDao,
    GenomicSetMemberDao,
    GenomicFileProcessedDao,
    GenomicSetDao,
    GenomicJobRunDao,
    GenomicManifestFeedbackDao, GenomicManifestFileDao, GenomicAW1RawDao, GenomicAW2RawDao)
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
    CVL_W1_MANIFEST_SUBFOLDER,
    CVL_W3_MANIFEST_SUBFOLDER,
    GENOMIC_GEM_A1_MANIFEST_SUBFOLDER,
    GENOMIC_GEM_A3_MANIFEST_SUBFOLDER,
    GENOME_TYPE_ARRAY,
    GENOME_TYPE_WGS,
    GAE_PROJECT,
    GENOMIC_AW3_ARRAY_SUBFOLDER,
    GENOMIC_AW3_WGS_SUBFOLDER,
    BIOBANK_AW2F_SUBFOLDER,
)
from rdr_service.code_constants import COHORT_1_REVIEW_CONSENT_YES_CODE
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

        # Sub Components
        self.file_validator = GenomicFileValidator(job_id=self.job_id)
        self.file_mover = GenomicFileMover(archive_folder=self.archive_folder_name)
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.member_dao = GenomicSetMemberDao()
        self.job_run_dao = GenomicJobRunDao()
        self.sample_dao = BiobankStoredSampleDao()
        self.feedback_dao = GenomicManifestFeedbackDao()
        self.manifest_dao = GenomicManifestFileDao()

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
                file_path = "/" + self.bucket_name + "/" + file_data[0]
                new_file_record = self.file_processed_dao.insert_file_record(
                    self.job_run_id,
                    file_path,
                    self.bucket_name,
                    file_data[0].split('/')[-1],
                    upload_date=file_data[1],
                    manifest_file_id=_manifest_file_id)

                # For BQ/PDR
                bq_genomic_file_processed_update(new_file_record.id, project_id=self.controller.bq_project_id)
                genomic_file_processed_update(new_file_record.id)

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
                 and self.file_validator.validate_filename(s.name.lower())]

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
            while len(self.file_queue) > 0:
                try:
                    ingestion_result = self._ingest_genomic_file(
                        self.file_queue[0])
                    file_ingested = self.file_queue.popleft()
                    results.append(ingestion_result == GenomicSubProcessResult.SUCCESS)
                    logging.info(f'Ingestion attempt for {file_ingested.fileName}: {ingestion_result}')

                    self.file_processed_dao.update_file_record(
                        file_ingested.id,
                        GenomicSubProcessStatus.COMPLETED,
                        ingestion_result
                    )

                    # For BQ/PDR
                    bq_genomic_file_processed_update(file_ingested.id, self.controller.bq_project_id)
                    genomic_file_processed_update(file_ingested.id)

                except IndexError:
                    logging.info('No files left in file queue.')

            return GenomicSubProcessResult.SUCCESS if all(results) \
                else GenomicSubProcessResult.ERROR

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
            self.file_validator.valid_schema = None
            validation_result = self.file_validator.validate_ingestion_file(
                self.file_obj.fileName, data_to_ingest)

            if validation_result != GenomicSubProcessResult.SUCCESS:
                return validation_result

            if self.job_id in [GenomicJob.AW1_MANIFEST, GenomicJob.AW1F_MANIFEST]:
                gc_site_id = self._get_site_from_aw1()
                return self._ingest_aw1_manifest(data_to_ingest, gc_site_id)

            if self.job_id == GenomicJob.METRICS_INGESTION:
                return self._process_gc_metrics_data_for_insert(data_to_ingest)

            if self.job_id == GenomicJob.GEM_A2_MANIFEST:
                return self._ingest_gem_a2_manifest(data_to_ingest)

            if self.job_id == GenomicJob.GEM_METRICS_INGEST:
                return self._ingest_gem_metrics_manifest(data_to_ingest)

            if self.job_id == GenomicJob.W2_INGEST:
                return self._ingest_cvl_w2_manifest(data_to_ingest)

            if self.job_id in (GenomicJob.AW4_ARRAY_WORKFLOW, GenomicJob.AW4_WGS_WORKFLOW):
                return self._ingest_aw4_manifest(data_to_ingest)

            if self.job_id in [GenomicJob.AW1C_INGEST, GenomicJob.AW1CF_INGEST]:
                return self._ingest_aw1c_manifest(data_to_ingest)

            if self.job_id in [GenomicJob.AW5_ARRAY_MANIFEST, GenomicJob.AW5_WGS_MANIFEST]:
                return self._ingest_aw5_manifest(data_to_ingest)

        else:
            logging.info("No data to ingest.")
            return GenomicSubProcessResult.NO_FILES
        return GenomicSubProcessResult.ERROR

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
            'gcManifestTestName': 'testname',
            'gcManifestFailureMode': 'failuremode',
            'gcManifestFailureDescription': 'failuremodedesc',
        }

    @staticmethod
    def get_aw1_raw_column_mappings():
        return {
            "package_id": "packageid",
            "biobankid_sample_id": "biobankidsampleid",
            "box_storageunit_id": "boxstorageunitid",
            "box_id_plate_id": "boxid/plateid",
            "well_position": "wellposition",
            "sample_id": "sampleid",
            "parent_sample_id": "parentsampleid",
            "collection_tube_id": "collectiontubeid",
            "matrix_id": "matrixid",
            "collection_date": "collectiondate",
            "biobank_id": "biobankid",
            "sex_at_birth": "sexatbirth",
            "age": "age",
            "ny_state": "nystate(y/n)",
            "sample_type": "sampletype",
            "treatments": "treatments",
            "quantity": "quantity(ul)",
            "total_concentration": "totalconcentration(ng/ul)",
            "total_dna": "totaldna(ng)",
            "visit_description": "visitdescription",
            "sample_source": "samplesource",
            "study": "study",
            "tracking_number": "trackingnumber",
            "contact": "contact",
            "email": "email",
            "study_pi": "studypi",
            "test_name": "testname",
            "failure_mode": "failuremode",
            "failure_mode_desc": "failuremodedesc",
        }

    @staticmethod
    def get_aw2_raw_column_mappings():
        return {
            "biobank_id": "biobankid",
            "sample_id": "sampleid",
            "biobankidsampleid": "biobankidsampleid",
            "lims_id": "limsid",
            "mean_coverage": "meancoverage",
            "genome_coverage": "genomecoverage",
            "aouhdr_coverage": "aouhdrcoverage",
            "contamination": "contamination",
            "sex_concordance": "sexconcordance",
            "sex_ploidy": "sexploidy",
            "aligned_q30_bases": "alignedq30bases",
            "array_concordance": "arrayconcordance",
            "processing_status": "processingstatus",
            "notes": "notes",
            "chipwellbarcode": "chipwellbarcode",
            "call_rate": "callrate",
        }

    def _ingest_aw1_manifest(self, data, _site):
        """
        AW1 ingestion method: Updates the GenomicSetMember with AW1 data
        If the row is determined to be a control sample,
        insert a new GenomicSetMember with AW1 data
        :param data:
        :param _site: gc_site ID
        :return: result code
        """
        _state = GenomicWorkflowState.AW0

        for row in data['rows']:
            row_copy = dict(zip([key.lower().replace(' ', '').replace('_', '')
                                 for key in row], row.values()))
            row_copy['site_id'] = _site

            # TODO: Disabling this fix but leaving in
            #  Until verified that this issue has been fixed in manifes
            # Fix for invalid parent sample values
            # try:
            #     parent_sample_id = int(row_copy['parentsampleid'])
            # except ValueError:
            #     parent_sample_id = 0

            # Skip rows if biobank_id is an empty string (row is empty well)
            if row_copy['biobankid'] == "":
                continue

            # Check if this sample has a control sample parent tube
            control_sample_parent = self.member_dao.get_control_sample_parent(
                row_copy['testname'],
                int(row_copy['parentsampleid'])
            )

            if control_sample_parent:
                logging.warning(f"Control sample found: {row_copy['parentsampleid']}")

                # Check if the control sample member exists for this GC, BID, collection tube, and sample ID
                # Since the Biobank is reusing the sample and collection tube IDs (which are supposed to be unique)
                cntrl_sample_member = self.member_dao.get_control_sample_for_gc_and_genome_type(
                    _site,
                    row_copy['testname'],
                    row_copy['biobankid'],
                    row_copy['collectiontubeid'],
                    row_copy['sampleid']
                )

                if not cntrl_sample_member:
                    # Insert new GenomicSetMember record if none exists
                    # for this control sample, genome type, and gc site
                    member = self.create_new_member_from_aw1_control_sample(row_copy)

                    # Update member for PDR
                    bq_genomic_set_member_update(member.id, project_id=self.controller.bq_project_id)
                    genomic_set_member_update(member.id)

                # Skip rest of iteration and go to next row
                continue

            # Find the existing GenomicSetMember
            # Set the member based on collection tube ID
            # row_copy['testname'] is the genome type (i.e. aou_array, aou_wgs)
            member = self.member_dao.get_member_from_collection_tube(row_copy['collectiontubeid'],
                                                                     row_copy['testname'])

            # Since member not found, and not a control sample,
            # check if collection tube id was swapped by Biobank
            if member is None:
                bid = row_copy['biobankid']

                # Strip biobank prefix if it's there
                if bid[0] in [get_biobank_id_prefix(), 'T']:
                    bid = bid[1:]

                member = self.member_dao.get_member_from_biobank_id_in_state(bid,
                                                                             row_copy['testname'],
                                                                             _state)

                # If member found, validate new collection tube ID, set collection tube ID
                if member:
                    if self._validate_collection_tube_id(row_copy['collectiontubeid'], bid):
                        with self.member_dao.session() as session:
                            self._record_sample_as_contaminated(session, member.collectionTubeId)

                        member.collectionTubeId = row_copy['collectiontubeid']

                else:
                    # Couldn't find genomic set member based on either biobank ID or collection tube
                    _message = f"Cannot find genomic set member: " \
                               f"collection_tube_id: {row_copy['collectiontubeid']}, "\
                               f"biobank id: {bid}, "\
                               f"genome type: {row_copy['testname']}"

                    self.controller.create_incident(source_job_run_id=self.job_run_id,
                                                    source_file_processed_id=self.file_obj.id,
                                                    code=GenomicIncidentCode.UNABLE_TO_FIND_MEMBER.name,
                                                    message=_message,
                                                    biobank_id=bid,
                                                    collection_tube_id=row_copy['collectiontubeid'],
                                                    sample_id=row_copy['sampleid'],
                                                    )
                    logging.error(_message)

                    # Skip rest of iteration and continue processing file
                    continue

            # Process the attribute data
            member_changed, member = self._process_aw1_attribute_data(row_copy, member)

            if member_changed:
                self.member_dao.update(member)

                # Update member for PDR
                bq_genomic_set_member_update(member.id, project_id=self.controller.bq_project_id)
                genomic_set_member_update(member.id)

        return GenomicSubProcessResult.SUCCESS

    def load_raw_awn_file(self):
        """
        Loads genomic_aw1_raw/genomic_aw2_raw
        with raw data from aw1/aw2 file
        :return:
        """
        # Set manifest-specific variables
        if self.controller.job_id == GenomicJob.LOAD_AW1_TO_RAW_TABLE:
            dao = GenomicAW1RawDao()
            awn_model = GenomicAW1Raw
            columns = self.get_aw1_raw_column_mappings()

        elif self.controller.job_id == GenomicJob.LOAD_AW2_TO_RAW_TABLE:
            dao = GenomicAW2RawDao()
            awn_model = GenomicAW2Raw
            columns = self.get_aw2_raw_column_mappings()

        else:
            logging.error("Job ID not LOAD_AW1_TO_RAW_TABLE or LOAD_AW2_TO_RAW_TABLE")
            return GenomicSubProcessResult.ERROR

        # look up if any rows exist already for the file
        records = dao.get_from_filepath(self.target_file)

        if records:
            logging.warning(f'File already exists in raw table: {self.target_file}')
            return GenomicSubProcessResult.SUCCESS

        file_data = self._retrieve_data_from_path(self.target_file)

        # Return the error status if there is an error in file_data
        if not isinstance(file_data, dict):
            return file_data

        # Processing raw data in batches
        batch_size = 100
        item_count = 0
        batch = list()

        for row in file_data['rows']:
            # Standardize fields to lower, no underscores or spaces
            row = dict(zip([key.lower().replace(' ', '').replace('_', '')
                            for key in row], row.values()))

            row_obj = self._set_raw_awn_attributes(row, awn_model(), columns)

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
            row = dict(zip([key.lower().replace(' ', '').replace('_', '')
                       for key in row], row.values()))

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
            row = dict(zip([key.lower().replace(' ', '').replace('_', '')
                            for key in row], row.values()))

        # Beging prep aw2 row
        row = self.prep_aw2_row_attributes(row, member)

        if row == GenomicSubProcessResult.ERROR:
            return GenomicSubProcessResult.ERROR

        # check whether metrics object exists for that member
        existing_metrics_obj = self.metrics_dao.get_metrics_by_member_id(member.id)

        if existing_metrics_obj is not None:
            metric_id = existing_metrics_obj.id
        else:
            metric_id = None

        upserted_obj = self.metrics_dao.upsert_gc_validation_metrics_from_dict(row, metric_id)

        # Update GC Metrics for PDR
        if upserted_obj:
            bq_genomic_gc_validation_metrics_update(upserted_obj.id, project_id=self.controller.bq_project_id)
            genomic_gc_validation_metrics_update(upserted_obj.id)

        self.update_member_for_aw2(member)

        # Update member in DB
        self.member_dao.update(member)

        # Update AW1 manifest feedback record count
        if existing_metrics_obj is None and not self.controller.bypass_record_count:
            # For feedback manifest loop
            # Get the genomic_manifest_file
            manifest_file = self.file_processed_dao.get(member.aw1FileProcessedId)
            if manifest_file is not None:
                self.feedback_dao.increment_feedback_count(manifest_file.genomicManifestFileId,
                                                           _project_id=self.controller.bq_project_id)

        return GenomicSubProcessResult.SUCCESS

    def increment_manifest_file_record_count_from_id(self):
        """
        Increments the manifest record count by 1
        """

        manifest_file = self.manifest_dao.get(self.file_obj.genomicManifestFileId)
        manifest_file.recordCount += 1

        with self.manifest_dao.session() as s:
            s.merge(manifest_file)

        bq_genomic_manifest_file_update(manifest_file.id, project_id=self.controller.bq_project_id)
        genomic_manifest_file_update(manifest_file.id)

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

        # Truncate call rate
        try:
            row['callrate'] = row['callrate'][:10]
        except KeyError:
            pass

        # Validate and clean contamination data
        try:
            row['contamination'] = float(row['contamination'])

            # Percentages shouldn't be less than 0
            if row['contamination'] < 0:
                row['contamination'] = 0

        except ValueError:
            logging.error(f'contamination must be a number for sample_id: {row["sampleid"]}')
            return GenomicSubProcessResult.ERROR

        # Calculate contamination_category
        contamination_value = float(row['contamination'])
        category = self.calculate_contamination_category(member.collectionTubeId,
                                                         contamination_value, member)
        row['contamination_category'] = category

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
            member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

        self.member_dao.update(member)

    def _ingest_gem_a2_manifest(self, file_data):
        """
        Processes the GEM A2 manifest file data
        Updates GenomicSetMember object with gem_pass field.
        :return: Result Code
        """
        try:
            for row in file_data['rows']:
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

                    member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

                self.member_dao.update(member)

                # Update member for PDR
                bq_genomic_set_member_update(member.id, project_id=self.controller.bq_project_id)
                genomic_set_member_update(member.id)

            return GenomicSubProcessResult.SUCCESS
        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_gem_metrics_manifest(self, file_data):
        """
        Processes the GEM Metrics manifest file data
        Updates GenomicSetMember object with metrics fields.
        :return: Result Code
        """

        try:
            for row in file_data['rows']:
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

                # Update member for PDR
                bq_genomic_set_member_update(member.id, project_id=self.controller.bq_project_id)
                genomic_set_member_update(member.id)

            return GenomicSubProcessResult.SUCCESS
        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_aw4_manifest(self, file_data):
        """
        Processes the AW4 manifest file data
        :param file_data:
        :return:
        """
        try:
            for row in file_data['rows']:
                row_copy = dict(zip([key.lower().replace(' ', '').replace('_', '')
                                     for key in row], row.values()))
                sample_id = row_copy['sampleid']
                genome_type = GENOME_TYPE_ARRAY \
                    if self.job_id == GenomicJob.AW4_ARRAY_WORKFLOW else GENOME_TYPE_WGS

                member = self.member_dao.get_member_from_aw3_sample(sample_id,
                                                                    genome_type)
                if member is None:
                    logging.warning(f'Invalid sample ID: {sample_id}')
                    continue

                member.aw4ManifestJobRunID = self.job_run_id
                member.qcStatus = self._get_qc_status_from_value(row_copy['qcstatus'])

                metrics = self.metrics_dao.get_metrics_by_member_id(member.id)

                if metrics:
                    metrics.drcSexConcordance = row_copy['drcsexconcordance']
                    metrics.drcContamination = row_copy['drccontamination']

                    if self.job_id == GenomicJob.AW4_ARRAY_WORKFLOW:
                        metrics.drcCallRate = row_copy['drccallrate']

                    elif self.job_id == GenomicJob.AW4_WGS_WORKFLOW:
                        metrics.drcMeanCoverage = row_copy['drcmeancoverage']
                        metrics.drcFpConcordance = row_copy['drcfpconcordance']

                    self.metrics_dao.upsert(metrics)

                self.member_dao.update(member)

                # Update member for PDR
                bq_genomic_set_member_update(member.id, project_id=self.controller.bq_project_id)
                genomic_set_member_update(member.id)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _retrieve_data_from_path(self, path):
        """
        Retrieves the last genomic data file from a bucket
        :param path: The source file to ingest
        :return: CSV data as a dicitonary
        """
        try:
            filename = path.split('/')[2]
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
        state_to_update = GenomicWorkflowState.AW0

        if self.controller.job_id == GenomicJob.AW1F_MANIFEST:
            state_to_update = GenomicWorkflowState.AW1

        if member.genomicWorkflowState == state_to_update:
            _signal = "aw1-reconciled"

            # Set the signal for a failed sample
            if aw1_data['failuremode'] is not None and aw1_data['failuremode'] != '':
                _signal = 'aw1-failed'

            member.genomicWorkflowState = GenomicStateHandler.get_new_state(
                member.genomicWorkflowState,
                signal=_signal)
            member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

        return member

    def _set_raw_awn_attributes(self, awn_data, awn_row_obj, columns):
        """
        Loads GenomicAW1Raw and GenomicAW2Raw attributes from awn_data
        :param awn_data: dict
        :param awn_row_obj: GenomicAW1Raw/GenomicAW2Raw object
        :param mapping_function: function that returns column mappings
        :return: GenomicAW1Raw or GenomicAW2Raw
        """

        awn_row_obj.file_path = self.target_file
        awn_row_obj.created = clock.CLOCK.now()
        awn_row_obj.modified = clock.CLOCK.now()

        for key in columns.keys():
            awn_row_obj.__setattr__(key, awn_data.get(columns[key]))

        return awn_row_obj

    def _process_gc_metrics_data_for_insert(self, data_to_ingest):
        """ Since input files vary in column names,
        this standardizes the field-names before passing to the bulk inserter
        :param data_to_ingest: stream of data in dict format
        :return result code
        """
        # iterate over each row from CSV and insert into gc metrics table
        for row in data_to_ingest['rows']:
            # change all key names to lower
            row_copy = dict(zip([key.lower().replace(' ', '').replace('_', '')
                                 for key in row],
                                row.values()))

            genome_type = self.file_validator.genome_type
            member = self.member_dao.get_member_from_sample_id(int(row_copy['sampleid']),
                                                               genome_type, )

            if member is not None:
                row_copy = self.prep_aw2_row_attributes(row_copy, member)

                # check whether metrics object exists for that member
                existing_metrics_obj = self.metrics_dao.get_metrics_by_member_id(member.id)

                if existing_metrics_obj is not None:

                    if self.controller.skip_updates:
                        # when running tool, updates can be skipped
                        continue

                    else:
                        metric_id = existing_metrics_obj.id
                else:
                    metric_id = None

                upserted_obj = self.metrics_dao.upsert_gc_validation_metrics_from_dict(row_copy, metric_id)

                # Update GC Metrics for PDR
                if upserted_obj:
                    bq_genomic_gc_validation_metrics_update(upserted_obj.id, project_id=self.controller.bq_project_id)
                    genomic_gc_validation_metrics_update(upserted_obj.id)

                self.update_member_for_aw2(member)

                # For feedback manifest loop
                # Get the genomic_manifest_file
                manifest_file = self.file_processed_dao.get(member.aw1FileProcessedId)
                if manifest_file is not None and existing_metrics_obj is None:
                    self.feedback_dao.increment_feedback_count(manifest_file.genomicManifestFileId,
                                                               _project_id=self.controller.bq_project_id)

            else:

                bid = row_copy['biobankid']

                if bid[0] in [get_biobank_id_prefix(), 'T']:
                    bid = bid[1:]

                # Couldn't find genomic set member based on either biobank ID or sample ID
                _message = f"Cannot find genomic set member for bid, sample_id: "\
                           f"{row_copy['biobankid']}, {row_copy['sampleid']}"

                self.controller.create_incident(source_job_run_id=self.job_run_id,
                                                source_file_processed_id=self.file_obj.id,
                                                code=GenomicIncidentCode.UNABLE_TO_FIND_MEMBER.name,
                                                message=_message,
                                                biobank_id=bid,
                                                sample_id=row_copy['sampleid'],
                                                )
                logging.error(_message)

        return GenomicSubProcessResult.SUCCESS

    def _ingest_cvl_w2_manifest(self, file_data):
        """
        Processes the CVL W2 manifest file data
        :return: Result Code
        """
        try:
            for row in file_data['rows']:
                # change all key names to lower
                row_copy = dict(zip([key.lower().replace(' ', '').replace('_', '')
                                     for key in row],
                                    row.values()))

                biobank_id = row_copy['biobankid']
                member = self.member_dao.get_member_from_biobank_id(biobank_id, GENOME_TYPE_WGS)

                if member is None:
                    logging.warning(f'Invalid Biobank ID: {biobank_id}')
                    continue

                member.genomeType = row_copy['testname']
                member.cvlW2ManifestJobRunID = self.job_run_id

                # update state and state modifed time only if changed
                if member.genomicWorkflowState != GenomicStateHandler.get_new_state(
                    member.genomicWorkflowState, signal='w2-ingestion-success'):

                    member.genomicWorkflowState = GenomicStateHandler.get_new_state(
                        member.genomicWorkflowState,
                        signal='w2-ingestion-success')

                    member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

                self.member_dao.update(member)

                # Update member for PDR
                bq_genomic_set_member_update(member.id, project_id=self.controller.bq_project_id)
                genomic_set_member_update(member.id)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_aw5_manifest(self, file_data):
        try:
            for row in file_data['rows']:
                row_copy = dict(zip([key.lower().replace(' ', '').replace('_', '')
                                     for key in row], row.values()))
                biobank_id = row_copy['biobankid']
                biobank_id = biobank_id[1:] if biobank_id[0].isalpha() else biobank_id
                sample_id = row_copy['sampleid']

                member = self.member_dao.get_member_from_biobank_id_and_sample_id(biobank_id, sample_id,
                                                                                  self.file_validator.genome_type)
                if not member:
                    logging.warning(f'can not find genomic member record for biobank_id: '
                                    f'{biobank_id} and sample_id: {sample_id}, skip this one')
                    continue

                existing_metrics_obj = self.metrics_dao.get_metrics_by_member_id(member.id)
                if existing_metrics_obj is not None:
                    metric_id = existing_metrics_obj.id
                else:
                    logging.warning(f'can not find metrics record for member id: '
                                    f'{member.id}, skip this one')
                    continue

                updated_obj = self.metrics_dao.update_gc_validation_metrics_deleted_flags_from_dict(row_copy,
                                                                                                    metric_id)

                # Update GC Metrics for PDR
                if updated_obj:
                    bq_genomic_gc_validation_metrics_update(updated_obj.id, project_id=self.controller.bq_project_id)
                    genomic_gc_validation_metrics_update(updated_obj.id)

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_aw1c_manifest(self, file_data):
        """
        Processes the CVL AW1C manifest file data
        :return: Result Code
        """
        try:
            for row in file_data['rows']:
                row_copy = dict(zip([key.lower().replace(' ', '').replace('_', '')
                                     for key in row], row.values()))
                collection_tube_id = row_copy['collectiontubeid']
                member = self.member_dao.get_member_from_collection_tube(collection_tube_id, GENOME_TYPE_WGS)

                if member is None:
                    # Currently ignoring invalid cases
                    logging.warning(f'Invalid collection tube ID: {collection_tube_id}')
                    continue

                # Update the AW1C job run ID and genome_type
                member.cvlAW1CManifestJobRunID = self.job_run_id
                member.genomeType = row_copy['testname']

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

                    member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

                self.member_dao.update(member)

                # Update member for PDR
                bq_genomic_set_member_update(member.id, project_id=self.controller.bq_project_id)
                genomic_set_member_update(member.id)

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
            genomeType=aw1_data['testname'],
            genomicWorkflowState=GenomicWorkflowState.AW1
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


class GenomicFileValidator:
    """
    This class validates the Genomic Centers files
    """
    GENOME_TYPE_MAPPINGS = {
        'gen': GENOME_TYPE_ARRAY,
        'seq': GENOME_TYPE_WGS,
    }

    def __init__(self, filename=None, data=None, schema=None, job_id=None):
        self.filename = filename
        self.data_to_validate = data
        self.valid_schema = schema
        self.job_id = job_id
        self.genome_type = None

        self.GC_METRICS_SCHEMAS = {
            'seq': (
                "biobankid",
                "sampleid",
                "biobankidsampleid",
                "limsid",
                "meancoverage",
                "genomecoverage",
                "aouhdrcoverage",
                "contamination",
                "sexconcordance",
                "sexploidy",
                "alignedq30bases",
                "arrayconcordance",
                "processingstatus",
                "notes",
            ),
            'gen': (
                "biobankid",
                "sampleid",
                "biobankidsampleid",
                "limsid",
                "chipwellbarcode",
                "callrate",
                "sexconcordance",
                "contamination",
                "processingstatus",
                "notes",
            ),
        }
        self.VALID_GENOME_CENTERS = ('uw', 'bam', 'bcm', 'bi', 'jh', 'rdr')
        self.VALID_CVL_FACILITIES = ('rdr', 'color', 'uw', 'baylor')

        self.GC_MANIFEST_SCHEMA = (
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
            "testname",
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
            "drccontamination",
            "drccallrate",
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
            "researchid",
            "qcstatus",
            "drcsexconcordance",
            "drccontamination",
            "drcmeancoverage",
            "drcfpconcordance",
        )

        self.AW5_WGS_SCHEMA = {
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "sexatbirth",
            "siteid",
            "aw2filename",
            "vcfhf",
            "vcfhfindex",
            "vcfhfmd5",
            "vcfraw",
            "vcfrawindex",
            "vcfrawmd5",
            "cram",
            "crammd5",
            "crai",
            "gvcf",
            "gvcfmd5",
        }

        self.AW5_ARRAY_SCHEMA = {
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "sexatbirth",
            "siteid",
            "aw2filename",
            "redidat",
            "redidatmd5",
            "greenidat",
            "greenidatmd5",
            "vcf",
            "vcfindex",
            "vcfmd5",
        }

    def validate_ingestion_file(self, filename, data_to_validate):
        """
        Procedure to validate an ingestion file
        :param filename:
        :param data_to_validate:
        :return: result code
        """
        self.filename = filename
        if not self.validate_filename(filename):
            return GenomicSubProcessResult.INVALID_FILE_NAME

        struct_valid_result = self._check_file_structure_valid(
            data_to_validate['fieldnames'])

        if struct_valid_result == GenomicSubProcessResult.INVALID_FILE_NAME:
            return GenomicSubProcessResult.INVALID_FILE_NAME

        if not struct_valid_result:
            logging.info("file structure of {} not valid.".format(filename))
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

        # Naming Rule Definitions
        def bb_result_name_rule(fn):
            """Biobank to DRC Result name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("-")]
            return (
                filename_components[0] == 'genomic' and
                filename_components[1] == 'manifest' and
                filename_components[2] in ('aou_array', 'aou_wgs')
            )

        def gc_validation_metrics_name_rule(fn):
            """GC metrics file name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in self.GC_METRICS_SCHEMAS.keys()
            )

        def bb_to_gc_manifest_name_rule(fn):
            """Biobank to GCs manifest name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in ('seq', 'gen')
            )

        def aw1f_manifest_name_rule(fn):
            """Biobank to GCs Failure (AW1F) manifest name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in ('seq', 'gen') and
                re.search(r"pkg-[0-9]{4}-[0-9]{5,}$",
                          filename_components[3]) is not None and
                filename_components[4] == 'failure.csv'
            )

        def cvl_w2_manifest_name_rule(fn):
            """
            CVL W2 (secondary validation) manifest name rule
            UW_AoU_CVL_RequestValidation_Date.csv
            """
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_CVL_FACILITIES and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl' and
                filename_components[3] == 'requestvalidation'
            )

        def gem_a2_manifest_name_rule(fn):
            """GEM A2 manifest name rule: i.e. AoU_GEM_A2_manifest_2020-07-11-00-00-00.csv"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                len(filename_components) == 5 and
                filename_components[0] == 'aou' and
                filename_components[1] == 'gem' and
                filename_components[2] == 'a2'
            )

        def cvl_aw1c_manifest_name_rule(fn):
            """AW1C Biobank to CVLs manifest name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl'
            )

        def cvl_aw1cf_manifest_name_rule(fn):
            """AW1F Biobank to CVLs manifest name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl' and
                filename_components[4] == 'failure.csv'
            )

        def gem_metrics_name_rule(fn):
            """GEM Metrics name rule: i.e. AoU_GEM_metrics_aggregate_2020-07-11-00-00-00.csv"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                filename_components[0] == 'aou' and
                filename_components[1] == 'gem' and
                filename_components[2] == 'metrics'
            )

        def aw4_arr_manifest_name_rule(fn):
            """DRC Broad AW4 Array manifest name rule: i.e. AoU_DRCB_GEN_2020-07-11-00-00-00.csv"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                filename_components[0] == 'aou' and
                filename_components[1] == 'drcb' and
                filename_components[2] == 'gen'
            )

        def aw4_wgs_manifest_name_rule(fn):
            """DRC Broad AW4 WGS manifest name rule: i.e. AoU_DRCB_SEQ_2020-07-11-00-00-00.csv"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                filename_components[0] == 'aou' and
                filename_components[1] == 'drcb' and
                filename_components[2] == 'seq'
            )

        def aw5_wgs_manifest_name_rule(fn):
            # don't have name convention right now, if have in the future, add here
            return fn.lower().endswith('csv')

        def aw5_array_manifest_name_rule(fn):
            # don't have name convention right now, if have in the future, add here
            return fn.lower().endswith('csv')

        name_rules = {
            GenomicJob.BB_RETURN_MANIFEST: bb_result_name_rule,
            GenomicJob.METRICS_INGESTION: gc_validation_metrics_name_rule,
            GenomicJob.AW1_MANIFEST: bb_to_gc_manifest_name_rule,
            GenomicJob.AW1F_MANIFEST: aw1f_manifest_name_rule,
            GenomicJob.GEM_A2_MANIFEST: gem_a2_manifest_name_rule,
            GenomicJob.W2_INGEST: cvl_w2_manifest_name_rule,
            GenomicJob.AW1C_INGEST: cvl_aw1c_manifest_name_rule,
            GenomicJob.AW1CF_INGEST: cvl_aw1cf_manifest_name_rule,
            GenomicJob.AW4_ARRAY_WORKFLOW: aw4_arr_manifest_name_rule,
            GenomicJob.AW4_WGS_WORKFLOW: aw4_wgs_manifest_name_rule,
            GenomicJob.GEM_METRICS_INGEST: gem_metrics_name_rule,
            GenomicJob.AW5_WGS_MANIFEST: aw5_wgs_manifest_name_rule,
            GenomicJob.AW5_ARRAY_MANIFEST: aw5_array_manifest_name_rule,
        }

        return name_rules[self.job_id](filename)

    def _check_file_structure_valid(self, fields):
        """
        Validates the structure of the CSV against a defined set of columns.
        :param fields: the data from the CSV file; dictionary per row.
        :return: boolean; True if valid structure, False if not.
        """
        if not self.valid_schema:
            self.valid_schema = self._set_schema(self.filename)

        if self.valid_schema == GenomicSubProcessResult.INVALID_FILE_NAME:
            return GenomicSubProcessResult.INVALID_FILE_NAME

        cases = tuple([field.lower().replace('\ufeff', '').replace(' ', '').replace('_', '')
                       for field in fields])
        all_file_columns_valid = all([c in self.valid_schema for c in cases])
        all_expected_columns_in_file = all([c in cases for c in self.valid_schema])

        return all([all_file_columns_valid, all_expected_columns_in_file])

    def _set_schema(self, filename):
        """Since the schemas are different for WGS and Array metrics files,
        this parses the filename to return which schema
        to use for validation of the CSV columns
        :param filename: filename of the csv to validate in string format.
        :return: schema_to_validate,
            (tuple from the CSV_SCHEMA or result code of INVALID_FILE_NAME).
        """
        try:
            if self.job_id == GenomicJob.METRICS_INGESTION:
                file_type = filename.lower().split("_")[2]
                self.genome_type = self.GENOME_TYPE_MAPPINGS[file_type]
                return self.GC_METRICS_SCHEMAS[file_type]
            if self.job_id == GenomicJob.AW1_MANIFEST:
                return self.GC_MANIFEST_SCHEMA
            if self.job_id == GenomicJob.GEM_A2_MANIFEST:
                return self.GEM_A2_SCHEMA
            if self.job_id == GenomicJob.AW1F_MANIFEST:
                return self.GC_MANIFEST_SCHEMA  # AW1F and AW1 use same schema

            if self.job_id == GenomicJob.GEM_METRICS_INGEST:
                return self.GEM_METRICS_SCHEMA

            if self.job_id == GenomicJob.W2_INGEST:
                return self.CVL_W2_SCHEMA

            if self.job_id == GenomicJob.AW4_ARRAY_WORKFLOW:
                return self.AW4_ARRAY_SCHEMA

            if self.job_id == GenomicJob.AW4_WGS_WORKFLOW:
                return self.AW4_WGS_SCHEMA

            if self.job_id in (GenomicJob.AW1C_INGEST, GenomicJob.AW1CF_INGEST):
                return self.GC_MANIFEST_SCHEMA

            if self.job_id == GenomicJob.AW5_WGS_MANIFEST:
                self.genome_type = self.GENOME_TYPE_MAPPINGS['seq']
                return self.AW5_WGS_SCHEMA

            if self.job_id == GenomicJob.AW5_ARRAY_MANIFEST:
                self.genome_type = self.GENOME_TYPE_MAPPINGS['gen']
                return self.AW5_ARRAY_SCHEMA

        except (IndexError, KeyError):
            return GenomicSubProcessResult.INVALID_FILE_NAME


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

        # Dao components
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.file_dao = GenomicFileProcessedDao()

        # Other components
        self.file_mover = file_mover
        self.storage_provider = storage_provider
        self.controller = controller

        # Data files and names will be different
        # file types are defined as
        # (field_for_received_flag, filename suffix, field_for_gcs_path)
        self.genotyping_file_types = (('idatRedReceived', "_red.idat", "idatRedPath"),
                                      ('idatGreenReceived', "_grn.idat", "idatGreenPath"),
                                      ('idatRedMd5Received', "_red.idat.md5sum", "idatRedMd5Path"),
                                      ('idatGreenMd5Received', "_grn.idat.md5sum", "idatGreenMd5Path"),
                                      ('vcfReceived', ".vcf.gz", "vcfPath"),
                                      ('vcfTbiReceived', ".vcf.gz.tbi", "vcfTbiPath"),
                                      ('vcfMd5Received', ".vcf.gz.md5sum", "vcfMd5Path"))

        self.sequencing_file_types = (("hfVcfReceived", ".hard-filtered.vcf.gz", "hfVcfPath"),
                                      ("hfVcfTbiReceived", ".hard-filtered.vcf.gz.tbi", "hfVcfTbiPath"),
                                      ("hfVcfMd5Received", ".hard-filtered.vcf.gz.md5sum", "hfVcfMd5Path"),
                                      ("rawVcfReceived", ".vcf.gz", "rawVcfPath"),
                                      ("rawVcfTbiReceived", ".vcf.gz.tbi", "rawVcfTbiPath"),
                                      ("rawVcfMd5Received", ".vcf.gz.md5sum", "rawVcfMd5Path"),
                                      ("cramReceived", ".cram", "cramPath"),
                                      ("cramMd5Received", ".cram.md5sum", "cramMd5Path"),
                                      ("craiReceived", ".cram.crai", "craiPath"))

    def reconcile_metrics_to_array_data(self, _gc_site_id):
        """ The main method for the AW2 manifest vs. array data reconciliation
        :param: _gc_site_id: "jh", "uw", "bi", etc.
        :return: result code
        """
        metrics = self.metrics_dao.get_with_missing_array_files(_gc_site_id)

        total_missing_data = []

        # Get list of files in GC data bucket
        if self.storage_provider:
            # Use the storage provider if it was set by tool
            files = self.storage_provider.list(self.bucket_name, prefix=None)

        else:
            files = list_blobs('/' + self.bucket_name)

        self.file_list = [f.name for f in files]

        # Iterate over metrics, searching the bucket for filenames where *_received = 0
        for metric in metrics:
            member = self.member_dao.get(metric.genomicSetMemberId)

            missing_data_files = []

            metric_touched = False

            for file_type in self.genotyping_file_types:
                if not getattr(metric, file_type[0]):
                    filename = f"{metric.chipwellbarcode}{file_type[1]}"
                    file_exists = self._get_full_filename(filename)

                    if file_exists != 0:
                        setattr(metric, file_type[0], 1)
                        setattr(metric, file_type[2], f'gs://{self.bucket_name}/{file_exists}')
                        metric_touched = True

                    if not file_exists:
                        missing_data_files.append(filename)

            if metric_touched:
                # Only upsert the metric if changed
                inserted_metrics_obj = self.metrics_dao.upsert(metric)

                # Update GC Metrics for PDR
                if inserted_metrics_obj:
                    bq_genomic_gc_validation_metrics_update(inserted_metrics_obj.id,
                                                            project_id=self.controller.bq_project_id)
                    genomic_gc_validation_metrics_update(inserted_metrics_obj.id)

                next_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState, signal='gem-ready')

                # Update Job Run ID on member
                self.member_dao.update_member_job_run_id(member, self.run_id, 'reconcileMetricsSequencingJobRunId',
                                                         project_id=self.controller.bq_project_id)
            else:
                next_state = None

            # Update state for missing files
            if len(missing_data_files) > 0:
                next_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState, signal='missing')

                total_missing_data.append((metric.genomicFileProcessedId, missing_data_files))

            if next_state is not None and next_state != member.genomicWorkflowState:
                self.member_dao.update_member_state(member, next_state, project_id=self.controller.bq_project_id)

        # Make a roc ticket for missing data files
        if len(total_missing_data) > 0:
            alert = GenomicAlertHandler()

            summary = '[Genomic System Alert] Missing AW2 Array Manifest Files'
            description = "The following AW2 manifests are missing data files."
            description += f"\nGenomic Job Run ID: {self.run_id}"

            for f in total_missing_data:

                description += self._compile_missing_data_alert(f[0], f[1])
            alert.make_genomic_alert(summary, description)

        return GenomicSubProcessResult.SUCCESS

    def reconcile_metrics_to_wgs_data(self, _gc_site_id):
        """ The main method for the AW2 manifest vs. sequencing data reconciliation
        :param: _gc_site_id: "jh", "uw", "bi", etc.
        :return: result code
        """
        metrics = self.metrics_dao.get_with_missing_wsg_files(_gc_site_id)

        # Get list of files in GC data bucket
        if self.storage_provider:
            # Use the storage provider if it was set by tool
            files = self.storage_provider.list(self.bucket_name, prefix=None)

        else:
            files = list_blobs('/' + self.bucket_name)

        self.file_list = [f.name for f in files]

        total_missing_data = []

        metric_touched = False

        # Iterate over metrics, searching the bucket for filenames
        for metric in metrics:
            member = self.member_dao.get(metric.GenomicGCValidationMetrics.genomicSetMemberId)

            gc_prefix = _gc_site_id.upper()

            missing_data_files = []
            for file_type in self.sequencing_file_types:

                if not getattr(metric.GenomicGCValidationMetrics, file_type[0]):

                    # Default filename in case the file is missing (used in alert)
                    default_filename = f"{gc_prefix}_{metric.biobankId}_{metric.sampleId}_" \
                                       f"{metric.GenomicGCValidationMetrics.limsId}_1{file_type[1]}"

                    file_type_expression = file_type[1].replace('.', '\.')

                    # Naming rule for WGS files:
                    filename_exp = rf"{gc_prefix}_([A-Z]?){metric.biobankId}_{metric.sampleId}" \
                                   rf"_{metric.GenomicGCValidationMetrics.limsId}_(\w*)(\d+){file_type_expression}$"

                    file_exists = self._get_full_filename_with_expression(filename_exp)

                    if file_exists != 0:
                        setattr(metric.GenomicGCValidationMetrics, file_type[0], 1)
                        setattr(metric.GenomicGCValidationMetrics, file_type[2],
                                f'gs://{self.bucket_name}/{file_exists}')
                        metric_touched = True

                    if not file_exists:
                        missing_data_files.append(default_filename)

            if metric_touched:
                # Only upsert the metric if changed
                inserted_metrics_obj = self.metrics_dao.upsert(metric.GenomicGCValidationMetrics)

                # Update GC Metrics for PDR
                if inserted_metrics_obj:
                    bq_genomic_gc_validation_metrics_update(inserted_metrics_obj.id,
                                                            project_id=self.controller.bq_project_id)
                    genomic_gc_validation_metrics_update(inserted_metrics_obj.id)

                next_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState, signal='cvl-ready')

                self.member_dao.update_member_job_run_id(member, self.run_id, 'reconcileMetricsSequencingJobRunId',
                                                         project_id=self.controller.bq_project_id)

            else:
                next_state = None

            # Handle for missing data files
            if len(missing_data_files) > 0:
                next_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState, signal='missing')

                total_missing_data.append((metric.GenomicGCValidationMetrics.genomicFileProcessedId,
                                           missing_data_files))

            # Update Member
            if next_state is not None and next_state != member.genomicWorkflowState:
                self.member_dao.update_member_state(member, next_state, project_id=self.controller.bq_project_id)

        # Make a roc ticket for missing data files
        if len(total_missing_data) > 0:
            alert = GenomicAlertHandler()

            summary = '[Genomic System Alert] Missing AW2 WGS Manifest Files'
            description = "The following AW2 manifests are missing data files."
            description += f"\nGenomic Job Run ID: {self.run_id}"

            for f in total_missing_data:
                description += self._compile_missing_data_alert(f[0], f[1])
            alert.make_genomic_alert(summary, description)

        return GenomicSubProcessResult.SUCCESS

    def _compile_missing_data_alert(self, _file_processed_id, _missing_data):
        """
        Compiles the description to include in a GenomicAlert
        :param _file_processed_id:
        :param _missing_data: list of files
        :return: summary, description
        """
        file = self.file_dao.get(_file_processed_id)

        description = f"\n\tManifest File: {file.fileName}"
        description += f"\n\tMissing Genotype Data: {_missing_data}"

        return description

    def generate_cvl_reconciliation_report(self):
        """
        The main method for the CVL Reconciliation report,
        ouptuts report file to the cvl subfolder and updates
        genomic_set_member
        :return: result code
        """
        members = self.member_dao.get_members_for_cvl_reconciliation()
        if members:
            cvl_subfolder = getSetting(GENOMIC_CVL_RECONCILIATION_REPORT_SUBFOLDER)
            self.cvl_file_name = f"{cvl_subfolder}/cvl_report_{self.run_id}.csv"
            self._write_cvl_report_to_file(members)

            results = []
            for member in members:
                results.append(self.member_dao.update_member_job_run_id(
                    member, job_run_id=self.run_id,
                    field='reconcileCvlJobRunId')
                )

            return GenomicSubProcessResult.SUCCESS \
                if GenomicSubProcessResult.ERROR not in results \
                else GenomicSubProcessResult.ERROR

        return GenomicSubProcessResult.NO_FILES

    def reconcile_gem_report_states(self, _last_run_time=None):
        """
        Scans GEM report states for changes
        :param _last_run_time: the time when the current job last ran
        """

        # Get unconsented members to update (consent > last run time of job_id)
        unconsented_gror_members = self.member_dao.get_unconsented_gror_since_date(_last_run_time)

        # update each member with the new state and withdrawal time
        for member in unconsented_gror_members:
            new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                          signal='unconsented')

            if new_state is not None or new_state != member.genomicWorkflowState:
                self.member_dao.update_member_state(member, new_state)

                # Handle withdrawal (gror/primary consent) for reportConsentRemovalDate
                removal_date = self.member_dao.get_gem_consent_removal_date(member)
                self.member_dao.update_report_consent_removal_date(member, removal_date)

        # Get reconsented members to update (consent > last run time of job_id)
        reconsented_gror_members = self.member_dao.get_reconsented_gror_since_date(_last_run_time)

        # update each member with the new state
        for member in reconsented_gror_members:
            new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                          signal='reconsented')

            if new_state is not None or new_state != member.genomicWorkflowState:
                self.member_dao.update_member_state(member, new_state)
                self.member_dao.update_report_consent_removal_date(member, None)

    def _check_genotyping_file_exists(self, bucket_name, filename):
        files = list_blobs('/' + bucket_name)
        filenames = [f.name for f in files if f.name.endswith(filename)]
        return 1 if len(filenames) > 0 else 0

    def _get_full_filename(self, filename):
        """ Searches file_list for names ending in filename
        :param filename: file name to match
        :return: first filename in list
        """
        filenames = [name for name in self.file_list if name.lower().endswith(filename.lower())]
        return filenames[0] if len(filenames) > 0 else 0

    def _get_full_filename_with_expression(self, expression):
        """ Searches file_list for names that match the expression
        :param expression: pattern to match
        :return: file name with highest revision number
        """
        filenames = [name for name in self.file_list if re.search(expression, name)]

        def sort_filenames(name):
            version = name.split('.')[0].split('_')[-1]

            if version[0].isalpha():
                version = version[1:]

            return int(version)

        # Naturally sort the list in descending order of revisions
        # ex: [name_11.ext, name_10.ext, name_9.ext, name_8.ext, etc.]
        filenames.sort(reverse=True, key=sort_filenames)

        return filenames[0] if len(filenames) > 0 else 0

    def _get_sequence_files(self, bucket_name):
        """
        Checks the bucket for sequencing files based on naming convention
        :param bucket_name:
        :return: file list or result code
        """
        try:
            files = list_blobs('/' + bucket_name)
            # TODO: naming_convention is not yet finalized
            naming_convention = r"^gc_sequencing_t\d*\.txt$"
            files = [s.name for s in files
                     if self.archive_folder not in s.name.lower()
                     if re.search(naming_convention,
                                  s.name.lower())]
            if not files:
                logging.info(
                    f'No valid sequencing files in bucket {bucket_name}'
                )
                return GenomicSubProcessResult.NO_FILES
            return files
        except FileNotFoundError:
            return GenomicSubProcessResult.ERROR

    def _parse_seq_filename(self, filename):
        """
        Takes a sequencing filename and returns the biobank id.
        :param filename:
        :return: biobank_id
        """
        # TODO: naming_convention is not yet finalized
        try:
            # pull biobank ID from filename
            return filename.lower().split('_')[-1].split('.')[0][1:]
        except IndexError:
            return GenomicSubProcessResult.INVALID_FILE_NAME

    def _update_genomic_set_member_seq_reconciliation(self, member, seq_file_name, job_run_id):
        """
        Uses member DAO to update GenomicSetMember object
        with sequencing reconciliation data
        :param member: the GenomicSetMember to update
        :param seq_file_name:
        :param job_run_id:
        :return: query result
        """
        return self.member_dao.update_member_sequencing_file(member,
                                                             job_run_id,
                                                             seq_file_name)

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
                         GenomicValidationFlag.INVALID_AIAN,
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
                                                         "valid_ai_ans"])

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

        if len(samples) > 0:
            samples_meta = self.GenomicSampleMeta(*samples)
            return self.process_samples_into_manifest(samples_meta, cohort=self.COHORT_3_ID)

        else:
            logging.info(f'New Participant Workflow: No new samples to process.')
            return GenomicSubProcessResult.NO_FILES

    def create_saliva_genomic_participants(self, local=False, config=None):
        """
        This method determines which samples to enter into
        the genomic system that are saliva only, via the
        config obj passed in the argument.

        :param: config : options for ror consent type and denoting if sample was generated in-home or in-clinic
        :return: result
        """
        participants = self._get_remaining_saliva_participants(config)

        if len(participants) > 0:
            return self.create_matrix_and_process_samples(participants, cohort=None, local=local, saliva=True)

        else:
            logging.info(
                f'Saliva Participant Workflow: No participants to process.')
            return GenomicSubProcessResult.NO_FILES

    def create_c2_genomic_participants(self, from_date, local=False):
        """
        Creates Cohort 2 Participants in the genomic system using reconsent.
        Validation is handled in the query that retrieves the newly consented
        participants. Only valid participants are currently sent.
        Refactored to first pull valid participants, then pull their samples,
        applying the new business logic of prioritizing
        collection date & blood over saliva.

        :param: from_date : the date from which to lookup new participants
        :return: result
        """

        participants = self._get_new_c2_participants(from_date)

        if len(participants) > 0:
            return self.create_matrix_and_process_samples(participants, cohort=self.COHORT_2_ID, local=local)

        else:
            logging.info(f'Cohort 2 Participant Workflow: No participants to process.')
            return GenomicSubProcessResult.NO_FILES

    def create_c1_genomic_participants(self, from_date, local=False):
        """
        Creates Cohort 1 Participants in the genomic system using reconsent.
        Validation is handled in the query that retrieves the newly consented
        participants. Only valid participants are currently sent.

        :param: from_date : the date from which to lookup new participants
        :return: result
        """

        participants = self._get_new_c1_participants(from_date)

        if len(participants) > 0:
            return self.create_matrix_and_process_samples(participants, cohort=self.COHORT_1_ID, local=local)

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
                participants=participants
            )

        logging.info(f'Long Read Participant Workflow: No participants to process.')
        return GenomicSubProcessResult.NO_FILES

    def process_genomic_members_into_manifest(self, *, participants):
        """
        Compiles AW0 Manifest from already submitted genomic members.
        :param participants:
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
                    genomeType=self._LR_GENOME_TYPE,
                    genomicWorkflowState=GenomicWorkflowState.LR_PENDING,
                    created=clock.CLOCK.now(),
                    modified=clock.CLOCK.now(),
                )

                processed_members.append(dup_member_obj)
                count = i + 1

                if count % 100 == 0:
                    self.genomic_members_insert(
                        members=processed_members,
                        session=session,
                        set_id=new_genomic_set.id,
                        bids=[pm.biobankId for pm in processed_members]
                    )
                    processed_members.clear()

            if count and processed_members:
                self.genomic_members_insert(
                    members=processed_members,
                    session=session,
                    set_id=new_genomic_set.id,
                    bids=[pm.biobankId for pm in processed_members]
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
                    samples_meta.valid_ai_ans[i],
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
                    ai_an='N' if samples_meta.valid_ai_ans[i] else 'Y',
                    genomeType=self._ARRAY_GENOME_TYPE,
                    genomicWorkflowState=GenomicWorkflowState.AW0_READY,
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
                        session=session,
                        set_id=new_genomic_set.id,
                        bids=bids
                    )
                    processed_array_wgs.clear()
                    bids.clear()

            if count and processed_array_wgs:
                self.genomic_members_insert(
                    members=processed_array_wgs,
                    session=session,
                    set_id=new_genomic_set.id,
                    bids=bids
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
                    self.member_dao.update_member_state(member, new_state)

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

    def genomic_members_insert(self, *, members, session, set_id, bids):
        """
        Bulk save of member for genomic_set_member as well as PDR
        batch updating of members
        :param: members
        :param: session
        :param: set_id
        :param: bids
        """

        try:
            session.bulk_save_objects(members)
            session.commit()
            members = self.member_dao.get_members_from_set_id(set_id, bids=bids)
            member_ids = [m.id for m in members]
            bq_genomic_set_member_batch_update(member_ids, project_id=self.controller.bq_project_id)
            genomic_set_member_batch_update(member_ids)
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

        return list(preferred_samples.values())

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

    def _get_new_c2_participants(self, from_date):
        """
        Retrieves C2 participants and validation data.
        Broken out so that DNA samples' business logic is handled separately
        :param from_date:
        :return:
        """
        _c2_participant_sql = self.query.new_c2_participants()

        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "ai_param": Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE.__int__(),
            "from_date_param": from_date.strftime("%Y-%m-%d"),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
            "suspension_param": SuspensionStatus.NOT_SUSPENDED.__int__(),
            "cohort_2_param": ParticipantCohort.COHORT_2.__int__(),
            "ignore_param": GenomicWorkflowState.IGNORE.__int__(),
        }

        with self.ps_dao.session() as session:
            result = session.execute(_c2_participant_sql, params).fetchall()

        return list([list(r) for r in zip(*result)])

    def _get_remaining_c2_participants(self):

        _c2_participant_sql = self.query.remaining_c2_participants()

        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "ai_param": Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE.__int__(),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
            "suspension_param": SuspensionStatus.NOT_SUSPENDED.__int__(),
            "cohort_2_param": ParticipantCohort.COHORT_2.__int__(),
            "ignore_param": GenomicWorkflowState.IGNORE.__int__(),
        }

        with self.ps_dao.session() as session:
            result = session.execute(_c2_participant_sql, params).fetchall()

        return list([list(r) for r in zip(*result)])

    def _get_new_c1_participants(self, from_date):
        """
        Retrieves C1 participants and validation data.
        :param from_date:
        :return:
        """
        _c1_participant_sql = self.query.new_c1_participants()

        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "ai_param": Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE.__int__(),
            "from_date_param": from_date.strftime("%Y-%m-%d"),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
            "suspension_param": SuspensionStatus.NOT_SUSPENDED.__int__(),
            "cohort_1_param": ParticipantCohort.COHORT_1.__int__(),
            "c1_reconsent_param": COHORT_1_REVIEW_CONSENT_YES_CODE,
            "ignore_param": GenomicWorkflowState.IGNORE.__int__(),
        }

        with self.ps_dao.session() as session:
            result = session.execute(_c1_participant_sql, params).fetchall()

        return list([list(r) for r in zip(*result)])

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

    def _get_remaining_saliva_participants(self, config):

        _saliva_sql = self.query.remaining_saliva_participants(config)

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

        # Insert new set for PDR
        bq_genomic_set_update(inserted_set.id, project_id=self.controller.bq_project_id)
        genomic_set_update(inserted_set.id)

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
    ManifestDef = namedtuple('ManifestDef', ["job_run_field",
                                             "source_data",
                                             "destination_bucket",
                                             "output_filename",
                                             "columns",
                                             "signal"])

    PROCESSING_STATUS_PASS = 'pass'
    DEFAULT_SIGNAL = 'manifest-generated'

    def __init__(self, job_run_id=None, bucket_name=None, **kwargs):
        # Attributes
        self.job_run_id = job_run_id
        self.bucket_name = bucket_name
        self.kwargs = kwargs

    def _get_source_data_query(self, manifest_type):
        """
        Returns the query to use for manifest's source data
        :param manifest_type:
        :return: query object
        """
        query_sql = ""

        # AW3 Array Manifest
        if manifest_type == GenomicManifestTypes.AW3_ARRAY:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicGCValidationMetrics.chipwellbarcode,
                        sqlalchemy.func.concat(get_biobank_id_prefix(), GenomicSetMember.biobankId),
                        GenomicSetMember.sampleId,
                        GenomicSetMember.sexAtBirth,
                        GenomicSetMember.gcSiteId,
                        GenomicGCValidationMetrics.idatRedPath,
                        GenomicGCValidationMetrics.idatRedMd5Path,
                        GenomicGCValidationMetrics.idatGreenPath,
                        GenomicGCValidationMetrics.idatGreenMd5Path,
                        GenomicGCValidationMetrics.vcfPath,
                        GenomicGCValidationMetrics.vcfTbiPath,
                        GenomicGCValidationMetrics.vcfMd5Path,
                        GenomicGCValidationMetrics.callRate,
                        GenomicGCValidationMetrics.sexConcordance,
                        GenomicGCValidationMetrics.contamination,
                        GenomicGCValidationMetrics.processingStatus,
                        Participant.researchId,
                    ]
                ).select_from(
                    sqlalchemy.join(
                        sqlalchemy.join(
                            sqlalchemy.join(ParticipantSummary,
                                            GenomicSetMember,
                                            GenomicSetMember.participantId == ParticipantSummary.participantId),
                            GenomicGCValidationMetrics,
                            GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                        ),
                        Participant,
                        Participant.participantId == ParticipantSummary.participantId
                    )
                ).where(
                    (GenomicGCValidationMetrics.processingStatus == self.PROCESSING_STATUS_PASS) &
                    (GenomicGCValidationMetrics.ignoreFlag != 1) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == GENOME_TYPE_ARRAY) &
                    (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN) &
                    (ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED) &
                    (GenomicGCValidationMetrics.idatRedReceived == 1) &
                    (GenomicGCValidationMetrics.idatGreenReceived == 1) &
                    (GenomicGCValidationMetrics.idatRedMd5Received == 1) &
                    (GenomicGCValidationMetrics.idatGreenMd5Received == 1) &
                    (GenomicGCValidationMetrics.vcfReceived == 1) &
                    (GenomicGCValidationMetrics.vcfMd5Received == 1) &
                    (GenomicSetMember.aw3ManifestJobRunID.is_(None))
                )
            )

        # AW3 WGS Manifest
        if manifest_type == GenomicManifestTypes.AW3_WGS:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sampleId,
                        sqlalchemy.func.concat(get_biobank_id_prefix(),
                                               GenomicSetMember.biobankId, '_',
                                               GenomicSetMember.sampleId),
                        GenomicSetMember.sexAtBirth,
                        GenomicSetMember.gcSiteId,
                        GenomicGCValidationMetrics.hfVcfPath,
                        GenomicGCValidationMetrics.hfVcfTbiPath,
                        GenomicGCValidationMetrics.hfVcfMd5Path,
                        GenomicGCValidationMetrics.rawVcfPath,
                        GenomicGCValidationMetrics.rawVcfTbiPath,
                        GenomicGCValidationMetrics.rawVcfMd5Path,
                        GenomicGCValidationMetrics.cramPath,
                        GenomicGCValidationMetrics.cramMd5Path,
                        GenomicGCValidationMetrics.craiPath,
                        GenomicGCValidationMetrics.contamination,
                        GenomicGCValidationMetrics.sexConcordance,
                        GenomicGCValidationMetrics.processingStatus,
                        GenomicGCValidationMetrics.meanCoverage,
                        Participant.researchId,
                        GenomicSetMember.sampleId,
                    ]
                ).select_from(
                    sqlalchemy.join(
                        sqlalchemy.join(
                            sqlalchemy.join(ParticipantSummary,
                                            GenomicSetMember,
                                            GenomicSetMember.participantId == ParticipantSummary.participantId),
                            GenomicGCValidationMetrics,
                            GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                        ),
                        Participant,
                        Participant.participantId == ParticipantSummary.participantId
                    )
                ).where(
                    (GenomicGCValidationMetrics.processingStatus == self.PROCESSING_STATUS_PASS) &
                    (GenomicGCValidationMetrics.ignoreFlag != 1) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == GENOME_TYPE_WGS) &
                    (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN) &
                    (ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED) &
                    (GenomicSetMember.aw3ManifestJobRunID == None) &
                    (GenomicGCValidationMetrics.hfVcfReceived == 1) &
                    (GenomicGCValidationMetrics.hfVcfTbiReceived == 1) &
                    (GenomicGCValidationMetrics.hfVcfMd5Received == 1) &
                    (GenomicGCValidationMetrics.rawVcfReceived == 1) &
                    (GenomicGCValidationMetrics.rawVcfTbiReceived == 1) &
                    (GenomicGCValidationMetrics.rawVcfMd5Received == 1) &
                    (GenomicGCValidationMetrics.cramReceived == 1) &
                    (GenomicGCValidationMetrics.cramMd5Received == 1) &
                    (GenomicGCValidationMetrics.craiReceived == 1)
                )
            )

        # CVL W1 Manifest
        if manifest_type == GenomicManifestTypes.CVL_W1:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicSet.genomicSetName,
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sampleId,
                        GenomicSetMember.sexAtBirth,
                        GenomicSetMember.nyFlag,
                        GenomicGCValidationMetrics.siteId,
                        sqlalchemy.bindparam('secondary_validation', None),
                        sqlalchemy.bindparam('date_submitted', None),
                        sqlalchemy.bindparam('test_name', 'aou_wgs'),
                    ]
                ).select_from(
                    sqlalchemy.join(
                        sqlalchemy.join(GenomicSet, GenomicSetMember, GenomicSetMember.genomicSetId == GenomicSet.id),
                        GenomicGCValidationMetrics,
                        GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                    )
                ).where(
                    (GenomicGCValidationMetrics.processingStatus == 'pass') &
                    (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.CVL_READY) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == "aou_wgs")
                )
            )

        # CVL W3 Manifest
        if manifest_type == GenomicManifestTypes.CVL_W3:
            query_sql = (
                sqlalchemy.select(
                    [
                        sqlalchemy.bindparam('value', ''),
                        GenomicSetMember.sampleId,
                        GenomicSetMember.biobankId,
                        GenomicSetMember.collectionTubeId.label("collection_tubeid"),
                        GenomicSetMember.sexAtBirth,
                        sqlalchemy.bindparam('genome_type', 'aou_wgs'),
                        GenomicSetMember.nyFlag,
                        sqlalchemy.bindparam('request_id', ''),
                        sqlalchemy.bindparam('package_id', ''),
                        GenomicSetMember.ai_an,
                        GenomicSetMember.gcSiteId.label('site_id'),
                    ]
                ).select_from(
                    sqlalchemy.join(
                        GenomicSetMember,
                        ParticipantSummary,
                        GenomicSetMember.participantId == ParticipantSummary.participantId
                    )
                ).where(
                    (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.W2) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == "aou_cvl") &
                    (ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED)
                )
            )

        # Color GEM A1 Manifest
        if manifest_type == GenomicManifestTypes.GEM_A1:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sampleId,
                        GenomicSetMember.sexAtBirth,
                        sqlalchemy.func.IF(ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                                           sqlalchemy.sql.expression.literal("yes"),
                                           sqlalchemy.sql.expression.literal("no")),
                        ParticipantSummary.consentForGenomicsRORAuthored,
                        GenomicGCValidationMetrics.chipwellbarcode,
                        sqlalchemy.func.upper(GenomicSetMember.gcSiteId),
                    ]
                ).select_from(
                    sqlalchemy.join(
                        sqlalchemy.join(ParticipantSummary,
                                        GenomicSetMember,
                                        GenomicSetMember.participantId == ParticipantSummary.participantId),
                        GenomicGCValidationMetrics,
                        GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                    )
                ).where(
                    (GenomicGCValidationMetrics.processingStatus == 'pass') &
                    (GenomicGCValidationMetrics.ignoreFlag != 1) &
                    (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.GEM_READY) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == "aou_array") &
                    (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN) &
                    (ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED) &
                    (ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED) &
                    (ParticipantSummary.participantOrigin != 'careevolution')
                ).group_by(
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sampleId,
                        GenomicSetMember.sexAtBirth,
                        sqlalchemy.func.IF(ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                                           sqlalchemy.sql.expression.literal("yes"),
                                           sqlalchemy.sql.expression.literal("no")),
                        ParticipantSummary.consentForGenomicsRORAuthored,
                        GenomicGCValidationMetrics.chipwellbarcode,
                        sqlalchemy.func.upper(GenomicSetMember.gcSiteId),
                           ).order_by(ParticipantSummary.consentForGenomicsRORAuthored).limit(10000)
            )

        # Color GEM A3 Manifest
        # Those with A1 and not A3 or updated consents since sent A3
        if manifest_type == GenomicManifestTypes.GEM_A3:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sampleId,
                        sqlalchemy.func.date_format(GenomicSetMember.reportConsentRemovalDate, '%Y-%m-%dT%TZ'),
                    ]
                ).select_from(
                    sqlalchemy.join(ParticipantSummary,
                                    GenomicSetMember,
                                    GenomicSetMember.participantId == ParticipantSummary.participantId)
                ).where(
                    (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.GEM_RPT_PENDING_DELETE) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == "aou_array")
                )
            )

        if manifest_type == GenomicManifestTypes.AW2F:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicSetMember.packageId,
                        sqlalchemy.func.concat(get_biobank_id_prefix(),
                                               GenomicSetMember.biobankId, "_", GenomicSetMember.sampleId),
                        GenomicSetMember.gcManifestBoxStorageUnitId,
                        GenomicSetMember.gcManifestBoxPlateId,
                        GenomicSetMember.gcManifestWellPosition,
                        GenomicSetMember.sampleId,
                        GenomicSetMember.gcManifestParentSampleId,
                        GenomicSetMember.collectionTubeId,
                        GenomicSetMember.gcManifestMatrixId,
                        sqlalchemy.bindparam('collection_date', ''),
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sexAtBirth,
                        sqlalchemy.bindparam('age', ''),
                        sqlalchemy.func.IF(GenomicSetMember.nyFlag == 1,
                                           sqlalchemy.sql.expression.literal("Y"),
                                           sqlalchemy.sql.expression.literal("N")),
                        sqlalchemy.bindparam('sample_type', 'DNA'),
                        GenomicSetMember.gcManifestTreatments,
                        GenomicSetMember.gcManifestQuantity_ul,
                        GenomicSetMember.gcManifestTotalConcentration_ng_per_ul,
                        GenomicSetMember.gcManifestTotalDNA_ng,
                        GenomicSetMember.gcManifestVisitDescription,
                        GenomicSetMember.gcManifestSampleSource,
                        GenomicSetMember.gcManifestStudy,
                        GenomicSetMember.gcManifestTrackingNumber,
                        GenomicSetMember.gcManifestContact,
                        GenomicSetMember.gcManifestEmail,
                        GenomicSetMember.gcManifestStudyPI,
                        GenomicSetMember.gcManifestTestName,
                        GenomicSetMember.gcManifestFailureMode,
                        GenomicSetMember.gcManifestFailureDescription,
                        GenomicGCValidationMetrics.processingStatus,
                        GenomicGCValidationMetrics.contamination,
                        sqlalchemy.case(
                            [
                                (GenomicGCValidationMetrics.contaminationCategory ==
                                 GenomicContaminationCategory.EXTRACT_WGS, "extract wgs"),

                                (GenomicGCValidationMetrics.contaminationCategory ==
                                 GenomicContaminationCategory.NO_EXTRACT, "no extract"),

                                (GenomicGCValidationMetrics.contaminationCategory ==
                                 GenomicContaminationCategory.EXTRACT_BOTH, "extract both"),

                                (GenomicGCValidationMetrics.contaminationCategory ==
                                 GenomicContaminationCategory.TERMINAL_NO_EXTRACT, "terminal no extract"),
                            ], else_=""
                        ),
                        sqlalchemy.func.IF(ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                                           sqlalchemy.sql.expression.literal("yes"),
                                           sqlalchemy.sql.expression.literal("no")),
                    ]
                ).select_from(
                    sqlalchemy.join(
                        ParticipantSummary,
                        GenomicSetMember,
                        GenomicSetMember.participantId == ParticipantSummary.participantId
                    ).join(
                        GenomicGCValidationMetrics,
                        GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                    ).join(
                        GenomicFileProcessed,
                        GenomicFileProcessed.id == GenomicSetMember.aw1FileProcessedId
                    )
                ).where(
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicGCValidationMetrics.ignoreFlag == 0) &
                    (GenomicGCValidationMetrics.contamination.isnot(None)) &
                    (GenomicGCValidationMetrics.contamination != '') &
                    (GenomicFileProcessed.genomicManifestFileId == self.kwargs['kwargs']['input_manifest'].id)
                )
            )
        return query_sql

    @staticmethod
    def _get_manifest_columns(manifest_type):
        """
        Defines the columns of each manifest-type
        :param manifest_type:
        :return: column tuple
        """
        column_config = {
            GenomicManifestTypes.CVL_W1: (
                    "genomic_set_name",
                    "biobank_id",
                    "sample_id",
                    "sex_at_birth",
                    "ny_flag",
                    "site_id",
                    "secondary_validation",
                    "date_submitted",
                    "test_name",
            ),
            GenomicManifestTypes.AW3_ARRAY: (
                "chipwellbarcode",
                "biobank_id",
                "sample_id",
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
            ),
            GenomicManifestTypes.GEM_A1:  (
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
            GenomicManifestTypes.CVL_W3: (
                "value",
                "sample_id",
                "biobank_id",
                "collection_tubeid",
                "sex_at_birth",
                "genome_type",
                "ny_flag",
                "request_id",
                "package_id",
                "ai_an",
                "site_id",
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
                "vcf_raw_path",
                "vcf_raw_index_path",
                "vcf_raw_md5_path",
                "cram_path",
                "cram_md5_path",
                "crai_path",
                "contamination",
                "sex_concordance",
                "processing_status",
                "mean_coverage",
                "research_id",
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
        return column_config[manifest_type]

    def get_def(self, manifest_type):
        """
        Returns the manifest definition based on manifest_type
        :param manifest_type:
        :return: ManifestDef()
        """
        now_formatted = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")

        # DRC Broad CVL WGS Manifest
        if manifest_type == GenomicManifestTypes.CVL_W1:
            return self.ManifestDef(
                job_run_field='cvlW1ManifestJobRunId',
                source_data=self._get_source_data_query(GenomicManifestTypes.CVL_W1),
                destination_bucket=f'{self.bucket_name}',
                output_filename=f'{CVL_W1_MANIFEST_SUBFOLDER}/AoU_CVL_Manifest_{now_formatted}.csv',
                columns=self._get_manifest_columns(GenomicManifestTypes.CVL_W1),
                signal=self.DEFAULT_SIGNAL,
            )

        # Color Array A1 Manifest
        if manifest_type == GenomicManifestTypes.GEM_A1:
            return self.ManifestDef(
                job_run_field='gemA1ManifestJobRunId',
                source_data=self._get_source_data_query(GenomicManifestTypes.GEM_A1),
                destination_bucket=f'{self.bucket_name}',
                output_filename=f'{GENOMIC_GEM_A1_MANIFEST_SUBFOLDER}/AoU_GEM_A1_manifest_{now_formatted}.csv',
                columns=self._get_manifest_columns(GenomicManifestTypes.GEM_A1),
                signal=self.DEFAULT_SIGNAL,
            )
        # Color A3 Manifest
        if manifest_type == GenomicManifestTypes.GEM_A3:
            return self.ManifestDef(
                job_run_field='gemA3ManifestJobRunId',
                source_data=self._get_source_data_query(GenomicManifestTypes.GEM_A3),
                destination_bucket=f'{self.bucket_name}',
                output_filename=f'{GENOMIC_GEM_A3_MANIFEST_SUBFOLDER}/AoU_GEM_A3_manifest_{now_formatted}.csv',
                columns=self._get_manifest_columns(GenomicManifestTypes.GEM_A3),
                signal=self.DEFAULT_SIGNAL,
            )

        # DRC to CVL W3 Manifest
        if manifest_type == GenomicManifestTypes.CVL_W3:
            return self.ManifestDef(
                job_run_field='cvlW3ManifestJobRunID',
                source_data=self._get_source_data_query(GenomicManifestTypes.CVL_W3),
                destination_bucket=f'{self.bucket_name}',
                output_filename=f'{CVL_W3_MANIFEST_SUBFOLDER}/AoU_CVL_W1_{now_formatted}.csv',
                columns=self._get_manifest_columns(GenomicManifestTypes.CVL_W3),
                signal=self.DEFAULT_SIGNAL,
            )

        # DRC to Broad AW3 Array Manifest
        if manifest_type == GenomicManifestTypes.AW3_ARRAY:
            return self.ManifestDef(
                job_run_field='aw3ManifestJobRunID',
                source_data=self._get_source_data_query(GenomicManifestTypes.AW3_ARRAY),
                destination_bucket=f'{self.bucket_name}',
                output_filename=f'{GENOMIC_AW3_ARRAY_SUBFOLDER}/AoU_DRCV_GEN_{now_formatted}.csv',
                columns=self._get_manifest_columns(GenomicManifestTypes.AW3_ARRAY),
                signal="bypass",
            )

        # DRC to Broad AW3 WGS Manifest
        if manifest_type == GenomicManifestTypes.AW3_WGS:
            return self.ManifestDef(
                job_run_field='aw3ManifestJobRunID',
                source_data=self._get_source_data_query(GenomicManifestTypes.AW3_WGS),
                destination_bucket=f'{self.bucket_name}',
                output_filename=f'{GENOMIC_AW3_WGS_SUBFOLDER}/AoU_DRCV_SEQ_{now_formatted}.csv',
                columns=self._get_manifest_columns(GenomicManifestTypes.AW3_WGS),
                signal="bypass",
            )

        # DRC to Biobank AW2F Feedback/Contamination Manifest
        if manifest_type == GenomicManifestTypes.AW2F:
            return self.ManifestDef(
                job_run_field=None,
                source_data=self._get_source_data_query(GenomicManifestTypes.AW2F),
                destination_bucket=f'{self.bucket_name}',
                output_filename=f'{BIOBANK_AW2F_SUBFOLDER}/GC_AoU_DataType_PKG-YYMM-xxxxxx_contamination.csv',
                columns=self._get_manifest_columns(GenomicManifestTypes.AW2F),
                signal="bypass",
            )


class ManifestCompiler:
    """
    This component compiles Genomic manifests
    based on definitions provided by ManifestDefinitionProvider
    """
    def __init__(self, run_id, bucket_name=None):
        self.run_id = run_id

        self.bucket_name = bucket_name
        self.output_file_name = None
        self.manifest_def = None

        self.def_provider = None

        # Dao components
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()

    def generate_and_transfer_manifest(self, manifest_type, genome_type, **kwargs):
        """
        Main execution method for ManifestCompiler
        :return: result dict:
            "code": (i.e. SUCCESS)
            "feedback_file": None or feedback file record to update,
            "record_count": integer
        """
        self.def_provider = ManifestDefinitionProvider(
            job_run_id=self.run_id, bucket_name=self.bucket_name, kwargs=kwargs
        )

        self.manifest_def = self.def_provider.get_def(manifest_type)

        source_data = self._pull_source_data()
        if source_data:
            self.output_file_name = self.manifest_def.output_filename

            # If the new manifest is a feedback manifest,
            # it will have an input manifest
            if "input_manifest" in kwargs.keys():

                # AW2F manifest file name is based of of AW1
                if manifest_type == GenomicManifestTypes.AW2F:

                    new_name = kwargs['input_manifest'].filePath.split('/')[-1]
                    new_name = new_name.replace('.csv', '_contamination.csv')

                    self.output_file_name = self.manifest_def.output_filename.replace(
                        "GC_AoU_DataType_PKG-YYMM-xxxxxx_contamination.csv",
                        f"{new_name}"
                    )

            logging.info(
                f'Preparing manifest of type {manifest_type}...'
                f'{self.manifest_def.destination_bucket}/{self.output_file_name}'
            )

            self._write_and_upload_manifest(source_data)

            results = []

            record_count = len(source_data)

            for row in source_data:
                member = self.member_dao.get_member_from_sample_id(row.sample_id, genome_type)

                if self.manifest_def.job_run_field is not None:
                    results.append(
                        self.member_dao.update_member_job_run_id(
                            member,
                            job_run_id=self.run_id,
                            field=self.manifest_def.job_run_field
                        )
                    )

                # Handle Genomic States for manifests
                if self.manifest_def.signal != "bypass":
                    new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                                  signal=self.manifest_def.signal)

                    if new_state is not None or new_state != member.genomicWorkflowState:
                        self.member_dao.update_member_state(member, new_state)

            # Assemble result dict
            result_code = GenomicSubProcessResult.SUCCESS \
                if GenomicSubProcessResult.ERROR not in results \
                else GenomicSubProcessResult.ERROR

            result = {
                "code": result_code,
                "record_count": record_count,
            }

            return result
        logging.info(f'No records found for manifest type: {manifest_type}.')
        return {
                "code": GenomicSubProcessResult.NO_FILES,
                "record_count": 0,
            }

    def _pull_source_data(self):
        """
        Runs the source data query
        :return: result set
        """
        with self.member_dao.session() as session:
            return session.execute(self.manifest_def.source_data).fetchall()

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


class GenomicAlertHandler:
    """
    Creates a jira ROC ticket using Jira utils
    """
    ROC_BOARD_ID = "ROC"

    def __init__(self):
        self.alert_envs = ["all-of-us-rdr-prod"]
        if GAE_PROJECT in self.alert_envs:
            self._jira_handler = JiraTicketHandler()
        else:
            self._jira_handler = None

    def make_genomic_alert(self, summary: str, description: str):
        """
        Wraps create_ticket with genomic specifics
        Get's the board ID and adds ticket to sprint
        :param summary: the 'title' of the ticket
        :param description: the 'body' of the ticket
        """
        if self._jira_handler is not None:
            ticket = self._jira_handler.create_ticket(summary, description,
                                                      board_id=self.ROC_BOARD_ID)

            active_sprint = self._jira_handler.get_active_sprint(
                self._jira_handler.get_board_by_id(self.ROC_BOARD_ID))

            self._jira_handler.add_ticket_to_sprint(ticket, active_sprint)

        else:
            logging.info('Suppressing alert for missing files')
            return
