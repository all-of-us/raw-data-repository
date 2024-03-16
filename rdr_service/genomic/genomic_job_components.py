"""
Component Classes for Genomic Jobs
Components are assembled by the JobController for a particular Genomic Job
"""

import csv
import json
import logging
import re
from typing import List, OrderedDict

import pytz
from collections import deque, namedtuple
from copy import deepcopy
from dateutil.parser import parse
import sqlalchemy

from rdr_service import clock, config
from rdr_service.cloud_utils.gcp_google_pubsub import submit_pipeline_pubsub_msg_from_model
from rdr_service.dao.code_dao import CodeDao
from rdr_service.genomic.genomic_short_read_workflow import GenomicAW1Workflow, GenomicAW2Workflow, GenomicAW4Workflow
from rdr_service.genomic.genomic_sub_workflow import GenomicSubWorkflow, GenomicSubLongReadWorkflow
from rdr_service.genomic_enums import ResultsModuleType
from rdr_service.genomic.genomic_data import GenomicQueryClass
from rdr_service.genomic.genomic_state_handler import GenomicStateHandler
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.code import Code
from rdr_service.model.participant_summary import ParticipantRaceAnswers, ParticipantSummary
from rdr_service.model.config_utils import get_biobank_id_prefix

from rdr_service.api_util import (
    open_cloud_file,
    list_blobs,
    get_blob
)
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
    GenomicManifestFileDao,
    GenomicAW1RawDao,
    GenomicAW2RawDao,
    GenomicIncidentDao,
    UserEventMetricsDao,
    GenomicCVLSecondSampleDao, GenomicAppointmentEventMetricsDao, GenomicLongReadDao, GenomicPRDao, GenomicRNADao,
    GenomicShortReadDao, GenomicCVLDao)
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
    GENOMIC_GEM_A1_MANIFEST_SUBFOLDER,
    GENOMIC_GEM_A3_MANIFEST_SUBFOLDER,
    GENOME_TYPE_ARRAY,
    GENOME_TYPE_WGS,
    GENOMIC_AW3_ARRAY_SUBFOLDER,
    GENOMIC_AW3_WGS_SUBFOLDER,
    BIOBANK_AW2F_SUBFOLDER,
    CVL_W1IL_HDR_MANIFEST_SUBFOLDER,
    CVL_W1IL_PGX_MANIFEST_SUBFOLDER,
    CVL_W2W_MANIFEST_SUBFOLDER,
    CVL_W3SR_MANIFEST_SUBFOLDER,
    LR_L0_MANIFEST_SUBFOLDER, PR_P0_MANIFEST_SUBFOLDER, RNA_R0_MANIFEST_SUBFOLDER, LR_L3_MANIFEST_SUBFOLDER
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
        self.sub_folder_name = sub_folder
        self.investigation_set_id = None
        self.participant_dao = None
        # Sub Components
        self.file_validator = GenomicFileValidator(
            job_id=self.job_id,
            controller=self.controller
        )
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.member_dao = GenomicSetMemberDao()
        self.job_run_dao = GenomicJobRunDao()
        self.incident_dao = GenomicIncidentDao()
        self.user_metrics_dao = UserEventMetricsDao()
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
                    logging.error(f'Exception occurred when ingesting manifest {current_file.filePath}: {e}')
                    self.controller.create_incident(
                        source_job_run_id=self.controller.job_run.id,
                        source_file_processed_id=current_file.id,
                        code=GenomicIncidentCode.MANIFEST_INGESTION_EXCEPTION.name,
                        message=f"{self.job_id.name}: Exception occurred when ingesting manifest "
                                f"{current_file.filePath}: {e}",
                        slack=True,
                        manifest_file_name=current_file.fileName
                    )
                    self.file_queue.popleft()
                except IndexError:
                    logging.info('No files left in file queue.')

            return GenomicSubProcessResult.SUCCESS if all(results) \
                else GenomicSubProcessResult.ERROR

    @classmethod
    def clean_row_keys(cls, val):
        def str_clean(str_val):
            return str_val.lower() \
                .replace(' ', '') \
                .replace('_', '')

        if type(val) is str or 'quoted_name' in val.__class__.__name__.lower():
            return str_clean(val)
        elif 'dict' in val.__class__.__name__.lower():
            return dict(zip([str_clean(key)
                             for key in val], val.values()))

    @classmethod
    def _clean_alpha_values(cls, value):
        return value[1:] if value[0].isalpha() else value

    def _ingest_genomic_file(self, file_obj):
        """
        Reads a file object from bucket and inserts into DB
        :param: file_obj: A genomic file object
        :return: A GenomicSubProcessResultCode
        """
        self.file_obj = file_obj
        data_to_ingest = self._retrieve_data_from_path(self.file_obj.filePath)

        if not data_to_ingest:
            logging.info("No data to ingest.")
            return GenomicSubProcessResult.NO_FILES

        if data_to_ingest == GenomicSubProcessResult.ERROR:
            return data_to_ingest

        logging.info(f'Ingesting data from {self.file_obj.fileName}')
        logging.info("Validating file.")

        workflow_map = {
            GenomicJob.AW1_MANIFEST: GenomicAW1Workflow,
            GenomicJob.AW1F_MANIFEST: GenomicAW1Workflow,
            GenomicJob.METRICS_INGESTION: GenomicAW2Workflow,
            GenomicJob.AW4_ARRAY_WORKFLOW: GenomicAW4Workflow,
            GenomicJob.AW4_WGS_WORKFLOW: GenomicAW4Workflow,
        }

        current_ingestion_map = {
            GenomicJob.GEM_A2_MANIFEST: self._ingest_gem_a2_manifest,
            GenomicJob.GEM_METRICS_INGEST: self._ingest_gem_metrics_manifest,
            GenomicJob.AW1C_INGEST: self._ingest_aw1c_manifest,
            GenomicJob.AW1CF_INGEST: self._ingest_aw1c_manifest,
            GenomicJob.AW5_ARRAY_MANIFEST: self._ingest_aw5_manifest,
            GenomicJob.AW5_WGS_MANIFEST: self._ingest_aw5_manifest,
            GenomicJob.CVL_W2SC_WORKFLOW: self._ingest_cvl_w2sc_manifest,
            GenomicJob.CVL_W3NS_WORKFLOW: self._ingest_cvl_w3ns_manifest,
            GenomicJob.CVL_W3SS_WORKFLOW: self._ingest_cvl_w3ss_manifest,
            GenomicJob.CVL_W3SC_WORKFLOW: self._ingest_cvl_w3sc_manifest,
            GenomicJob.CVL_W4WR_WORKFLOW: self._ingest_cvl_w4wr_manifest,
            GenomicJob.CVL_W5NF_WORKFLOW: self._ingest_cvl_w5nf_manifest,
            GenomicJob.LR_LR_WORKFLOW: self._ingest_lr_manifest,
            GenomicJob.LR_L1_WORKFLOW: self._ingest_lr_manifest,
            GenomicJob.LR_L1F_WORKFLOW: self._ingest_lr_manifest,
            GenomicJob.LR_L2_ONT_WORKFLOW: self._ingest_lr_manifest,
            GenomicJob.LR_L2_PB_CCS_WORKFLOW: self._ingest_lr_manifest,
            GenomicJob.LR_L4_WORKFLOW: self._ingest_lr_manifest,
            GenomicJob.LR_L4F_WORKFLOW: self._ingest_lr_manifest,
            GenomicJob.LR_L5_WORKFLOW: self._ingest_lr_manifest,
            GenomicJob.LR_L6_WORKFLOW: self._ingest_lr_manifest,
            GenomicJob.LR_L6F_WORKFLOW: self._ingest_lr_manifest,
            GenomicJob.PR_PR_WORKFLOW: self._ingest_pr_manifest,
            GenomicJob.PR_P1_WORKFLOW: self._ingest_pr_manifest,
            GenomicJob.PR_P2_WORKFLOW: self._ingest_pr_manifest,
            GenomicJob.RNA_RR_WORKFLOW: self._ingest_rna_manifest,
            GenomicJob.RNA_R1_WORKFLOW: self._ingest_rna_manifest,
            GenomicJob.RNA_R2_WORKFLOW: self._ingest_rna_manifest
        }

        current_ingestion_workflow = current_ingestion_map.get(self.job_id)
        if not current_ingestion_workflow:
            current_workflow = workflow_map.get(self.job_id)(file_ingester=self)
            current_ingestion_workflow = current_workflow.run_ingestion

        self.file_validator.valid_schema = None

        validation_result = self.file_validator.validate_ingestion_file(
            filename=self.file_obj.fileName,
            data_to_validate=data_to_ingest
        )

        if validation_result != GenomicSubProcessResult.SUCCESS:
            # delete raw records
            if self.job_id == GenomicJob.AW1_MANIFEST:
                GenomicAW1RawDao().delete_from_filepath(file_obj.filePath)
            if self.job_id == GenomicJob.METRICS_INGESTION:
                GenomicAW2RawDao().delete_from_filepath(file_obj.filePath)
            return validation_result

        try:
            ingestions = self._set_data_ingest_iterations(data_to_ingest['rows'])
            for row in ingestions:
                current_ingestion_workflow(row)
            self._set_manifest_file_resolved()
            return GenomicSubProcessResult.SUCCESS
        # pylint: disable=broad-except
        except Exception as e:
            logging.warning(f'Exception occurred on manifest ingestion workflow: {e}')
            return GenomicSubProcessResult.ERROR

    def _set_data_ingest_iterations(self, data_rows: List[dict]):
        excluded_jobs, all_ingestions = [
            GenomicJob.LR_LR_WORKFLOW,
            GenomicJob.PR_PR_WORKFLOW,
            GenomicJob.RNA_RR_WORKFLOW
        ], []
        if self.controller.max_num \
            and self.job_id not in excluded_jobs \
                and len(data_rows) > self.controller.max_num:
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

    def load_raw_manifest_file(self, raw_dao, **kwargs):
        """
        Loads raw models with raw data from manifests file
        Ex: genomic_aw1_raw => aw1_manifest
        :param raw_dao: Model Dao Class
        :return:
        """

        dao = raw_dao() if not kwargs.get('model') else raw_dao(model_type=kwargs.get('model'))

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
        batch_size, item_count, batch = 100, 0, list()
        for row in file_data['rows']:
            if special_mappings := kwargs.get('special_mappings'):
                for mapping_key in special_mappings:
                    row[special_mappings.get(mapping_key)] = row.get(mapping_key)
                    del row[mapping_key]

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

    @classmethod
    def increment_manifest_file_record_count_from_id(cls, file_obj):
        """
        Increments the manifest record count by 1
        """
        manifest_file = GenomicManifestFileDao().get(file_obj.genomicManifestFileId)
        manifest_file.recordCount += 1

        with GenomicManifestFileDao().session() as s:
            s.merge(manifest_file)

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
                member = self.member_dao.get_member_from_sample_id_with_state(
                    sample_id,
                    GENOME_TYPE_ARRAY,
                    GenomicWorkflowState.A1
                )
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

    def _ingest_aw5_manifest(self, rows):
        metric_pipeline_id_map = {
            GenomicJob.AW5_WGS_MANIFEST: config.GENOMIC_UPDATED_WGS_DRAGEN
        }

        try:
            for row in rows:
                row_copy = self.clean_row_keys(row)

                biobank_id = row_copy['biobankid']
                biobank_id = self._clean_alpha_values(biobank_id)
                sample_id = row_copy['sampleid']

                member = self.member_dao.get_member_from_biobank_id_and_sample_id(biobank_id, sample_id)
                if not member:
                    logging.warning(f'Can not find genomic member record for biobank_id: '
                                    f'{biobank_id} and sample_id: {sample_id}, skipping...')
                    continue

                existing_metrics_obj = self.metrics_dao.get_metrics_by_member_id(
                    member_id=member.id,
                    pipeline_id=metric_pipeline_id_map.get(self.job_id, None)
                )

                if not existing_metrics_obj:
                    logging.warning(f'Can not find metrics record for member id: '
                                    f'{member.id}, skipping...')
                    continue

                metric_id = existing_metrics_obj.id
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
                row_copy = self.clean_row_keys(row)

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
                    # Publish PDR data-pipeline pub-sub event in chunks up to 250 records.
                    submit_pipeline_pubsub_msg_from_model(batch, database='rdr')

                item_count = 0
                batch.clear()

        if item_count:
            with metric_dao.session() as session:
                # Use session add_all() so we can get the newly created primary key id values back.
                session.add_all(batch)
                session.commit()
                # Batch update PDR resource records.
                submit_pipeline_pubsub_msg_from_model(batch, database='rdr')

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
            clean_row = {k.lower().replace('\ufeff', ''): v for k, v in row.copy().items()}
            data_to_ingest['rows'].append(clean_row)
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
        row = self.clean_row_keys(row_data)

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
            clean_column = self.clean_row_keys(column)
            row_value = row.get(clean_column)
            if row_value or row_value == "":
                row_obj[column] = row_value[0:512]

        row_obj['file_path'] = self.target_file
        row_obj['created'] = clock.CLOCK.now()
        row_obj['modified'] = clock.CLOCK.now()

        return row_obj

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

    def insert_member_for_replating(self, member_id, category):
        """
        Inserts a new member record for replating.
        :param member_id: GenomicSetMember.id
        :param category: GenomicContaminationCategory
        :return:
        """
        member = self.member_dao.get(member_id)
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

    def _base_cvl_ingestion(self, **kwargs):
        row_copy = self.clean_row_keys(kwargs.get('row'))
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

        return row_copy, member

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
                    run_attr='cvlW2scManifestJobRunID'
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
                    run_attr='cvlW3nsManifestJobRunID'
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
                    run_attr='cvlW3scManifestJobRunID'
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
                    run_attr='cvlW3ssManifestJobRunID'
                )
                if not (row_copy and member):
                    continue

                row_copy = dict(zip([key.replace('/', '').split('(')[0] for key in row_copy],
                                    row_copy.values()))

                # cvl second sample
                second_sample_obj = self.cvl_second_sample_dao.model_type()
                setattr(second_sample_obj, 'genomic_set_member_id', member.id)
                for col in sample_cols:
                    cleaned_col = self.clean_row_keys(col)
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
        run_id = None
        for result_key in run_attr_mapping.keys():
            if result_key in self.file_obj.fileName.lower():
                run_id = run_attr_mapping[result_key]
                break
        try:
            for row in rows:
                row_copy, member = self._base_cvl_ingestion(
                    row=row,
                    run_attr=run_id,
                )
                if not (row_copy and member):
                    continue

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_cvl_w5nf_manifest(self, rows):
        run_attr_mapping = {
            'hdrv1': 'cvlW5nfHdrManifestJobRunID',
            'pgxv1': 'cvlW5nfPgxManifestJobRunID'
        }
        run_id = None
        for result_key in run_attr_mapping.keys():
            if result_key in self.file_obj.fileName.lower():
                run_id = run_attr_mapping[result_key]
                break
        try:
            for row in rows:
                row_copy, member = self._base_cvl_ingestion(
                    row=row,
                    run_attr=run_id,
                )
                if not (row_copy and member):
                    continue

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    # Long Read
    def _ingest_lr_manifest(self, rows: List[OrderedDict]) -> GenomicSubProcessResult:
        try:
            GenomicSubLongReadWorkflow.create_genomic_sub_workflow(
                dao=GenomicLongReadDao,
                job_id=self.job_id,
                job_run_id=self.job_run_id,
                manifest_file_name=self.file_obj.fileName
            ).run_workflow(row_data=rows)
            return GenomicSubProcessResult.SUCCESS
        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    # Proteomics
    def _ingest_pr_manifest(self, rows: List[OrderedDict]) -> GenomicSubProcessResult:
        try:
            GenomicSubWorkflow.create_genomic_sub_workflow(
                dao=GenomicPRDao,
                job_id=self.job_id,
                job_run_id=self.job_run_id,
                manifest_file_name=self.file_obj.fileName
            ).run_workflow(row_data=rows)
            return GenomicSubProcessResult.SUCCESS
        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    # RNA Seq
    def _ingest_rna_manifest(self, rows: List[OrderedDict]) -> GenomicSubProcessResult:
        try:
            GenomicSubWorkflow.create_genomic_sub_workflow(
                dao=GenomicRNADao,
                job_id=self.job_id,
                job_run_id=self.job_run_id,
                manifest_file_name=self.file_obj.fileName
            ).run_workflow(row_data=rows)
            return GenomicSubProcessResult.SUCCESS
        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    @classmethod
    def validate_collection_tube_id(cls, collection_tube_id, bid):
        """
        Returns true if biobank_ID is associated to biobank_stored_sample_id
        (collection_tube_id)
        :param collection_tube_id:
        :param bid:
        :return: boolean
        """
        sample = BiobankStoredSampleDao().get(collection_tube_id)
        if not sample:
            return False
        return int(sample.biobankId) == int(bid)

    @staticmethod
    def get_qc_status_from_value(aw4_value):
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
                session.add(GenomicSampleContamination(
                    sampleId=sample_id,
                    failedInJob=self.job_id
                ))
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

    def __init__(self, filename=None, data=None, schema=None, job_id=None, controller=None):
        self.filename = filename
        self.data_to_validate = data
        self.valid_schema = schema
        self.job_id = job_id
        self.genome_type = None
        self.controller = controller
        self.gc_site_id = None

        self.VALID_CVL_FACILITIES = ('rdr', 'co', 'uw', 'bcm')
        self.CVL_ANALYSIS_TYPES = ('hdrv1', 'pgxv1')
        self.VALID_GENOME_CENTERS = ('uw', 'bam', 'bcm', 'bi', 'jh', 'ha', 'rdr')
        self.DRC_BROAD = 'drc_broad'

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

        # CVL pipeline
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

        # AW pipeline
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

        self.AW5_WGS_SCHEMA = (
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
        )

        self.AW5_ARRAY_SCHEMA = (
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
        )

        # Long Read pipeline
        self.LR_LR_SCHEMA = (
            "biobankid",
            "genometype",
            "parenttubeid",
            "lrsiteid",
            "longreadplatform"
        )

        self.LR_L1_SCHEMA = (
            "packageid",
            "biobankidsampleid",
            "boxstorageunitid",
            "boxidplateid",
            "wellposition",
            "sampleid",
            "parentsampleid",
            "collectiontubeid",
            "matrixid",
            "collectiondate",
            "biobankid",
            "sexatbirth",
            "age",
            "nystateyn",
            "sampletype",
            "treatments",
            "quantityul",
            "totalconcentrationngul",
            "totaldnang",
            "visitdescription",
            "samplesource",
            "study",
            "trackingnumber",
            "contact",
            "email",
            "studypi",
            "genometype",
            "lrsiteid",
            "longreadplatform",
            "failuremode",
            "failuremodedesc"
        )

        self.LR_L2_ONT_SCHEMA = (
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "flowcellid",
            "basecallerversion",
            "basecallermodel",
            "bampath",
            "longreadplatform",
            "barcode",
            "limsid",
            "processingstatus",
            "translocationspeed",
            "minimumreadlength",
            "mappedreadspct",
            "meancoverage",
            "genomecoverage",
            "readerrorrate",
            "readlengthn50",
            "meanreadquality",
            "alignedq10bases",
            "contamination",
            "arrayconcordance",
            "sexconcordance",
            "sexploidy",
            "samplesource",
            "genometype"
        )

        self.LR_L2_PB_CCS_SCHEMA = (
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "limsid",
            "aggregationlevel",
            "flowcellid",
            "barcode",
            "meancoverage",
            "genomecoverage",
            "contamination",
            "sexconcordance",
            "sexploidy",
            "alignedhifibases",
            "readerrorrate",
            "numhifireads",
            "readlengthmean",
            "arrayconcordance",
            "samplesource",
            "mappedreadspct",
            "genometype",
            "processingstatus",
            "bampath",
            "longreadplatform",
            "instrument",
            "smrtlinkserverversion",
            "instrumenticsversion"
        )

        self.LR_L4_SCHEMA = (
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "aggregationlevel",
            "flowcellid",
            "barcode",
            "lrsiteid",
            "longreadplatform",
            "sexatbirth",
            "bampath",
            "drccontamination",
            "drcsexconcordance",
            "drcarrayconcordance",
            "drcmeancoverage",
            "drcprocessingstatus",
            "drcfailuremode",
            "drcfailuremodedesc",
            "drcprocessingcount",
            "passtoresearchpipeline",
        )

        self.LR_L5_SCHEMA = (
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "flowcellid",
            "barcode",
            "lrsiteid",
            "longreadplatform",
        )

        self.LR_L6_SCHEMA = (
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "lrsiteid",
            "longreadplatform",
            "sexatbirth",
            "drccontamination",
            "drcsexconcordance",
            "drcarrayconcordance",
            "drcmeancoverage",
            "drcprocessingstatus",
            "drcfailuremode",
            "drcfailuremodedesc",
            "drcprocessingcount",
            "passtoresearchpipeline"
        )

        # PR pipeline
        self.PR_PR_SCHEMA = (
            "biobankid",
            "genometype",
            "psiteid",
        )

        self.PR_P1_SCHEMA = (
            "packageid",
            "biobankidsampleid",
            "boxstorageunitid",
            "boxidplateid",
            "wellposition",
            "sampleid",
            "parentsampleid",
            "collectiontubeid",
            "matrixid",
            "collectiondate",
            "biobankid",
            "sexatbirth",
            "age",
            "nystateyn",
            "sampletype",
            "treatments",
            "quantityul",
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

        self.PR_P2_SCHEMA = (
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "limsid",
            "samplesource",
            "genometype",
            "softwareversion",
            "npxexplorepath",
            "analysisreportpath",
            "kittype",
            "notes"
        )

        # RNA pipeline
        self.RNA_RR_SCHEMA = (
            "biobankid",
            "genometype",
            "rsiteid",
        )

        self.RNA_R1_SCHEMA = (
            "packageid",
            "biobankidsampleid",
            "boxstorageunitid",
            "boxidplateid",
            "wellposition",
            "sampleid",
            "parentsampleid",
            "collectiontubeid",
            "matrixid",
            "collectiondate",
            "biobankid",
            "sexatbirth",
            "age",
            "nystateyn",
            "sampletype",
            "treatments",
            "quantityul",
            "totalconcentrationngul",
            "totalyieldng",
            "rqs",
            "260230",
            "260280",
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

        self.RNA_R2_SCHEMA = (
            "biobankid",
            "sampleid",
            "biobankidsampleid",
            "limsid",
            "samplesource",
            "alignmentratepct",
            "duplicationpct",
            "mrnabasespct",
            "readsalignedinpairs",
            "ribosomalbasespct",
            "mediancvcoverage",
            "meaninsertsize",
            "rqs",
            "genometype",
            "processingstatus",
            "pipelineid",
            "crampath",
            "crammd5path",
            "notes"
        )

        self.values_for_validation = {
            GenomicJob.METRICS_INGESTION: {
                GENOME_TYPE_ARRAY: {
                    'pipelineid': [
                        'cidr_egt_1',
                        'original_egt'
                    ]
                },
                GENOME_TYPE_WGS: {
                    'pipelineid': [
                        config.GENOMIC_DEPRECATED_WGS_DRAGEN,
                        config.GENOMIC_UPDATED_WGS_DRAGEN
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
        # AW pipeline
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
                len(filename_components) <= 6 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in ('seq', 'gen') and
                re.search(r"pkg-[0-9]{4}-[0-9]{5,}$",
                          filename_components[3]) is not None and
                filename_components[4] in ('failure', 'failure.csv') and
                filename.lower().endswith('csv')
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

        # CVL pipeline
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

        # Long read pipeline
        def lr_lr_manifest_name_rule():
            """
            LR LR manifest name rule
            """
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'lr' and
                filename_components[3] == 'requests' and
                filename.lower().endswith('csv')
            )

        def lr_l1_manifest_name_rule():
            """
            LR L1 manifest name rule
            """
            return (
                len(filename_components) <= 5 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'lr' and
                'pkg' in filename_components[3] and
                filename.lower().endswith('csv')
            )

        def lr_l1f_manifest_name_rule():
            """
            LR L1F manifest name rule
            """
            return (
                len(filename_components) <= 5 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'l1f' and
                'pkg' in filename_components[3] and
                filename.lower().endswith('csv')
            )

        def lr_l2_ont_manifest_name_rule():
            """
            LR L2 ONT manifest name rule
            """
            return (
                len(filename_components) <= 7 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'l2' and
                filename_components[4] == 'ont' and
                filename.lower().endswith('csv')
            )

        def lr_l2_pb_ccs_manifest_name_rule():
            """
            LR L2 PB CCS manifest name rule
            """
            return (
                len(filename_components) <= 7 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'l2' and
                filename_components[4] == 'pbccs' and
                filename.lower().endswith('csv')
            )

        def lr_l4_manifest_name_rule():
            """
            LR L4/L4F manifest name rule
            """
            return (
                len(filename_components) <= 4 and
                filename_components[0] == 'aou' and
                filename_components[1] in ['l4', 'l4f'] and
                filename.lower().endswith('csv')
            )

        def lr_l5_manifest_name_rule():
            """
            LR L5 manifest name rule
            """
            return (
                len(filename_components) <= 4 and
                filename_components[0] == 'aou' and
                filename_components[1] == 'l5' and
                filename.lower().endswith('csv')
            )

        def lr_l6_manifest_name_rule():
            """
            LR L6/L6F manifest name rule
            """
            return (
                len(filename_components) <= 4 and
                filename_components[0] == 'aou' and
                filename_components[1] in ['l6', 'l6f'] and
                filename.lower().endswith('csv')
            )

        # PR pipeline
        def pr_pr_manifest_name_rule():
            """
            PR PR manifest name rule
            """
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'pr' and
                filename_components[3] == 'requests' and
                filename.lower().endswith('csv')
            )

        def pr_p1_manifest_name_rule():
            """
            PR P1 manifest name rule
            """
            return (
                len(filename_components) == 4 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'proteomics' and
                'pkg' in filename_components[3] and
                filename.lower().endswith('csv')
            )

        def pr_p2_manifest_name_rule():
            """
            PR P2 manifest name rule
            """
            return (
                len(filename_components) >= 4 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'p2' and
                filename.lower().endswith('csv')
            )

        # RNA pipeline
        def rna_rr_manifest_name_rule():
            """
            RNA PR manifest name rule
            """
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'rr' and
                filename_components[3] == 'requests' and
                filename.lower().endswith('csv')
            )

        def rna_r1_manifest_name_rule():
            """
            RNA P1 manifest name rule
            """
            return (
                len(filename_components) == 4 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'rnaseq' and
                'pkg' in filename_components[3] and
                filename.lower().endswith('csv')
            )

        def rna_r2_manifest_name_rule():
            """
            RNA R2 manifest name rule
            """
            return (
                len(filename_components) == 6 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] == 'r2' and
                'v' in filename_components[-1] and
                filename.lower().endswith('csv')
            )

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
            GenomicJob.CVL_W5NF_WORKFLOW: cvl_w5nf_manifest_name_rule,
            GenomicJob.LR_LR_WORKFLOW: lr_lr_manifest_name_rule,
            GenomicJob.LR_L1_WORKFLOW: lr_l1_manifest_name_rule,
            GenomicJob.LR_L1F_WORKFLOW: lr_l1f_manifest_name_rule,
            GenomicJob.LR_L2_ONT_WORKFLOW: lr_l2_ont_manifest_name_rule,
            GenomicJob.LR_L2_PB_CCS_WORKFLOW: lr_l2_pb_ccs_manifest_name_rule,
            GenomicJob.LR_L4_WORKFLOW: lr_l4_manifest_name_rule,
            GenomicJob.LR_L4F_WORKFLOW: lr_l4_manifest_name_rule,
            GenomicJob.LR_L5_WORKFLOW: lr_l5_manifest_name_rule,
            GenomicJob.LR_L6_WORKFLOW: lr_l6_manifest_name_rule,
            GenomicJob.LR_L6F_WORKFLOW: lr_l6_manifest_name_rule,
            GenomicJob.PR_PR_WORKFLOW: pr_pr_manifest_name_rule,
            GenomicJob.PR_P1_WORKFLOW: pr_p1_manifest_name_rule,
            GenomicJob.PR_P2_WORKFLOW: pr_p2_manifest_name_rule,
            GenomicJob.RNA_RR_WORKFLOW: rna_rr_manifest_name_rule,
            GenomicJob.RNA_R1_WORKFLOW: rna_r1_manifest_name_rule,
            GenomicJob.RNA_R2_WORKFLOW: rna_r2_manifest_name_rule
        }

        try:
            return ingestion_name_rules.get(self.job_id)()
        except KeyError:
            return GenomicSubProcessResult.ERROR

    def validate_values(self, data):
        is_invalid, message = False, None
        cleaned_fieldnames = [
            self._clean_field_name(fieldname) for fieldname in data['fieldnames']
        ]

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
                self.genome_type = GENOME_TYPE_ARRAY
                return self.AW5_WGS_SCHEMA
            if self.job_id == GenomicJob.AW5_ARRAY_MANIFEST:
                self.genome_type = GENOME_TYPE_WGS
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
            if self.job_id == GenomicJob.LR_LR_WORKFLOW:
                return self.LR_LR_SCHEMA
            if self.job_id in [
                GenomicJob.LR_L1_WORKFLOW,
                GenomicJob.LR_L1F_WORKFLOW
            ]:
                return self.LR_L1_SCHEMA
            if self.job_id == GenomicJob.LR_L2_ONT_WORKFLOW:
                return self.LR_L2_ONT_SCHEMA
            if self.job_id == GenomicJob.LR_L2_PB_CCS_WORKFLOW:
                return self.LR_L2_PB_CCS_SCHEMA
            if self.job_id in [
                GenomicJob.LR_L4_WORKFLOW,
                GenomicJob.LR_L4F_WORKFLOW
            ]:
                return self.LR_L4_SCHEMA
            if self.job_id == GenomicJob.LR_L5_WORKFLOW:
                return self.LR_L5_SCHEMA
            if self.job_id in [
                GenomicJob.LR_L6_WORKFLOW,
                GenomicJob.LR_L6F_WORKFLOW
            ]:
                return self.LR_L6_SCHEMA
            if self.job_id == GenomicJob.PR_PR_WORKFLOW:
                return self.PR_PR_SCHEMA
            if self.job_id == GenomicJob.PR_P1_WORKFLOW:
                return self.PR_P1_SCHEMA
            if self.job_id == GenomicJob.PR_P2_WORKFLOW:
                return self.PR_P2_SCHEMA
            if self.job_id == GenomicJob.RNA_RR_WORKFLOW:
                return self.RNA_RR_SCHEMA
            if self.job_id == GenomicJob.RNA_R1_WORKFLOW:
                return self.RNA_R1_SCHEMA
            if self.job_id == GenomicJob.RNA_R2_WORKFLOW:
                return self.RNA_R2_SCHEMA

        except (IndexError, KeyError):
            return GenomicSubProcessResult.ERROR


class GenomicReconciler:
    """ This component handles reconciliation between genomic datasets """

    def __init__(self, run_id, job_id):

        self.run_id = run_id
        self.job_id = job_id
        self.member_dao = GenomicSetMemberDao()

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

    def create_new_genomic_participants(self):
        """
        This method determines which samples to enter into the genomic system
        from Cohort 3 (New Participants).
        Validation is handled in the query that retrieves the newly consented
        participants' samples to process.
        :param: from_date : the date from which to lookup new biobank_ids
        :return: result
        """
        samples = self._get_new_biobank_samples()

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

    def _get_new_biobank_samples(self):
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
        self.short_read_dao = GenomicShortReadDao()
        self.long_read_dao = GenomicLongReadDao()
        self.cvl_dao = GenomicCVLDao()
        self.pr_dao = GenomicPRDao()
        self.rna_dao = GenomicRNADao()

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
            GenomicManifestTypes.LR_L0: (
                'biobank_id',
                'collection_tube_id',
                'sex_at_birth',
                'genome_type',
                'ny_flag',
                'validation_passed',
                'ai_an',
                'parent_tube_id',
                'lr_site_id',
                'long_read_platform'
            ),
            GenomicManifestTypes.LR_L3: (
                'biobank_id',
                'sample_id',
                'biobankid_sampleid',
                'flowcell_id',
                'barcode',
                'long_read_platform',
                'bam_path',
                'sex_at_birth',
                'lr_site_id',
                'sample_source',
                'gc_processing_status',
                'fragment_length',
                'pacbio_instrument_type',
                'smrtlink_server_version',
                'pacbio_instrument_ics_version',
                'gc_read_error_rate',
                'gc_mean_coverage',
                'gc_genome_coverage',
                'gc_contamination',
                'ont_basecaller_version',
                'ont_basecaller_model',
                'ont_mean_read_qual'
            ),
            GenomicManifestTypes.PR_P0: (
                'biobank_id',
                'collection_tube_id',
                'sex_at_birth',
                'genome_type',
                'ny_flag',
                'validation_passed',
                'ai_an',
                'p_site_id',
            ),
            GenomicManifestTypes.RNA_R0: (
                'biobank_id',
                'collection_tube_id',
                'sex_at_birth',
                'genome_type',
                'ny_flag',
                'validation_passed',
                'ai_an',
                'r_site_id',
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
                'query': self.cvl_dao.get_data_ready_for_w1il_manifest,
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
                'query': self.cvl_dao.get_data_ready_for_w1il_manifest,
                'params': {
                    'module': 'hdr',
                    'cvl_id': self.cvl_site_id
                }
            },
            GenomicManifestTypes.CVL_W2W: {
                'job_run_field': 'cvlW2wJobRunId',
                'output_filename':
                    f'{CVL_W2W_MANIFEST_SUBFOLDER}/{self.cvl_site_id.upper()}_AoU_CVL_W2W_{now_formatted}.csv',
                'query': self.cvl_dao.get_data_ready_for_w2w_manifest,
                'params': {
                    'cvl_id': self.cvl_site_id
                }
            },
            GenomicManifestTypes.CVL_W3SR: {
                'job_run_field': 'cvlW3srManifestJobRunID',
                'output_filename': f'{CVL_W3SR_MANIFEST_SUBFOLDER}/{self.cvl_site_id.upper()}_AoU_CVL_W3SR'
                                   f'_{now_formatted}.csv',
                'query': self.cvl_dao.get_w3sr_records,
                'params': {
                    'site_id': self.cvl_site_id
                }
            },
            GenomicManifestTypes.AW3_ARRAY: {
                'job_run_field': 'aw3ManifestJobRunID',
                'output_filename': f'{GENOMIC_AW3_ARRAY_SUBFOLDER}/AoU_DRCV_GEN_{now_formatted}.csv',
                'signal': 'bypass',
                'query': self.short_read_dao.get_aw3_array_records,
                'params': {
                    'genome_type': self.genome_type
                }
            },
            GenomicManifestTypes.AW3_WGS: {
                'job_run_field': 'aw3ManifestJobRunID',
                'output_filename': f'{self.kwargs.get("pipeline_id")}/'
                                   f'{GENOMIC_AW3_WGS_SUBFOLDER}/AoU_DRCV_SEQ_{now_formatted}.csv',
                'signal': 'bypass',
                'query': self.short_read_dao.get_aw3_wgs_records,
                'params': {
                    'genome_type': self.genome_type,
                    'pipeline_id': self.kwargs.get('pipeline_id')
                }
            },
            GenomicManifestTypes.AW2F: {
                'job_run_field': 'aw2fManifestJobRunID',
                'output_filename': f'{BIOBANK_AW2F_SUBFOLDER}/GC_AoU_DataType_PKG-YYMM-xxxxxx_contamination.csv',
                'signal': 'bypass'
            },
            GenomicManifestTypes.LR_L0: {
                'output_filename':
                    f'{LR_L0_MANIFEST_SUBFOLDER}/LongRead-Manifest-AoU-{self.kwargs.get("long_read_max_set")}'
                    f'-{now_formatted}.csv',
                'query': self.long_read_dao.get_manifest_zero_records_from_max_set
            },
            GenomicManifestTypes.LR_L3: {
                'output_filename':
                    f'{LR_L3_MANIFEST_SUBFOLDER}/AoU_L3_'
                    f'{now_formatted}.csv',
                'query': self.long_read_dao.get_manifest_three_records
            },
            GenomicManifestTypes.PR_P0: {
                'output_filename':
                    f'{PR_P0_MANIFEST_SUBFOLDER}/Proteomics-Manifest-AoU-{self.kwargs.get("pr_max_set")}'
                    f'-{now_formatted}.csv',
                'query': self.pr_dao.get_manifest_zero_records_from_max_set
            },
            GenomicManifestTypes.RNA_R0: {
                'output_filename':
                    f'{RNA_R0_MANIFEST_SUBFOLDER}/RNASeq-Manifest-AoU-{self.kwargs.get("rna_max_set")}'
                    f'-{now_formatted}.csv',
                'query': self.rna_dao.get_manifest_zero_records_from_max_set
            },
        }
        def_config = def_config.get(manifest_type)
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

    def generate_and_transfer_manifest(
        self,
        manifest_type,
        genome_type,
        version=None,
        **kwargs
    ):
        """
        Main execution method for ManifestCompiler
        :return: result dict:
            "code": (i.e. SUCCESS)
            "feedback_file": None or feedback file record to update,
            "record_count": integer
        """

        def _extract_member_ids_update(obj_list: List[dict]) -> List[int]:
            if self.controller.job_id in [
                GenomicJob.LR_L0_WORKFLOW,
                GenomicJob.PR_P0_WORKFLOW,
                GenomicJob.RNA_R0_WORKFLOW
            ]:
                return []

            update_member_ids = [
                obj.genomic_set_member_id for obj in
                obj_list if hasattr(obj, 'genomic_set_member_id')
            ]
            if update_member_ids:
                return update_member_ids

            sample_ids = [obj.sampleId if hasattr(obj, 'sampleId') else obj.sample_id for obj in obj_list]
            update_member_ids = self.member_dao.get_member_ids_from_sample_ids(sample_ids, genome_type)
            return [update_member_id.id for update_member_id in update_member_ids]

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

        all_member_ids = []
        if self.max_num and len(source_data) > self.max_num:
            current_list, count = [], 0

            for obj in source_data:
                current_list.append(obj)
                if len(current_list) == self.max_num:
                    count += 1
                    self.output_file_name = self.manifest_def.output_filename
                    self.output_file_name = f'{self.output_file_name.split(".csv")[0]}_{count}.csv'
                    file_path = f'{self.manifest_def.destination_bucket}/{self.output_file_name}'
                    member_ids = _extract_member_ids_update(current_list)
                    all_member_ids.extend(member_ids)

                    logging.info(
                        f'Preparing manifest of type {manifest_type}...'
                        f'{file_path}'
                    )

                    self._write_and_upload_manifest(current_list)
                    self.controller.manifests_generated.append({
                        'file_path': file_path,
                        'record_count': len(current_list),
                        'member_ids': member_ids
                    })
                    current_list.clear()

            if current_list:
                count += 1
                self.output_file_name = self.manifest_def.output_filename
                self.output_file_name = f'{self.output_file_name.split(".csv")[0]}_{count}.csv'
                file_path = f'{self.manifest_def.destination_bucket}/{self.output_file_name}'
                member_ids = _extract_member_ids_update(current_list)
                all_member_ids.extend(member_ids)

                logging.info(
                    f'Preparing manifest of type {manifest_type}...'
                    f'{file_path}'
                )

                self._write_and_upload_manifest(current_list)
                self.controller.manifests_generated.append({
                    'file_path': file_path,
                    'record_count': len(current_list),
                    'member_ids': member_ids
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
            member_ids = _extract_member_ids_update(source_data)
            all_member_ids.extend(member_ids)

            logging.info(
                f'Preparing manifest of type {manifest_type}...'
                f'{file_path}'
            )

            self._write_and_upload_manifest(source_data)
            self.controller.manifests_generated.append({
                'file_path': file_path,
                'record_count': len(source_data),
                'member_ids': member_ids
            })

        for member in self.member_dao.get_members_from_member_ids(all_member_ids):
            # member workflow states
            if self.manifest_def.signal != "bypass":
                # genomic workflow state
                new_wf_state = GenomicStateHandler.get_new_state(
                    member.genomicWorkflowState,
                    signal=self.manifest_def.signal
                )
                if new_wf_state or new_wf_state != member.genomicWorkflowState:
                    self.member_dao.update_member_workflow_state(member, new_wf_state)

        # Updates job run field on set member
        if self.manifest_def.job_run_field and all_member_ids:
            self.controller.execute_cloud_task({
                'member_ids': list(set(all_member_ids)),
                'field': self.manifest_def.job_run_field,
                'value': self.run_id,
                'is_job_run': True
            }, 'genomic_set_member_update_task')

        return {"code": GenomicSubProcessResult.SUCCESS}

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
            GenomicManifestTypes.AW3_WGS,
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
            exporter, manifest_data = SqlExporter(self.bucket_name), []
            # filter data based on excluded columns
            if len(source_data[0]) > len(self.manifest_def.columns):
                for row in source_data:
                    manifest_data.append(
                        tuple(x for i, x in enumerate(row, start=1) if i <= len(
                            self.manifest_def.columns))
                    )

            manifest_data = source_data if not len(manifest_data) else manifest_data
            with exporter.open_cloud_writer(self.output_file_name) as writer:
                writer.write_header(self.manifest_def.columns)
                writer.write_rows(manifest_data)

            return GenomicSubProcessResult.SUCCESS

        except RuntimeError:
            return GenomicSubProcessResult.ERROR
