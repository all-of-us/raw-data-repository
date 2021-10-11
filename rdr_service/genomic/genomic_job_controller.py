"""
This module tracks and validates the status of Genomics Pipeline Subprocesses.
"""
import logging
from datetime import datetime

import pytz
from sendgrid import sendgrid
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from rdr_service import clock, config
from rdr_service.api_util import list_blobs
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.config import (
    GAE_PROJECT,
    GENOMIC_GC_METRICS_BUCKET_NAME,
    getSetting,
    getSettingList,
    GENOME_TYPE_ARRAY,
    MissingConfigException,
    RDR_SLACK_WEBHOOKS
)
from rdr_service.dao.bq_genomics_dao import bq_genomic_job_run_update, bq_genomic_file_processed_update, \
    bq_genomic_manifest_file_update, bq_genomic_manifest_feedback_update, \
    bq_genomic_gc_validation_metrics_batch_update, bq_genomic_set_member_batch_update, \
    bq_genomic_gc_validation_metrics_update
from rdr_service.genomic.genomic_data_quality_components import ReportingComponent
from rdr_service.genomic.genomic_mappings import raw_aw1_to_genomic_set_member_fields, \
    raw_aw2_to_genomic_set_member_fields, genomic_data_file_mappings, genome_centers_id_from_bucket_array, \
    wgs_file_types_attributes, array_file_types_attributes
from rdr_service.genomic.genomic_set_file_handler import DataError
from rdr_service.genomic.genomic_state_handler import GenomicStateHandler
from rdr_service.model.genomics import GenomicManifestFile, GenomicManifestFeedback, GenomicIncident, \
    GenomicGCValidationMetrics, GenomicInformingLoop, GenomicGcDataFile
from rdr_service.genomic_enums import GenomicJob, GenomicWorkflowState, GenomicSubProcessStatus, \
    GenomicSubProcessResult, GenomicIncidentCode, GenomicManifestTypes
from rdr_service.genomic.genomic_job_components import (
    GenomicFileIngester,
    GenomicReconciler,
    GenomicBiobankSamplesCoupler,
    ManifestCompiler,
)
from rdr_service.dao.genomics_dao import (
    GenomicFileProcessedDao,
    GenomicJobRunDao,
    GenomicManifestFileDao,
    GenomicManifestFeedbackDao,
    GenomicIncidentDao,
    GenomicSetMemberDao,
    GenomicAW1RawDao,
    GenomicAW2RawDao,
    GenomicGCValidationMetricsDao,
    GenomicInformingLoopDao,
    GenomicGcDataFileDao,
    GenomicGcDataFileMissingDao,
    GcDataFileStagingDao)
from rdr_service.resource.generators.genomics import genomic_job_run_update, genomic_file_processed_update, \
    genomic_manifest_file_update, genomic_manifest_feedback_update, genomic_gc_validation_metrics_batch_update, \
    genomic_set_member_batch_update
from rdr_service.services.slack_utils import SlackMessageHandler


class GenomicJobController:
    """This class controls the tracking of Genomics subprocesses"""

    def __init__(
        self,
        job_id,
        bucket_name=GENOMIC_GC_METRICS_BUCKET_NAME,
        sub_folder_name=None,
        sub_folder_tuple=None,
        archive_folder_name=None,
        bucket_name_list=None,
        storage_provider=None,
        bq_project_id=None,
        task_data=None,
        server_config=None,
        max_num=None
    ):

        self.job_id = job_id
        self.job_run = None
        self.bucket_name = getSetting(bucket_name, default="")
        self.sub_folder_name = getSetting(sub_folder_name, default=sub_folder_name)
        self.sub_folder_tuple = sub_folder_tuple
        self.bucket_name_list = getSettingList(bucket_name_list, default=[])
        self.archive_folder_name = archive_folder_name
        self.bq_project_id = bq_project_id
        self.task_data = task_data
        self.bypass_record_count = False
        self.skip_updates = False
        self.server_config = server_config
        self.subprocess_results = set()
        self.job_result = GenomicSubProcessResult.UNSET
        self.last_run_time = datetime(2019, 11, 5, 0, 0, 0)
        self.max_num = max_num
        self.member_ids_for_update = []
        self.manifests_generated = []

        # Components
        self.job_run_dao = GenomicJobRunDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.manifest_file_dao = GenomicManifestFileDao()
        self.manifest_feedback_dao = GenomicManifestFeedbackDao()
        self.incident_dao = GenomicIncidentDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.member_dao = GenomicSetMemberDao()
        self.informing_loop_dao = GenomicInformingLoopDao()
        self.missing_files_dao = GenomicGcDataFileMissingDao()
        self.ingester = None
        self.file_mover = None
        self.reconciler = None
        self.biobank_coupler = None
        self.manifest_compiler = None
        self.staging_dao = None
        self.storage_provider = storage_provider
        self.genomic_alert_slack = SlackMessageHandler(
            webhook_url=config.getSettingJson(RDR_SLACK_WEBHOOKS).get('rdr_genomic_alerts')
        )

    def __enter__(self):
        logging.info(f'Beginning {self.job_id.name} workflow')
        self.job_run = self._create_run(self.job_id)
        self.last_run_time = self._get_last_successful_run_time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._end_run()

    def insert_genomic_manifest_file_record(self):
        """
        Inserts genomic_manifest_file record from _file_data dict
        :return: GenomicManifestFile object
        """

        # Set attributes for GenomicManifestFile
        now = datetime.utcnow()

        try:
            _uploadDate = self.task_data.file_data.upload_date
            _manifest_type = self.task_data.file_data.manifest_type
            _file_path = self.task_data.file_data.file_path

        except AttributeError:
            raise AttributeError("upload_date, manifest_type, and file_path required")

        manifest_file = self.manifest_file_dao.get_manifest_file_from_filepath(_file_path)

        if manifest_file is None:
            path_list = _file_path.split('/')
            file_to_insert = GenomicManifestFile(
                created=now,
                modified=now,
                uploadDate=_uploadDate,
                manifestTypeId=_manifest_type,
                filePath=_file_path,
                bucketName=path_list[0],
                recordCount=0,  # Initializing with 0, counting records when processing file
                rdrProcessingComplete=0,
                fileName=path_list[-1]
            )

            manifest_file = self.manifest_file_dao.insert(file_to_insert)

            bq_genomic_manifest_file_update(manifest_file.id, self.bq_project_id)
            genomic_manifest_file_update(manifest_file.id)

        return manifest_file

    def insert_genomic_manifest_feedback_record(self, manifest_file):
        """
        Inserts run record from _file_data dict
        :param manifest_file: JSONObject
        :return: GenomicManifestFeedbackobject
        """

        # Set attributes for GenomicManifestFile
        now = datetime.utcnow()

        feedback_file = self.manifest_feedback_dao.get_feedback_record_from_manifest_id(manifest_file.id)

        if feedback_file is None:
            feedback_to_insert = GenomicManifestFeedback(
                created=now,
                modified=now,
                inputManifestFileId=manifest_file.id,
                feedbackRecordCount=0,
                feedbackComplete=0,
                ignore=0,
            )

            feedback_file = self.manifest_feedback_dao.insert(feedback_to_insert)

            bq_genomic_manifest_feedback_update(feedback_file.id, self.bq_project_id)
            genomic_manifest_feedback_update(feedback_file.id)

        return feedback_file

    def get_feedback_records_to_send(self, _num=60):
        """
        Retrieves genomic_manifest_feedback records that are past _num.
        :return: list of GenomicManifestFeedback records
        """
        return self.manifest_feedback_dao.get_feedback_records_past_date_cutoff(num_days=_num)

    def get_aw2f_remainder_records(self):
        """
        Retrieves genomic_manifest_feedback records that have already been sent
        but have remaining data to send
        :return: list of GenomicManifestFeedback records
        """
        ids = self.manifest_feedback_dao.get_contamination_remainder_feedback_ids()
        return self.manifest_feedback_dao.get_feedback_records_from_ids(ids)

    def ingest_awn_data_for_member(self, file_path, member):
        """
        Executed from genomic tools. Ingests data for a single GenomicSetMember
        Currently supports AW1 and AW2
        :param file_path:
        :param member:
        :return:
        """
        print(f"Ingesting member ID {member.id} data for file: {file_path}")

        # Get max file-processed ID for filename
        file_processed = self.file_processed_dao.get_max_file_processed_for_filepath(file_path)

        if file_processed is not None:
            # Use ingester to ingest 1 row from file
            self.ingester = GenomicFileIngester(job_id=self.job_id,
                                                job_run_id=self.job_run.id,
                                                _controller=self,
                                                target_file=file_path[1:])  # strip leading "/"

            self.ingester.file_obj = file_processed
            self.job_result = GenomicSubProcessResult.SUCCESS

            if self.job_id == GenomicJob.AW1_MANIFEST:
                self.job_result = self.ingester.ingest_single_aw1_row_for_member(member)

            if self.job_id == GenomicJob.METRICS_INGESTION:
                self.job_result = self.ingester.ingest_single_aw2_row_for_member(member)

        else:
            print(f'No file processed IDs for {file_path}')

    def ingest_gc_metrics(self):
        """
        Uses ingester to ingest files.
        """
        try:
            logging.info('Running Validation Metrics Ingestion Workflow.')

            for gc_bucket_name in self.bucket_name_list:
                for folder in self.sub_folder_tuple:
                    self.sub_folder_name = config.getSetting(folder)
                    self.ingester = GenomicFileIngester(job_id=self.job_id,
                                                        job_run_id=self.job_run.id,
                                                        bucket=gc_bucket_name,
                                                        sub_folder=self.sub_folder_name,
                                                        _controller=self)
                    self.subprocess_results.add(
                        self.ingester.generate_file_queue_and_do_ingestion()
                    )

            self.job_result = self._aggregate_run_results()

        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def ingest_specific_manifest(self, filename):
        """
        Uses GenomicFileIngester to ingest specific Manifest file.
        """
        try:
            self.ingester = GenomicFileIngester(job_id=self.job_id,
                                                job_run_id=self.job_run.id,
                                                bucket=self.bucket_name,
                                                target_file=filename,
                                                _controller=self)

            self.job_result = self.ingester.generate_file_queue_and_do_ingestion()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def ingest_member_ids_from_awn_raw_table(self, member_ids):
        """
        Pulls data from genomic_aw1_raw or genomic_aw2_raw based on the value
        of self.job_id.
        In the case of GenomicJob.AW1_MANIFEST, this loads AW1 data to genomic_set_member.
        In the case of GenomicJob.METRICS_INGESTION, this loads AW2 data to genomic_set_member
        and genomic_gc_validation_metrics.

        :param member_ids: list of genomic_set_member_ids to ingest
        :return: ingestion results as string
        """

        if self.job_id not in [GenomicJob.AW1_MANIFEST, GenomicJob.METRICS_INGESTION]:
            raise AttributeError(f"{self.job_id.name} is invalid for this workflow")

        if self.job_id == GenomicJob.AW1_MANIFEST:
            raw_dao = GenomicAW1RawDao()

        else:
            raw_dao = GenomicAW2RawDao()

        # Get member records
        members = self.member_dao.get_members_from_member_ids(member_ids)
        update_recs = []
        completed_members = []
        multiples = []
        missing = []
        metrics = []  # for PDR inserts

        for member in members:
            if not member.biobankId.startswith("HG"):
                # add prefix to biobank_id
                try:
                    pre = self.server_config[config.BIOBANK_ID_PREFIX][0]
                except KeyError:
                    # Set default for unit tests
                    pre = "A"
                bid = f"{pre}{member.biobankId}"
            else:
                bid = member.biobankId
            # Get Raw AW1 Records for biobank IDs and genome_type
            try:
                raw_rec = raw_dao.get_raw_record_from_bid_genome_type(
                    biobank_id=bid,
                    genome_type=member.genomeType
                )
            except MultipleResultsFound:
                multiples.append(member.id)
            except NoResultFound:
                missing.append(member.id)
            else:
                update_recs.append((member, raw_rec))

        if update_recs:
            # Get unique file_paths
            paths = self.get_unique_file_paths_for_raw_records([rec[1] for rec in update_recs])
            file_proc_map = self.map_file_paths_to_fp_id(paths)
            # Process records
            with self.member_dao.session() as session:
                for record_to_update in update_recs:
                    # AW1
                    if self.job_id == GenomicJob.AW1_MANIFEST:
                        self.set_rdr_aw1_attributes_from_raw(record_to_update, file_proc_map)
                        self.set_aw1_attributes_from_raw(record_to_update)
                    # AW2
                    elif self.job_id == GenomicJob.METRICS_INGESTION:
                        self.preprocess_aw2_attributes_from_raw(record_to_update, file_proc_map)
                        metrics_obj = self.set_validation_metrics_from_raw(record_to_update)
                        metrics_obj = session.merge(metrics_obj)
                        session.commit()
                        metrics.append(metrics_obj.id)

                    session.merge(record_to_update[0])
                    completed_members.append(record_to_update[0].id)
            # BQ Updates
            if self.job_id == GenomicJob.METRICS_INGESTION:
                # Metrics
                bq_genomic_gc_validation_metrics_batch_update(metrics, project_id=self.bq_project_id)
                genomic_gc_validation_metrics_batch_update(metrics)

            # Members
            bq_genomic_set_member_batch_update(metrics, project_id=self.bq_project_id)
            genomic_set_member_batch_update(completed_members)

        return self.compile_raw_ingestion_results(
            completed_members,
            missing,
            multiples,
            metrics
        )

    def ingest_data_files_into_gc_metrics(self, file_path, bucket_name):
        try:
            logging.info(f'Inserting data file: {file_path}')

            data_file = file_path.split('/')[-1]
            sample_id = data_file.split('_')[2]
            member = self.member_dao.get_member_from_sample_id(sample_id)
            metrics = None if not member else self.metrics_dao.get_metrics_by_member_id(member.id)

            if metrics:
                ext = file_path.split('.', 1)[-1]
                attrs = []
                for key, value in genomic_data_file_mappings.items():
                    if ext in value['file_ext']:
                        attrs = genomic_data_file_mappings[key]['model_attrs']
                        break

                if attrs:
                    for value in attrs:
                        if 'Path' in value:
                            metrics.__setattr__(value, f'{bucket_name}/{file_path}')
                        else:
                            metrics.__setattr__(value, 1)

                    metrics_obj = self.metrics_dao.upsert(metrics)
                    bq_genomic_gc_validation_metrics_update(metrics_obj.id, project_id=self.bq_project_id)
                    bq_genomic_gc_validation_metrics_update(metrics_obj.id)
            else:
                message = f'{self.job_id.name}: Cannot find genomics metric record for sample id: {sample_id}'
                logging.warning(message)
                self.create_incident(
                    source_job_run_id=self.job_run.id,
                    code=GenomicIncidentCode.UNABLE_TO_FIND_METRIC.name,
                    message=message,
                    sample_id=sample_id if sample_id else '',
                    data_file_path=file_path
                )
        except RuntimeError:
            logging.warning('Inserting data file failure')

    def ingest_informing_loop_records(self, *, loop_type, records):
        if records:
            logging.info(f'Inserting informing loop for Participant: {records[0].participantId}')

            module_type = [obj for obj in records if obj.fieldName == 'module_type' and obj.valueString]
            decision_value = [obj for obj in records if obj.fieldName == 'decision_value' and obj.valueString]

            loop_obj = GenomicInformingLoop(
                participant_id=records[0].participantId,
                message_record_id=records[0].messageRecordId,
                event_type=loop_type,
                event_authored_time=records[0].eventAuthoredTime,
                module_type=module_type[0].valueString if module_type else None,
                decision_value=decision_value[0].valueString if decision_value else None,
            )

            self.informing_loop_dao.insert(loop_obj)

    def accession_data_files(self, file_path, bucket_name):
        data_file_dao = GenomicGcDataFileDao()

        if data_file_dao.get_with_file_path(file_path):
            logging.info(f'{file_path} already exists.')
            return 0

        # split file name
        file_attrs = self.parse_data_file_path(file_path)

        # get GC
        gc_id = self.get_gc_site_for_data_file(bucket_name, file_path,
                                               file_attrs['name_components'])

        # Insert record
        data_file_record = GenomicGcDataFile(
            file_path=file_path,
            gc_site_id=gc_id,
            bucket_name=bucket_name,
            file_prefix=file_attrs['file_prefix'],
            file_name=file_attrs['file_name'],
            file_type=file_attrs['file_type'],
            identifier_type=file_attrs['identifier_type'],
            identifier_value=file_attrs['identifier_value'],
        )

        data_file_dao.insert(data_file_record)

    def parse_data_file_path(self, file_path):

        path_components = file_path.split('/')
        name_components = path_components[-1].split("_")

        # Set ID type and Value
        id_type, id_value = self.set_identifier_fields(file_path, name_components)

        # Set file type
        if "idat" in file_path.lower():
            file_type = name_components[-1]
        else:
            file_type = ".".join(name_components[-1].split('.')[1:])

        attr_dict = {
            'path_components': path_components,
            'name_components': name_components,
            'file_prefix': "/".join(path_components[1:-1]),
            'file_name': path_components[-1],
            'file_type': file_type,
            'identifier_type': id_type,
            'identifier_value': id_value
        }

        return attr_dict

    @staticmethod
    def set_identifier_fields(file_path: str, name_components: list):
        if "genotyping" in file_path.lower():
            id_type = "chipwellbarcode"
            id_value = "_".join(name_components[0:2]).split('.')[0]  # ex: 204027270091_R02C01_Grn.idat

        elif "wgs" in file_path.lower():
            id_type = "sample_id"
            id_value = name_components[2]  # ex: UW_A102807943_21046008189_689024_v1.cram

        else:
            id_type = None
            id_value = None

        return id_type, id_value

    @staticmethod
    def get_gc_site_for_data_file(bucket_name: str, file_path: str, name_components: list):
        if "genotyping" in file_path.lower():
            # get GC from bucket
            name = bucket_name.split('-')[-1]
            return genome_centers_id_from_bucket_array[name]

        elif "wgs" in file_path.lower():
            # get from name
            return name_components[0].lower()
        else:
            return 'rdr'

    def reconcile_feedback_records(self):
        records = self.manifest_feedback_dao.get_feedback_reconcile_records()
        logging.info('Running feedback records reconciliation')

        for record in records:
            if record.raw_feedback_count > record.feedbackRecordCount \
                    and record.raw_feedback_count != record.feedbackRecordCount:

                logging.info(f'Updating feedback record count for file path: {record.filePath}')

                feedback_record = self.manifest_feedback_dao.get(record.feedback_id)
                feedback_record.feedbackRecordCount = record.raw_feedback_count
                self.manifest_feedback_dao.update(feedback_record)

    def gc_missing_files_record_clean_up(self, num_days=90):
        logging.info('Running missing resolved data files cleanup')

        self.missing_files_dao.remove_resolved_from_days(
            num_days=num_days
        )

    def resolve_missing_gc_files(self):
        logging.info('Resolving missing gc data files')

        files_to_resolve = self.missing_files_dao.get_files_to_resolve(limit=200)
        if files_to_resolve:

            resolve_arrays = [obj for obj in files_to_resolve if obj.identifier_type == 'chipwellbarcode']
            if resolve_arrays:
                self.missing_files_dao.batch_update_resolved_file(resolve_arrays)

            resolve_wgs = [obj for obj in files_to_resolve if obj.identifier_type == 'sample_id']
            if resolve_wgs:
                self.missing_files_dao.batch_update_resolved_file(resolve_wgs)

            logging.info('Resolving missing gc data files complete')
        else:
            logging.info('No missing gc data files to resolve')

    def update_member_aw2_missing_states_if_resolved(self):
        # get array member_ids that are resolved but still in GC_DATA_FILES_MISSING
        logging.info("Updating Array GC_DATA_FILES_MISSING members")
        array_member_ids = self.member_dao.get_aw2_missing_with_all_files(config.GENOME_TYPE_ARRAY)
        self.member_dao.batch_update_member_field(member_ids=array_member_ids,
                                                  field='genomicWorkflowState',
                                                  value=GenomicWorkflowState.GEM_READY,
                                                  project_id=self.bq_project_id)
        logging.info(f"Updated {len(array_member_ids)} Array members.")

        logging.info("Updating WGS GC_DATA_FILES_MISSING members")
        # get wgs member_ids that are resolved but still in GC_DATA_FILES_MISSING
        wgs_member_ids = self.member_dao.get_aw2_missing_with_all_files(config.GENOME_TYPE_WGS)
        self.member_dao.batch_update_member_field(member_ids=wgs_member_ids,
                                                  field='genomicWorkflowState',
                                                  value=GenomicWorkflowState.CVL_READY,
                                                  project_id=self.bq_project_id)
        logging.info(f"Updated {len(wgs_member_ids)} WGS members.")

        self.job_result = GenomicSubProcessResult.SUCCESS

    def reconcile_gc_data_file_to_table(self):
        """
        Entrypoint for reconciliation of GC data buckets to
        genomic_gc_data_file table.
        """
        # truncate staging table
        self.staging_dao = GcDataFileStagingDao()
        self.staging_dao.truncate()

        self.load_gc_data_file_staging()

        # compare genomic_gc_data_file to temp table
        missing_records = self.staging_dao.get_missing_gc_data_file_records()

        # create genomic_gc_data_file records for missing files
        for missing_file in missing_records:
            self.accession_data_files(missing_file.file_path, missing_file.bucket_name)

        self.job_result = GenomicSubProcessResult.SUCCESS

    def load_gc_data_file_staging(self):
        # Determine bucket mappings to use
        buckets = config.getSettingJson(config.DATA_BUCKET_SUBFOLDERS_PROD)

        # get file extensions
        extensions = [file_def['file_type'] for file_def in wgs_file_types_attributes if file_def['required']] + \
                     [file_def['file_type'] for file_def in array_file_types_attributes if file_def['required']]

        extensions.extend(['hard-filtered.gvcf.gz.md5sum', 'hard-filtered.gvcf.gz'])  # need to add gvcf but not req.
        # get files in each bucket and load temp table
        for bucket in buckets:
            for folder in buckets[bucket]:
                if self.storage_provider:
                    blobs = self.storage_provider.list(bucket, prefix=folder)
                else:
                    blobs = list_blobs(bucket, prefix=folder)

                files = []
                for blob in blobs:
                    # Only write the required files
                    if any(extension in blob.name for extension in extensions):
                        files.append({
                            'bucket_name': bucket,
                            'file_path': f'{bucket}/{blob.name}'
                        })

                        if len(files) % 10000 == 0:
                            self.staging_dao.insert_filenames_bulk(files)
                            files = []

                self.staging_dao.insert_filenames_bulk(files)

    def reconcile_raw_to_aw1_ingested(self):
        # Compare AW1 Raw to Genomic Set Member
        raw_dao = GenomicAW1RawDao()
        deltas = raw_dao.get_set_member_deltas()

        # Update Genomic Set Member records for deltas
        for record in deltas:
            member = self.member_dao.get_member_from_raw_aw1_record(record)
            if member:
                member = self.set_aw1_attributes_from_raw((member, record))

                # Set additional attributes
                member.gcSiteId = record.site_name
                member.aw1FileProcessedId = (
                    self.file_processed_dao.get_max_file_processed_for_filepath(record.file_path).id
                )
                member.genomicWorkflowState = GenomicWorkflowState.AW1

                with self.member_dao.session() as session:
                    session.merge(member)

        self.job_result = GenomicSubProcessResult.SUCCESS

    def reconcile_raw_to_aw2_ingested(self):
        # Compare AW2 Raw to genomic_gc_validation_metrics
        raw_dao = GenomicAW2RawDao()
        deltas = raw_dao.get_aw2_ingestion_deltas()
        inserted_metric_ids = []

        # resolve deltas
        for record in deltas:
            member = self.member_dao.get_member_from_sample_id(record.sample_id)

            if member:
                # Get file_processed record
                file_proc_map = self.map_file_paths_to_fp_id([record.file_path])

                # Insert record into genomic_gc_validation_metrics
                with self.metrics_dao.session() as session:
                    self.preprocess_aw2_attributes_from_raw((member, record), file_proc_map)
                    metrics_obj = self.set_validation_metrics_from_raw((member, record))
                    metrics_obj = session.merge(metrics_obj)
                    session.commit()
                    inserted_metric_ids.append(metrics_obj.id)

        self.job_result = GenomicSubProcessResult.SUCCESS

    @staticmethod
    def set_aw1_attributes_from_raw(rec: tuple):
        """
        :param rec: GenomicSetMember, GenomicAW1Raw
        :return:
        """
        member, raw = rec
        # Iterate through mapped fields
        _map = raw_aw1_to_genomic_set_member_fields
        for key in _map.keys():
            member.__setattr__(_map[key], getattr(raw, key))

        return member

    @staticmethod
    def set_validation_metrics_from_raw(rec: tuple):
        """
        Sets attributes on GenomicGCValidationMetrics from
        GenomicAW2Raw object.
        :param rec: GenomicSetMember, GenomicAW2Raw
        :return:
        """
        member, raw = rec
        metric = GenomicGCValidationMetrics()
        metric.genomicSetMemberId = member.id
        metric.contaminationCategory = raw.contamination_category
        metric.genomicFileProcessedId = member.aw2FileProcessedId

        # Iterate mapped fields
        _map = raw_aw2_to_genomic_set_member_fields
        for key in _map.keys():
            metric.__setattr__(_map[key], getattr(raw, key))

        return metric

    def preprocess_aw2_attributes_from_raw(self, rec: tuple, file_proc_map: dict):
        member, raw = rec

        member.aw2FileProcessedId = file_proc_map[raw.file_path]

        # Only update the state if it was AW1
        if member.genomicWorkflowState == GenomicWorkflowState.AW1:
            member.genomicWorkflowState = GenomicWorkflowState.AW2
            member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

        # Truncate call rate
        try:
            raw.call_rate = raw.call_rate[:10]
        except TypeError:
            # ignore if missing
            pass

        # Validate and clean contamination data
        try:
            raw.contamination = float(raw.contamination)

            # Percentages shouldn't be less than 0
            if raw.contamination < 0:
                raw.contamination = 0
        except ValueError:
            raise ValueError(f'contamination must be a number for member_id: {member.id}')

        # Calculate contamination_category using an ingester
        ingester = GenomicFileIngester(_controller=self, job_id=self.job_id)
        category = ingester.calculate_contamination_category(member.collectionTubeId,
                                                             raw.contamination, member)
        raw.contamination_category = category

    @staticmethod
    def compile_raw_ingestion_results(completed, missing, multiples, metrics):
        result_msg = ''
        result_msg += 'Ingestion From Raw Results:'
        result_msg += f'    Updated Member IDs: {completed}'
        result_msg += f'    Missing Member IDs: {missing}'
        result_msg += f'    Multiples found for Member IDs: {multiples}'
        result_msg += f'    Inserted Metrics IDs: {metrics}' if metrics else ""

        return result_msg

    @staticmethod
    def get_unique_file_paths_for_raw_records(raw_records):
        paths = set()
        for r in raw_records:
            paths.add(r.file_path)

        return paths

    def map_file_paths_to_fp_id(self, paths):
        path_map = {}

        for p in paths:
            file_obj = self.file_processed_dao.get_max_file_processed_for_filepath(f'{p}')

            if not file_obj:
                raise DataError(f"No genomic_file_processed record for {p}")

            path_map[p] = file_obj.id

        return path_map

    def set_rdr_aw1_attributes_from_raw(self, rec: tuple, file_proc_map: dict):
        member = rec[0]
        raw = rec[1]

        # Set job run and file processed IDs
        member.reconcileGCManifestJobRunId = self.job_run.id

        # Don't overwrite aw1_file_processed_id when ingesting an AW1F
        if self.job_id == GenomicJob.AW1_MANIFEST:
            member.aw1FileProcessedId = file_proc_map[raw.file_path]

        # Set the GC site ID (sourced from file-name)
        member.gcSiteId = raw.file_path.split('/')[-1].split("_")[0].lower()

        # Only update the state if it was AW0 or AW1 (if in failure manifest workflow)
        # We do not want to regress a state for reingested data
        state_to_update = GenomicWorkflowState.AW0

        if self.job_id == GenomicJob.AW1F_MANIFEST:
            state_to_update = GenomicWorkflowState.AW1

        if member.genomicWorkflowState == state_to_update:
            _signal = "aw1-reconciled"

            # Set the signal for a failed sample
            if raw.failure_mode not in [None, '']:
                _signal = 'aw1-failed'

            member.genomicWorkflowState = GenomicStateHandler.get_new_state(
                member.genomicWorkflowState,
                signal=_signal)
            member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()

        return member

    def run_reconciliation_to_data(self, *, genome_type):
        """
        Reconciles the metrics based on type of files using reconciler component
        :param genome_type array or wgs
        """
        self.reconciler = GenomicReconciler(self.job_run.id, self.job_id,
                                            storage_provider=self.storage_provider,
                                            controller=self)

        try:
            # Set reconciler's bucket and filter queries on gc_site_id for each bucket
            for bucket_name in self.bucket_name_list:
                self.reconciler.bucket_name = bucket_name
                site_id_mapping = config.getSettingJson("gc_name_to_id_mapping")

                gc_site_id = 'rdr'

                if 'baylor' in bucket_name.lower():
                    baylor = 'baylor_{}'.format(genome_type)
                    gc_site_id = site_id_mapping[baylor]

                if 'broad' in bucket_name.lower():
                    gc_site_id = site_id_mapping['broad']

                if 'northwest' in bucket_name.lower():
                    gc_site_id = site_id_mapping['northwest']

                # Run the reconciliation by GC
                self.job_result = self.reconciler.reconcile_metrics_to_data_files(genome_type,
                                                                                  _gc_site_id=gc_site_id)

        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_new_participant_workflow(self):
        """
        Creates new GenomicSet, GenomicSetMembers,
        And manifest file using BiobankSamplesCoupler
        and ManifestCoupler components
        """
        self.biobank_coupler = GenomicBiobankSamplesCoupler(self.job_run.id, controller=self)

        try:
            last_run_date = self._get_last_successful_run_time()
            logging.info(f'Running New Participant Workflow.')
            self.job_result = self.biobank_coupler.create_new_genomic_participants(last_run_date)
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_c2_participant_workflow(self):
        """
        Creates new GenomicSet, GenomicSetMembers,
        And manifest file for Cohort 2 participants
        """
        self.biobank_coupler = GenomicBiobankSamplesCoupler(self.job_run.id, controller=self)

        try:
            last_run_date = self._get_last_successful_run_time()
            logging.info('Running C2 Participant Workflow.')
            self.job_result = self.biobank_coupler.create_c2_genomic_participants(last_run_date)
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_c1_participant_workflow(self):
        """
        Creates new GenomicSet, GenomicSetMembers,
        And manifest file for Cohort 1 participants
        """
        self.biobank_coupler = GenomicBiobankSamplesCoupler(self.job_run.id, controller=self)

        try:
            last_run_date = self._get_last_successful_run_time()
            logging.info('Running C1 Participant Workflow.')
            self.job_result = self.biobank_coupler.create_c1_genomic_participants(last_run_date)
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_genomic_centers_manifest_workflow(self):
        """
        Uses GenomicFileIngester to ingest Genomic Manifest files (AW1).
        Reconciles samples in manifests against GenomicSetMember.validationStatus
        """
        try:
            for gc_bucket_name in self.bucket_name_list:
                for folder in self.sub_folder_tuple:
                    self.sub_folder_name = config.getSetting(folder)
                    self.ingester = GenomicFileIngester(job_id=self.job_id,
                                                        job_run_id=self.job_run.id,
                                                        bucket=gc_bucket_name,
                                                        sub_folder=self.sub_folder_name,
                                                        _controller=self)
                    self.subprocess_results.add(
                        self.ingester.generate_file_queue_and_do_ingestion()
                    )
            self.job_result = self._aggregate_run_results()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def ingest_specific_aw1_manifest(self, filename):
        """
        Uses GenomicFileIngester to ingest specific Genomic Manifest files (AW1).
        """
        try:
            self.ingester = GenomicFileIngester(job_id=self.job_id,
                                                job_run_id=self.job_run.id,
                                                bucket=self.bucket_name,
                                                target_file=filename,
                                                _controller=self)

            self.job_result = self.ingester.generate_file_queue_and_do_ingestion()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_aw1f_manifest_workflow(self):
        """
        Ingests the BB to GC failure manifest (AW1F)
        from post-accessioning subfolder.
        """
        try:
            for gc_bucket_name in self.bucket_name_list:
                for folder in self.sub_folder_tuple:
                    self.sub_folder_name = config.getSetting(folder)
                    self.ingester = GenomicFileIngester(job_id=self.job_id,
                                                        job_run_id=self.job_run.id,
                                                        bucket=gc_bucket_name,
                                                        sub_folder=self.sub_folder_name,
                                                        _controller=self)
                    self.subprocess_results.add(
                        self.ingester.generate_file_queue_and_do_ingestion()
                    )
            self.job_result = self._aggregate_run_results()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def process_failure_manifests_for_alerts(self):
        """
        Scans for new AW1F and AW1CF files in GC buckets and sends email alert
        """

        # Setup date
        timezone = pytz.timezone('Etc/Greenwich')
        date_limit = timezone.localize(self.last_run_time)

        new_failure_files = dict()

        # Get files in each gc bucket and folder where updated date > last run date
        logging.info("Searching buckets for accessioning FAILURE files.")
        for gc_bucket_name in self.bucket_name_list:
            failures_in_bucket = list()

            for folder in self.sub_folder_tuple:
                # If the folder is defined in config, use that,
                # otherwise use the string constant.
                try:
                    self.sub_folder_name = config.getSetting(folder)
                except MissingConfigException:
                    self.sub_folder_name = folder

                logging.info(f"Scanning folder: {self.sub_folder_name}")

                bucket = '/' + gc_bucket_name
                files = list_blobs(bucket, prefix=self.sub_folder_name)

                files_filtered = [s.name for s in files
                                  if s.updated > date_limit
                                  and s.name.endswith("_FAILURE.csv")]

                if len(files_filtered) > 0:
                    for f in files_filtered:
                        logging.info(f'Found failure file: {f}')
                        failures_in_bucket.append(f)

            if len(failures_in_bucket) > 0:
                new_failure_files[gc_bucket_name] = failures_in_bucket

        self.job_result = GenomicSubProcessResult.NO_FILES

        if len(new_failure_files) > 0:
            # Compile email message
            logging.info('Compiling email...')
            email_req = self._compile_accesioning_failure_alert_email(new_failure_files)

            # send email
            try:
                logging.info('Sending Email to SendGrid...')
                self._send_email_with_sendgrid(email_req)

                logging.info('Email Sent.')
                self.job_result = GenomicSubProcessResult.SUCCESS

            except RuntimeError:
                self.job_result = GenomicSubProcessResult.ERROR

    def run_cvl_reconciliation_report(self):
        """
        Creates the CVL reconciliation report using the reconciler object
        """
        self.reconciler = GenomicReconciler(
            self.job_run.id, self.job_id, bucket_name=self.bucket_name, controller=self
        )
        try:
            cvl_result = self.reconciler.generate_cvl_reconciliation_report()
            if cvl_result == GenomicSubProcessResult.SUCCESS:
                logging.info(f'CVL reconciliation report created: {self.reconciler.cvl_file_name}')
                # Insert the file record
                self.file_processed_dao.insert_file_record(
                    self.job_run.id,
                    f'{self.bucket_name}/{self.reconciler.cvl_file_name}',
                    self.bucket_name,
                    self.reconciler.cvl_file_name,
                    end_time=clock.CLOCK.now(),
                    file_result=cvl_result
                )
                self.subprocess_results.add(cvl_result)
            self.job_result = self._aggregate_run_results()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def generate_manifest(self, manifest_type, _genome_type, **kwargs):
        """
        Creates Genomic manifest using ManifestCompiler component
        """
        self.manifest_compiler = ManifestCompiler(
            run_id=self.job_run.id,
            bucket_name=self.bucket_name,
            max_num=self.max_num,
            controller=self
        )

        try:
            logging.info(f'Running Manifest Compiler for {manifest_type.name}.')

            # Set the feedback manifest name based on the input manifest name
            if "feedback_record" in kwargs.keys():
                input_manifest = self.manifest_file_dao.get(kwargs['feedback_record'].inputManifestFileId)
                version_num = kwargs['feedback_record'].version
                result = self.manifest_compiler.generate_and_transfer_manifest(
                    manifest_type,
                    _genome_type,
                    version=version_num + 1,
                    input_manifest=input_manifest
                )

            else:
                result = self.manifest_compiler.generate_and_transfer_manifest(
                    manifest_type,
                    _genome_type
                )

            if result['code'] == GenomicSubProcessResult.SUCCESS \
                    and self.manifests_generated:

                now_time = datetime.utcnow()

                for manifest in self.manifests_generated:
                    compiled_file_name = manifest["file_path"].split(f'{self.bucket_name}/')[-1]
                    logging.info(f'Manifest created: {compiled_file_name}')
                    new_manifest_record = self.manifest_file_dao.get_manifest_file_from_filepath(manifest['file_path'])

                    if not new_manifest_record:
                        # Insert manifest_file record
                        new_manifest_obj = GenomicManifestFile(
                            uploadDate=now_time,
                            manifestTypeId=manifest_type,
                            filePath=manifest['file_path'],
                            bucketName=self.bucket_name,
                            recordCount=manifest['record_count'],
                            rdrProcessingComplete=1,
                            rdrProcessingCompleteDate=now_time,
                            fileName=manifest['file_path'].split('/')[-1]
                        )
                        new_manifest_record = self.manifest_file_dao.insert(new_manifest_obj)

                        bq_genomic_manifest_file_update(new_manifest_obj.id, self.bq_project_id)
                        genomic_manifest_file_update(new_manifest_obj.id)

                    # update feedback records if manifest is a feedback manifest
                    if "feedback_record" in kwargs.keys():
                        r = kwargs['feedback_record']
                        r.feedbackManifestFileId = new_manifest_record.id
                        r.feedbackComplete = 1
                        r.version += 1
                        r.feedbackCompleteDate = now_time

                        with self.manifest_feedback_dao.session() as session:
                            session.merge(r)

                    # Insert the file_processed record
                    new_file_record = self.file_processed_dao.insert_file_record(
                        self.job_run.id,
                        f'{self.bucket_name}/{self.manifest_compiler.output_file_name}',
                        self.bucket_name,
                        self.manifest_compiler.output_file_name,
                        end_time=now_time,
                        file_result=result['code'],
                        upload_date=now_time,
                        manifest_file_id=new_manifest_record.id
                    )

                    file_record_attr = self.update_member_file_record(manifest_type)
                    if self.member_ids_for_update and file_record_attr:
                        self.execute_cloud_task({
                            'member_ids': self.member_ids_for_update,
                            'field': file_record_attr,
                            'value': new_manifest_record.id,
                        }, 'genomic_set_member_update_task')

                    # For BQ/PDR
                    bq_genomic_file_processed_update(new_file_record.id, self.bq_project_id)
                    genomic_file_processed_update(new_file_record.id)

                    self.subprocess_results.add(result["code"])
            self.job_result = self._aggregate_run_results()

        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def reconcile_report_states(self, _genome_type):
        """
        Wrapper for the Reconciler reconcile_gem_report_states
        and reconcile_rhp_report_states
        :param _genome_type: array or wgs
        """

        self.reconciler = GenomicReconciler(self.job_run.id, self.job_id, controller=self)
        if _genome_type == GENOME_TYPE_ARRAY:
            self.reconciler.reconcile_gem_report_states(_last_run_time=self.last_run_time)

    def run_general_ingestion_workflow(self):
        """
        Ingests A single genomic file
        Depending on job_id, bucket_name, etc.
        """
        self.ingester = GenomicFileIngester(job_id=self.job_id,
                                            job_run_id=self.job_run.id,
                                            bucket=self.bucket_name,
                                            sub_folder=self.sub_folder_name,
                                            _controller=self)
        try:
            self.job_result = self.ingester.generate_file_queue_and_do_ingestion()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_aw1c_workflow(self):
        """
        Uses GenomicFileIngester to ingest CVL Genomic Manifest files (AW1C).
        Reconciles samples in AW1C manifest against those sent in W3
        """
        try:
            for cvl_bucket_name in self.bucket_name_list:
                self.ingester = GenomicFileIngester(job_id=self.job_id,
                                                    job_run_id=self.job_run.id,
                                                    bucket=cvl_bucket_name,
                                                    sub_folder=self.sub_folder_name,
                                                    _controller=self)
                self.subprocess_results.add(
                    self.ingester.generate_file_queue_and_do_ingestion()
                )
            self.job_result = self._aggregate_run_results()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_aw1cf_manifest_workflow(self):
        """
        Uses GenomicFileIngester to ingest CVL Manifest Failure files (AW1CF).
        """
        try:
            for cvl_bucket_name in self.bucket_name_list:
                self.ingester = GenomicFileIngester(job_id=self.job_id,
                                                    job_run_id=self.job_run.id,
                                                    bucket=cvl_bucket_name,
                                                    sub_folder=self.sub_folder_name,
                                                    _controller=self)
                self.subprocess_results.add(
                    self.ingester.generate_file_queue_and_do_ingestion()
                )
            self.job_result = self._aggregate_run_results()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def load_raw_awn_data_from_filepath(self, file_path):
        """
        Loads raw AW1/AW2 data to genomic_aw1_raw/genomic_aw2_raw

        :param file_path: "bucket/folder/manifest_file.csv"
        :return:
        """
        logging.info(f"Loading manifest: {file_path}")

        self.ingester = GenomicFileIngester(job_id=self.job_id,
                                            job_run_id=self.job_run.id,
                                            target_file=file_path,
                                            _controller=self)

        self.job_result = self.ingester.load_raw_awn_file()

    def create_incident(
        self,
        save_incident=True,
        slack=False,
        **kwargs
    ):
        """
        Creates an GenomicIncident and sends alert via Slack if default
        for slack arg is True and saves an incident record to GenomicIncident
        if save_incident arg is True.
        :param save_incident: bool
        :param slack: bool
        :return:
        """
        num_days = 7
        incident = None
        message = kwargs.get('message', None)
        created_incident = self.incident_dao.get_by_message(message) if message else None
        today = clock.CLOCK.now()

        if created_incident and (today.date() - created_incident.created.date()).days <= num_days:
            return

        if save_incident:
            insert_kwargs = {key: value for key, value in kwargs.items()
                             if key in GenomicIncident.__table__.columns.keys()}
            incident = self.incident_dao.insert(GenomicIncident(**insert_kwargs))

        if slack:
            message_data = {'text': message}
            slack_alert = self.genomic_alert_slack.send_message_to_webhook(
                message_data=message_data
            )

            if slack_alert and incident:
                incident.slack_notification = 1
                incident.slack_notification_date = today
                self.incident_dao.update(incident)

        logging.warning(message)

    @staticmethod
    def update_member_file_record(manifest_type):
        file_attr = None

        attributes_map = {
            GenomicManifestTypes.AW3_ARRAY: 'aw3ManifestFileId',
            GenomicManifestTypes.AW3_WGS: 'aw3ManifestFileId'
        }
        if manifest_type in attributes_map.keys():
            file_attr = attributes_map[manifest_type]

        return file_attr

    @staticmethod
    def execute_cloud_task(payload, endpoint):
        if GAE_PROJECT != 'localhost':
            _task = GCPCloudTask()
            _task.execute(
                endpoint,
                payload=payload,
                queue='genomics'
            )

    def _end_run(self):
        """Updates the genomic_job_run table with end result"""
        self.job_run_dao.update_run_record(
            self.job_run.id, self.job_result, GenomicSubProcessStatus.COMPLETED)

        # Update run for PDR
        bq_genomic_job_run_update(self.job_run.id, self.bq_project_id)
        genomic_job_run_update(self.job_run.id)

        # Insert incident if job isn't successful
        if self.job_result.number > 2:
            # TODO: implement specific codes for each job result
            self.create_incident(
                code="UNKNOWN",
                message=self.job_result.name,
                source_job_run_id=self.job_run.id
            )

    def _aggregate_run_results(self):
        """
        This method aggregates the run results based on a priority of
        sub-process results
        :return: result code
        """
        # Any Validation Failure = a job result of an error, no files is OK
        yay = (GenomicSubProcessResult.SUCCESS, GenomicSubProcessResult.NO_FILES)
        return yay[0] if all([r in yay for r in self.subprocess_results]) \
            else GenomicSubProcessResult.ERROR

    def _get_last_successful_run_time(self):
        """Return last successful run's starttime from `genomics_job_runs`"""
        last_run_time = self.job_run_dao.get_last_successful_runtime(self.job_id)
        return last_run_time if last_run_time else self.last_run_time

    def _create_run(self, job_id):
        new_run = self.job_run_dao.insert_run_record(job_id)

        # Insert new run for PDR
        bq_genomic_job_run_update(new_run.id, self.bq_project_id)
        genomic_job_run_update(new_run.id)

        return new_run

    def _compile_accesioning_failure_alert_email(self, alert_files):
        """
        Takes a dict of all new failure files from
        GC buckets' accessioning folders
        :param alert_files: dict
        :return: email dict ready for SendGrid API
        """

        # Set email data here
        try:
            recipients = config.getSettingList(config.AW1F_ALERT_RECIPIENTS)
        except MissingConfigException:
            recipients = ["test-genomic@vumc.org"]

        subject = "All of Us GC Manifest Failure Alert"
        from_email = config.SENDGRID_FROM_EMAIL

        email_message = "New AW1 Failure manifests have been found:\n"

        if self.job_id == GenomicJob.AW1CF_ALERTS:
            email_message = "New AW1CF CVL Failure manifests have been found:\n"

        for bucket in alert_files.keys():
            email_message += f"\t{bucket}:\n"
            for file in alert_files[bucket]:
                email_message += f"\t\t{file}\n"

        data = {
            "personalizations": [
                {
                    "to": [{"email": r} for r in recipients],
                    "subject": subject
                }
            ],
            "from": {
                "email": from_email
            },
            "content": [
                {
                    "type": "text/plain",
                    "value": email_message
                }
            ]
        }

        return data

    @staticmethod
    def _send_email_with_sendgrid(_email):
        """
        Calls SendGrid API with email request
        :param _email:
        :return: sendgrid response
        """
        sg = sendgrid.SendGridAPIClient(api_key=config.getSetting(config.SENDGRID_KEY))
        response = sg.client.mail.send.post(request_body=_email)
        return response


class DataQualityJobController:
    """
    Analogous to the GenomicJobController but
    more tailored for data quality jobs.
    Executes jobs as cloud tasks or via tools ran locally
    """

    def __init__(self, job, bq_project_id=None):
        super().__init__()

        # Job attributes
        self.job = job
        self.job_run = None
        self.from_date = datetime(2021, 2, 23, 0, 0, 0)
        self.job_run_result = GenomicSubProcessResult.UNSET

        # Other attributes
        self.bq_project_id = bq_project_id

        # Components
        self.job_run_dao = GenomicJobRunDao()
        self.genomic_report_slack = SlackMessageHandler(
            webhook_url=config.getSettingJson(RDR_SLACK_WEBHOOKS).get('rdr_genomic_reports')
        )

    def __enter__(self):
        logging.info(f'Workflow Initiated: {self.job.name}')
        self.job_run = self.create_genomic_job_run()
        self.from_date = self.get_last_successful_run_time()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.info(f'Workflow Completed: {self.job.name}')
        self.end_genomic_job_run()

    def create_genomic_job_run(self):
        """
        Creates GenomicJobRun record
        :return: GenomicJobRun
        """
        new_run = self.job_run_dao.insert_run_record(self.job)

        # Insert new run for PDR
        bq_genomic_job_run_update(new_run.id, self.bq_project_id)
        genomic_job_run_update(new_run.id)

        return new_run

    def end_genomic_job_run(self):
        """Updates the genomic_job_run table with end result"""
        self.job_run_dao.update_run_record(self.job_run.id, self.job_run_result, GenomicSubProcessStatus.COMPLETED)

        # Update run for PDR
        bq_genomic_job_run_update(self.job_run.id, self.bq_project_id)
        genomic_job_run_update(self.job_run.id)

    def get_last_successful_run_time(self):
        """Return last successful run's start time from genomic_job_run"""
        last_run_time = self.job_run_dao.get_last_successful_runtime(self.job)
        return last_run_time if last_run_time else self.from_date

    def get_job_registry_entry(self, job):
        """
        Registry for jobs and which method to execute.
        Reports will execute get_report()
        In the future, other DQ pipeline jobs will execute other methods
        :param job:
        :return:
        """
        # Only 'get_report()' is used. Subsequent PRs will expand this
        job_registry = {
            GenomicJob.DAILY_SUMMARY_REPORT_JOB_RUNS: self.get_report,
            GenomicJob.WEEKLY_SUMMARY_REPORT_JOB_RUNS: self.get_report,
            GenomicJob.DAILY_SUMMARY_REPORT_INGESTIONS: self.get_report,
            GenomicJob.WEEKLY_SUMMARY_REPORT_INGESTIONS: self.get_report,
            GenomicJob.DAILY_SUMMARY_REPORT_INCIDENTS: self.get_report,
        }

        return job_registry[job]

    def execute_workflow(self, **kwargs):
        """
        Serves as the interface for the controller
        to execute a genomic data quality workflow
        :param kwargs:
        :return: dictionary of the results of a workflow
        """
        # print(f"Executing {self.job}")
        logging.info(f"Executing {self.job}")

        job_function = self.get_job_registry_entry(self.job)
        result_data = job_function(**kwargs)

        return result_data

    def get_report(self, **kwargs):
        """
        Executes a reporting job using the ReportingComponent

        supports time frames:
        'D': 24 hours
        'W': 1 week

        support levels:
        'SUMMARY'
        'DETAIL'

        :return: dict of result data
        """

        rc = ReportingComponent(self)

        report_params = rc.get_report_parameters(**kwargs)

        rc.set_report_def(**report_params)

        report_data = rc.get_report_data()

        report_string = rc.format_report(report_data)

        report_result = report_string

        # Compile long reports to cloud file and post link
        if len(report_string.split("\n")) > 25:
            file_path = rc.create_report_file(report_string, report_params['display_name'])

            message = report_params['display_name'] + " too long for output.\n"
            message += f"Report too long for Slack Output, download the report here:" \
                       f" https://storage.cloud.google.com{file_path}"

            message_data = {'text': message}

            report_result = file_path

        else:
            message_data = {'text': report_string}

        # Send report to slack #rdr-genomics channel
        if kwargs.get('slack') is True:
            self.genomic_report_slack.send_message_to_webhook(
                message_data=message_data
            )

        self.job_run_result = GenomicSubProcessResult.SUCCESS

        return report_result
