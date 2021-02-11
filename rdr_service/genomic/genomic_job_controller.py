"""
This module tracks and validates the status of Genomics Pipeline Subprocesses.
"""
import logging
from datetime import datetime

import pytz
from sendgrid import sendgrid

from rdr_service import clock, config
from rdr_service.api_util import list_blobs

from rdr_service.config import (
    GENOMIC_GC_METRICS_BUCKET_NAME,
    getSetting,
    getSettingList,
    GENOME_TYPE_ARRAY,
    MissingConfigException)
from rdr_service.dao.bq_genomics_dao import bq_genomic_job_run_update, bq_genomic_file_processed_update, \
    bq_genomic_manifest_file_update, bq_genomic_manifest_feedback_update
from rdr_service.model.genomics import GenomicManifestFile, GenomicManifestFeedback
from rdr_service.participant_enums import (
    GenomicSubProcessResult,
    GenomicSubProcessStatus,
    GenomicJob)
from rdr_service.genomic.genomic_job_components import (
    GenomicFileIngester,
    GenomicReconciler,
    GenomicBiobankSamplesCoupler,
    ManifestCompiler,
)
from rdr_service.dao.genomics_dao import (
    GenomicFileProcessedDao,
    GenomicJobRunDao,
    GenomicManifestFileDao, GenomicManifestFeedbackDao)
from rdr_service.resource.generators.genomics import genomic_job_run_update, genomic_file_processed_update, \
    genomic_manifest_file_update, genomic_manifest_feedback_update


class GenomicJobController:
    """This class controlls the tracking of Genomics subprocesses"""

    def __init__(self, job_id,
                 bucket_name=GENOMIC_GC_METRICS_BUCKET_NAME,
                 sub_folder_name=None,
                 sub_folder_tuple=None,
                 archive_folder_name=None,
                 bucket_name_list=None,
                 storage_provider=None,
                 bq_project_id=None,
                 task_data=None,
                 ):

        self.job_id = job_id
        self.job_run = None
        self.bucket_name = getSetting(bucket_name, default="")
        self.sub_folder_name = getSetting(sub_folder_name, default="")
        self.sub_folder_tuple = sub_folder_tuple
        self.bucket_name_list = getSettingList(bucket_name_list, default=[])
        self.archive_folder_name = archive_folder_name
        self.bq_project_id = bq_project_id
        self.task_data = task_data
        self.bypass_record_count = False

        self.subprocess_results = set()
        self.job_result = GenomicSubProcessResult.UNSET

        self.last_run_time = datetime(2019, 11, 5, 0, 0, 0)

        # Components
        self.job_run_dao = GenomicJobRunDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.manifest_file_dao = GenomicManifestFileDao()
        self.manifest_feedback_dao = GenomicManifestFeedbackDao()
        self.ingester = None
        self.file_mover = None
        self.reconciler = None
        self.biobank_coupler = None
        self.manifest_compiler = None
        self.storage_provider = storage_provider

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

            file_to_insert = GenomicManifestFile(
                created=now,
                modified=now,
                uploadDate=_uploadDate,
                manifestTypeId=_manifest_type,
                filePath=_file_path,
                bucketName=_file_path.split('/')[0],
                recordCount=0,  # Initializing with 0, counting records when processing file
                rdrProcessingComplete=0,
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

    def get_feedback_complete_records(self):
        """
        Retrieves genomic_manifest_feedback records that are complete
        and have not had a feedback_manifest_ID
        :return: list of GenomicManifestFeedback
        """
        return self.manifest_feedback_dao.get_feedback_equals_record_count()

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

    def run_reconciliation_to_genotyping_data(self):
        """
        Reconciles the metrics to genotyping files using reconciler component
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
                    gc_site_id = site_id_mapping['baylor_array']

                if 'broad' in bucket_name.lower():
                    gc_site_id = site_id_mapping['broad']

                if 'northwest' in bucket_name.lower():
                    gc_site_id = site_id_mapping['northwest']

                # Run the reconciliation by GC
                self.job_result = self.reconciler.reconcile_metrics_to_genotyping_data(_gc_site_id=gc_site_id)

        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_reconciliation_to_sequencing_data(self):
        """
        Reconciles the metrics to sequencing files using reconciler component
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
                    gc_site_id = site_id_mapping['baylor_wgs']

                if 'broad' in bucket_name.lower():
                    gc_site_id = site_id_mapping['broad']

                if 'northwest' in bucket_name.lower():
                    gc_site_id = site_id_mapping['northwest']

                self.job_result = self.reconciler.reconcile_metrics_to_sequencing_data(_gc_site_id=gc_site_id)

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
        self.manifest_compiler = ManifestCompiler(run_id=self.job_run.id,
                                                  bucket_name=self.bucket_name)
        try:
            logging.info(f'Running Manifest Compiler for {manifest_type.name}.')

            # Set the feedback manifest name based on the input manifest name
            if "feedback_record" in kwargs.keys():
                input_manifest = self.manifest_file_dao.get(kwargs['feedback_record'].inputManifestFileId)

                result = self.manifest_compiler.generate_and_transfer_manifest(manifest_type,
                                                                               _genome_type,
                                                                               input_manifest=input_manifest)

            else:
                result = self.manifest_compiler.generate_and_transfer_manifest(manifest_type, _genome_type)

            if result['code'] == GenomicSubProcessResult.SUCCESS:
                logging.info(f'Manifest created: {self.manifest_compiler.output_file_name}')

                new_file_path = f'{self.bucket_name}/{self.manifest_compiler.output_file_name}'

                now_time = datetime.utcnow()

                # Insert manifest_file record
                new_manifest_obj = GenomicManifestFile(
                    uploadDate=now_time,
                    manifestTypeId=manifest_type,
                    filePath=new_file_path,
                    bucketName=self.bucket_name,
                    recordCount=result['record_count'],
                    rdrProcessingComplete=1,
                    rdrProcessingCompleteDate=now_time,
                )
                new_manifest_record = self.manifest_file_dao.insert(new_manifest_obj)

                bq_genomic_manifest_file_update(new_manifest_obj.id, self.bq_project_id)
                genomic_manifest_file_update(new_manifest_obj.id)

                # update feedback records if manifest is a feedback manifest
                if "feedback_record" in kwargs.keys():
                    r = kwargs['feedback_record']

                    r.feedbackManifestFileId = new_manifest_record.id
                    r.feedbackComplete = 1
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

    def load_raw_aw1_data_from_filepath(self, file_path):
        """
        Loads raw AW1 data to genomic_aw1_raw
        :param file_path: "bucket/folder/manifest_file.csv"
        :return:
        """
        logging.info(f"Loading manifest: {file_path}")

        self.ingester = GenomicFileIngester(job_id=self.job_id,
                                            job_run_id=self.job_run.id,
                                            target_file=file_path,
                                            _controller=self)

        self.ingester.load_raw_aw1_file()


    def _end_run(self):
        """Updates the genomic_job_run table with end result"""
        self.job_run_dao.update_run_record(self.job_run.id, self.job_result, GenomicSubProcessStatus.COMPLETED)

        # Update run for PDR
        bq_genomic_job_run_update(self.job_run.id, self.bq_project_id)
        genomic_job_run_update(self.job_run.id)

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

    def _send_email_with_sendgrid(self, _email):
        """
        Calls SendGrid API with email request
        :param _email:
        :return: sendgrid response
        """
        sg = sendgrid.SendGridAPIClient(api_key=config.getSetting(config.SENDGRID_KEY))

        response = sg.client.mail.send.post(request_body=_email)

        # print(response.status_code)
        # print(response.body)
        # print(response.headers)

        return response

