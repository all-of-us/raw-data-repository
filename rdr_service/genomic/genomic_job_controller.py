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
from rdr_service.participant_enums import (
    GenomicSubProcessResult,
    GenomicSubProcessStatus)
from rdr_service.genomic.genomic_job_components import (
    GenomicFileIngester,
    GenomicReconciler,
    GenomicBiobankSamplesCoupler,
    ManifestCompiler,
)
from rdr_service.dao.genomics_dao import (
    GenomicFileProcessedDao,
    GenomicJobRunDao
)


class GenomicJobController:
    """This class controlls the tracking of Genomics subprocesses"""

    def __init__(self, job_id,
                 bucket_name=GENOMIC_GC_METRICS_BUCKET_NAME,
                 sub_folder_name=None,
                 sub_folder_tuple=None,
                 archive_folder_name=None,
                 bucket_name_list=None,
                 ):

        self.job_id = job_id
        self.job_run = None
        self.bucket_name = getSetting(bucket_name, default="")
        self.sub_folder_name = getSetting(sub_folder_name, default="")
        self.sub_folder_tuple = sub_folder_tuple
        self.bucket_name_list = getSettingList(bucket_name_list, default=[])
        self.archive_folder_name = archive_folder_name

        self.subprocess_results = set()
        self.job_result = GenomicSubProcessResult.UNSET

        self.last_run_time = datetime(2019, 11, 5, 0, 0, 0)

        # Components
        self.job_run_dao = GenomicJobRunDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.ingester = None
        self.file_mover = None
        self.reconciler = None
        self.biobank_coupler = None
        self.manifest_compiler = None

    def __enter__(self):
        logging.info(f'Beginning {self.job_id.name} workflow')
        self.job_run = self._create_run(self.job_id)
        self.last_run_time = self._get_last_successful_run_time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._end_run()

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

    def run_reconciliation_to_manifest(self):
        """
        Reconciles the metrics to manifest using reconciler component
        :return: result code for job run
        """
        self.reconciler = GenomicReconciler(self.job_run.id, self.job_id)
        try:
            self.job_result = self.reconciler.reconcile_metrics_to_manifest()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_reconciliation_to_genotyping_data(self):
        """
        Reconciles the metrics to genotyping files using reconciler component
        """
        self.reconciler = GenomicReconciler(self.job_run.id, self.job_id)
        try:
            self.job_result = self.reconciler.reconcile_metrics_to_genotyping_data()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_reconciliation_to_sequencing_data(self):
        """
        Reconciles the metrics to sequencing files using reconciler component
        """
        self.reconciler = GenomicReconciler(self.job_run.id, self.job_id)
        try:
            self.job_result = self.reconciler.reconcile_metrics_to_sequencing_data()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_new_participant_workflow(self):
        """
        Creates new GenomicSet, GenomicSetMembers,
        And manifest file using BiobankSamplesCoupler
        and ManifestCoupler components
        """
        self.biobank_coupler = GenomicBiobankSamplesCoupler(self.job_run.id)

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
        self.biobank_coupler = GenomicBiobankSamplesCoupler(self.job_run.id)

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
        self.biobank_coupler = GenomicBiobankSamplesCoupler(self.job_run.id)

        try:
            last_run_date = self._get_last_successful_run_time()
            logging.info('Running C1 Participant Workflow.')
            self.job_result = self.biobank_coupler.create_c1_genomic_participants(last_run_date)
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_biobank_return_manifest_workflow(self):
        """
        Uses ingester to ingest manifest result files.
        Moves file to archive when done.
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

    def process_new_aw1f_for_alerts(self):
        """
        Scans for new AW1F files in GC buckets and sends email alert
        """

        # Setup date
        timezone = pytz.timezone('Etc/Greenwich')
        date_limit = timezone.localize(self.last_run_time)

        new_failure_files = dict()

        # Get files in each gc bucket and folder where updated date > last run date
        for gc_bucket_name in self.bucket_name_list:
            failures_in_bucket = list()

            for folder in self.sub_folder_tuple:
                self.sub_folder_name = config.getSetting(folder)
                bucket = '/' + gc_bucket_name
                files = list_blobs(bucket, prefix=self.sub_folder_name)

                files_filtered = [s.name for s in files
                                  if s.updated > date_limit
                                  and s.name.endswith("_FAILURE.csv")]

                if len(files_filtered) > 0:
                    for f in files_filtered:
                        failures_in_bucket.append(f)

            if len(failures_in_bucket) > 0:
                new_failure_files[gc_bucket_name] = failures_in_bucket

        # Compile email message
        email_req = self._compile_accesioning_failure_alert_email(new_failure_files)

        # send email
        self._send_email_with_sendgrid(email_req)

    def run_cvl_reconciliation_report(self):
        """
        Creates the CVL reconciliation report using the reconciler object
        """
        self.reconciler = GenomicReconciler(
            self.job_run.id, self.job_id, bucket_name=self.bucket_name
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

    def generate_manifest(self, manifest_type, _genome_type):
        """
        Creates Genomic manifest using ManifestCompiler component
        """
        self.manifest_compiler = ManifestCompiler(run_id=self.job_run.id,
                                                  bucket_name=self.bucket_name)
        try:
            logging.info(f'Running Manifest Compiler for {manifest_type.name}.')
            result = self.manifest_compiler.generate_and_transfer_manifest(manifest_type, _genome_type)
            if result == GenomicSubProcessResult.SUCCESS:
                logging.info(f'Manifest created: {self.manifest_compiler.output_file_name}')
                # Insert the file record
                self.file_processed_dao.insert_file_record(
                    self.job_run.id,
                    f'{self.bucket_name}/{self.manifest_compiler.output_file_name}',
                    self.bucket_name,
                    self.manifest_compiler.output_file_name,
                    end_time=clock.CLOCK.now(),
                    file_result=result
                )
                self.subprocess_results.add(result)
            self.job_result = self._aggregate_run_results()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def reconcile_report_states(self, _genome_type):
        """
        Wrapper for the Reconciler reconcile_gem_report_states
        and reconcile_rhp_report_states
        :param _genome_type: array or wgs
        """

        self.reconciler = GenomicReconciler(self.job_run.id, self.job_id)

        if _genome_type == GENOME_TYPE_ARRAY:
            self.reconciler.reconcile_gem_report_states(_last_run_time=self.last_run_time)

    def run_gem_a2_workflow(self):
        """
        Ingests GEM A2 Manifest
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

    def run_cvl_w2_workflow(self):
        """
        Ingests CVL W2 Manifest
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

    def run_aw4_workflow(self):
        """
        Ingests AW4 Array & WGS Manifests
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

    def _end_run(self):
        """Updates the genomic_job_run table with end result"""
        self.job_run_dao.update_run_record(self.job_run.id, self.job_result, GenomicSubProcessStatus.COMPLETED)

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
        return self.job_run_dao.insert_run_record(job_id)

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

        for bucket in alert_files.keys():
            email_message += f"\t{bucket}:\n"
            for file in alert_files[bucket]:
                email_message += f"\t\t{file}:\n"

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

