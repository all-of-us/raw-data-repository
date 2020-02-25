"""
This module tracks and validates the status of Genomics Pipeline Subprocesses.
"""

import logging
from datetime import datetime

from rdr_service import clock

from rdr_service.config import (
    GENOMIC_GC_METRICS_BUCKET_NAME,
    GENOMIC_GC_PROCESSED_FOLDER_NAME,
    getSetting,
    getSettingList)
from rdr_service.participant_enums import (
    GenomicSubProcessResult,
    GenomicSubProcessStatus)
from rdr_service.genomic.genomic_job_components import (
    GenomicFileIngester,
    GenomicFileMover,
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
                 archive_folder_name=GENOMIC_GC_PROCESSED_FOLDER_NAME,
                 bucket_name_list=None
                 ):

        self.job_id = job_id
        self.job_run = None
        self.bucket_name = getSetting(bucket_name, default="")
        self.sub_folder_name = getSetting(sub_folder_name, default="")
        self.bucket_name_list = getSettingList(bucket_name_list, default=[])
        self.archive_folder_name = archive_folder_name

        self.subprocess_results = set()
        self.job_result = GenomicSubProcessResult.UNSET

        self.default_date = datetime(2019, 11, 5, 0, 0, 0)

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
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._end_run()

    def ingest_gc_metrics(self):
        """
        Uses ingester to ingest files. Moves file to archive when done.
        """
        self.ingester = GenomicFileIngester(job_id=self.job_id,
                                            job_run_id=self.job_run.id,
                                            bucket=self.bucket_name,
                                            archive_folder=self.archive_folder_name,
                                            sub_folder=self.sub_folder_name)
        try:
            logging.info('Running Validation Metrics Ingestion Workflow.')
            self.job_result = self.ingester.generate_file_queue_and_do_ingestion()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_reconciliation_to_manifest(self):
        """
        Reconciles the metrics to manifest using reconciler component
        :return: result code for job run
        """
        self.reconciler = GenomicReconciler(self.job_run.id)
        try:
            self.job_result = self.reconciler.reconcile_metrics_to_manifest()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_reconciliation_to_sequencing(self):
        """
        Reconciles the metrics to sequencing file using reconciler component
        """
        self.file_mover = GenomicFileMover(
            archive_folder=self.archive_folder_name
        )
        self.reconciler = GenomicReconciler(
            self.job_run.id, self.archive_folder_name, self.file_mover
        )
        try:
            self.job_result = self.reconciler.reconcile_metrics_to_sequencing(self.bucket_name)

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

    def run_biobank_return_manifest_workflow(self):
        """
        Uses ingester to ingest manifest result files.
        Moves file to archive when done.
        """
        self.ingester = GenomicFileIngester(job_id=self.job_id,
                                            job_run_id=self.job_run.id,
                                            bucket=self.bucket_name,
                                            archive_folder=self.archive_folder_name,
                                            sub_folder=self.sub_folder_name)
        try:
            self.job_result = self.ingester.generate_file_queue_and_do_ingestion()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_genomic_centers_manifest_workflow(self):
        """
        Uses GenomicFileIngester to ingest Genomic Manifest files.
        """
        try:
            for gc_bucket_name in self.bucket_name_list:
                self.ingester = GenomicFileIngester(job_id=self.job_id,
                                                    job_run_id=self.job_run.id,
                                                    bucket=gc_bucket_name,
                                                    archive_folder=self.archive_folder_name,
                                                    sub_folder=self.sub_folder_name)
                self.subprocess_results.add(
                    self.ingester.generate_file_queue_and_do_ingestion()
                )
            self.job_result = self._aggregate_run_results()
        except RuntimeError:
            self.job_result = GenomicSubProcessResult.ERROR

    def run_cvl_reconciliation_report(self):
        """
        Creates the CVL reconciliation report using the reconciler object
        """
        self.reconciler = GenomicReconciler(
            self.job_run.id, bucket_name=self.bucket_name
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

    def generate_manifest(self, manifest_type):
        """
        Creates the CVL WGS manifest using ManifestCompiler component
        """
        self.manifest_compiler = ManifestCompiler(run_id=self.job_run.id,
                                                  bucket_name=self.bucket_name)
        try:
            logging.info(f'Running Manifest Compiler for {manifest_type.name}.')
            result = self.manifest_compiler.generate_and_transfer_manifest(manifest_type)
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
        return last_run_time if last_run_time else self.default_date

    def _create_run(self, job_id):
        return self.job_run_dao.insert_run_record(job_id)

