"""
This module tracks and validates the status of Genomics Pipeline Subprocesses.
"""

import logging

from rdr_service.config import (
    GENOMIC_GC_METRICS_BUCKET_NAME,
    GENOMIC_GC_PROCESSED_FOLDER_NAME,
    getSetting
)
from rdr_service.participant_enums import GenomicSubProcessResult, GenomicSubProcessStatus
from rdr_service.genomic.genomic_job_components import (
    GenomicFileIngester,
    GenomicFileMover,
    GenomicReconciler
)
from rdr_service.dao.genomics_dao import (
    GenomicFileProcessedDao,
    GenomicJobRunDao
)


class GenomicJobController:
    """This class controlls the tracking of Genomics subprocesses"""

    def __init__(self, job_id,
                 bucket_name=GENOMIC_GC_METRICS_BUCKET_NAME,
                 job_name="genomic_cell_line_metrics",
                 archive_folder_name=GENOMIC_GC_PROCESSED_FOLDER_NAME
                 ):

        self.job_id = job_id
        self.bucket_name = getSetting(bucket_name)
        self.archive_folder_name = archive_folder_name
        self.job_name = job_name

        self.subprocess_results = set()
        self.job_result = GenomicSubProcessResult.UNSET

        # Components
        self.job_run_dao = GenomicJobRunDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.ingester = None
        self.file_mover = None
        self.reconciler = None

        self.job_run = self._create_run(job_id)

    def ingest_gc_metrics(self):
        """
        Uses ingester to ingest files. Moves file to archive when done.
        :return: result code of file ingestion subprocess
        """
        self.ingester = GenomicFileIngester()
        self.file_mover = GenomicFileMover(archive_folder=self.archive_folder_name)

        file_queue_result = self.ingester.generate_file_processing_queue(self.bucket_name,
                                                                         self.archive_folder_name,
                                                                         self.job_run.id)

        if file_queue_result == GenomicSubProcessResult.NO_FILES:
            logging.info('No files to process.')
            return file_queue_result
        else:
            logging.info('Processing files in queue.')
            while len(self.ingester.file_queue) > 0:
                try:
                    ingestion_result = self.ingester.ingest_gc_validation_metrics_file(
                        self.ingester.file_queue[0])
                    file_ingested = self.ingester.file_queue.popleft()
                    logging.info(f'Ingestion attempt for {file_ingested.fileName}: {ingestion_result}')
                    self.ingester.update_file_processed(
                        file_ingested.id,
                        GenomicSubProcessStatus.COMPLETED,
                        ingestion_result
                    )
                    self.subprocess_results.add(ingestion_result)
                    self.file_mover.archive_file(file_ingested)
                except IndexError:
                    logging.info('No files left in file queue.')

            run_result = self.aggregate_run_results()
            return run_result

    def run_reconciliation_to_manifest(self):
        """
        Reconciles the metrics to manifest using reconciler component
        :return: result code for job run
        """
        self.reconciler = GenomicReconciler(self.job_run.id)
        try:
            return self.reconciler.reconcile_metrics_to_manifest()
        except RuntimeError:
            return GenomicSubProcessResult.ERROR

    def run_reconciliation_to_sequencing(self):
        """
        Reconciles the metrics to sequencing file using reconciler component
        :return: result code for job run
        """
        self.file_mover = GenomicFileMover(
            archive_folder=self.archive_folder_name
        )
        self.reconciler = GenomicReconciler(
            self.job_run.id, self.archive_folder_name, self.file_mover
        )
        try:
            return self.reconciler.reconcile_metrics_to_sequencing(self.bucket_name)

        except RuntimeError:
            return GenomicSubProcessResult.ERROR

    def end_run(self, result):
        """Updates the genomic_job_run table with end result"""
        self.job_run_dao.update_run_record(self.job_run.id, result)

    def aggregate_run_results(self):
        """
        This method aggregates the run results based on a priority of
        sub-process results
        :return: result code
        """
        # Any Validation Failure = a job result of an error
        if GenomicSubProcessResult.ERROR in self.subprocess_results:
            return GenomicSubProcessResult.ERROR
        if GenomicSubProcessResult.INVALID_FILE_NAME in self.subprocess_results:
            return GenomicSubProcessResult.ERROR
        if GenomicSubProcessResult.INVALID_FILE_STRUCTURE in self.subprocess_results:
            return GenomicSubProcessResult.ERROR

        return GenomicSubProcessResult.SUCCESS

    def _get_last_successful_run_time(self, job_name):
        """Return last successful run's starttime from `genomics_job_runs`"""
        # TODO: implement once 'Cell Line' test runs are complete
        # pylint: disable=unused-argument
        last_run_time = "2019-11-05"
        return last_run_time

    def _create_run(self, job_id):
        return self.job_run_dao.insert_run_record(job_id)

