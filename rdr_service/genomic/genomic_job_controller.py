"""
This module tracks and validates the status of Genomics Pipeline Subprocesses.
"""
from collections import deque
import logging

from rdr_service.api_util import open_cloud_file, list_blobs
from rdr_service.config import GENOMIC_GC_METRICS_BUCKET_NAME, getSetting
from rdr_service.model.genomics import GenomicSubProcessResult
from rdr_service.genomic.genomic_job_components import GenomicFileIngester
from rdr_service.dao.genomics_dao import (
    GenomicFileProcessedDao,
    GenomicJobRunDao
)


class GenomicJobController:
    """This class controlls the tracking of Genomics subprocesses"""

    def __init__(self,
                 bucket_name=GENOMIC_GC_METRICS_BUCKET_NAME,
                 file_queue=None,
                 job_name="genomic_cell_line_metrics"
                 ):

        self.bucket_name = getSetting(bucket_name)
        self.file_queue = file_queue
        self.job_name = job_name

        self.subprocess_results = set()
        self.job_result = GenomicSubProcessResult.UNSET

        # Ingester component
        self.ingester = None

        # Dao Components
        self.job_run_dao = GenomicJobRunDao()
        self.file_processed_dao = GenomicFileProcessedDao()

        # TODO: currently set up for only gc metrics ingestion;
        #  need to make more generic
        self.job_run = self._create_run(1)

    def __enter__(self):
        pass

    def __exit__(self):
        pass

    def generate_file_processing_queue(self):
        """Creates the list of files to be ingested in this run.
        TODO: Ordered by timestamp.
        They are written to the DB in `genomic_files_processed` with the current run ID"""

        # last_run_time = self._get_last_successful_run_time(self.job_name)
        files = self._get_uningested_file_names_from_bucket(self.bucket_name)
        if files:
            for file_name in files:
                file_path = "/" + self.bucket_name + "/" + file_name
                self._create_file_record(self.job_run.id,
                                         file_path,
                                         self.bucket_name,
                                         file_name)
            self.file_queue = deque(self._get_file_queue_for_run(self.job_run.id))
        else:
            return GenomicSubProcessResult.NO_FILES

    def process_file_using_ingestor(self, file_obj):
        """
        Runs ingestor's main method
        :param file_obj:
        :return: result code of file ingestion
        """
        self.ingester = GenomicFileIngester()
        result = self.ingester.ingest_gc_validation_metrics_file(file_obj)
        self.subprocess_results.add(result)
        return result

    def update_file_processed(self, file_id, status, result):
        """Updates the genomic_file_processed record """
        self.file_processed_dao.update_file_record(file_id, status, result)

    def end_run(self, result):
        """Updates the genomic_job_run table with end result"""
        self.job_run_dao.update_run_record(self.job_run.id, result)

    def aggregate_run_results(self):
        """This method aggregates the run results based on a priority of
        sub-process results
        :return: result of run
        """
        # Prioritize errors over validation failures
        if GenomicSubProcessResult.ERROR in self.subprocess_results:
            return GenomicSubProcessResult.ERROR
        if GenomicSubProcessResult.INVALID_FILE_NAME in self.subprocess_results:
            return GenomicSubProcessResult.ERROR
        if GenomicSubProcessResult.INVALID_FILE_STRUCTURE in self.subprocess_results:
            return GenomicSubProcessResult.ERROR

        return GenomicSubProcessResult.SUCCESS

    def _get_uningested_file_names_from_bucket(self, bucket_name):
        # TODO: get list of ingested files from DB and check against those in the bucket
        # ingested_files = from database
        # files = [f for file in bucket_name if file not in ingested_files]
        files = list_blobs('/' + bucket_name)
        if not files:
            logging.info('No files in cloud bucket {}'.format(bucket_name))
            return GenomicSubProcessResult.NO_FILES
        files = [s.name for s in files
                 if s.name.lower().endswith('.csv')]
        return files

    def _get_last_successful_run_time(self, job_name):
        """Return last successful run's starttime from `genomics_job_runs`"""
        last_run_time = "2019-11-05"
        return last_run_time

    def _create_file_record(self, run_id, path, bucket_name, file_name):
        self.file_processed_dao.insert_file_record(run_id, path,
                                                   bucket_name, file_name)

    def _create_run(self, job_id):
        return self.job_run_dao.insert_run_record(job_id)

    def _get_file_queue_for_run(self, run_id):
        return self.file_processed_dao.get_files_for_run(run_id)

