import logging

from rdr_service.dao.genomics_dao import GenomicSetDao
from rdr_service.genomic import (
    genomic_biobank_menifest_handler,
    genomic_set_file_handler,
    validation,
    genomic_center_menifest_handler,
    genomic_job_controller
)
from rdr_service.participant_enums import (
    GenomicSetStatus,
    GenomicSubProcessStatus,
    GenomicSubProcessResult,
    GenomicJob
)


def process_genomic_water_line():
    """
  Entrypoint, executed as a cron job
  """
    genomic_set_id = genomic_set_file_handler.read_genomic_set_from_bucket()
    if genomic_set_id is not None:
        logging.info("Read input genomic set file successfully.")
        dao = GenomicSetDao()
        validation.validate_and_update_genomic_set_by_id(genomic_set_id, dao)
        genomic_set = dao.get(genomic_set_id)
        if genomic_set.genomicSetStatus == GenomicSetStatus.VALID:
            genomic_biobank_menifest_handler.create_and_upload_genomic_biobank_manifest_file(genomic_set_id)
            logging.info("Validation passed, generate biobank manifest file successfully.")
        else:
            logging.info("Validation failed.")
        genomic_set_file_handler.create_genomic_set_status_result_file(genomic_set_id)
    else:
        logging.info("No file found or nothing read from genomic set file")

    genomic_biobank_menifest_handler.process_genomic_manifest_result_file_from_bucket()
    genomic_center_menifest_handler.process_genotyping_manifest_files()


def ingest_genomic_centers_metrics_files():
    """
    Entrypoint for GC Metrics File Ingestion subprocess of genomic_pipeline.
    """
    job_id = GenomicJob.METRICS_INGESTION

    run_controller = genomic_job_controller.GenomicJobController(job_id)
    file_queue_result = run_controller.generate_file_processing_queue()

    if file_queue_result == GenomicSubProcessResult.NO_FILES:
        logging.info('No files to process.')
        run_controller.end_run(file_queue_result)
    else:
        while len(run_controller.file_queue) > 0:
            try:
                ingestion_result = run_controller.process_file_using_ingester(
                    run_controller.file_queue[0])
                file_ingested = run_controller.file_queue.popleft()
                run_controller.update_file_processed(
                    file_ingested.id,
                    GenomicSubProcessStatus.COMPLETED,
                    ingestion_result
                )
            except IndexError:
                logging.info('No files left in file queue.')

        run_result = run_controller.aggregate_run_results()
        run_controller.end_run(run_result)


def reconcile_metrics():
    """
    Entrypoint for GC Metrics File reconciliation
    against Manifest subprocess of genomic_pipeline.
    """
    job_id = GenomicJob.RECONCILE_MANIFEST
    run_controller = genomic_job_controller.GenomicJobController(job_id)
    result = run_controller.run_reconciliation_to_manifest()
    run_controller.end_run(result)
