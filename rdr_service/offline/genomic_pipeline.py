import logging

from rdr_service.dao.genomics_dao import GenomicSetDao
from rdr_service.genomic import (
    genomic_biobank_menifest_handler,
    genomic_set_file_handler,
    validation,
    genomic_center_menifest_handler
)
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.participant_enums import (
    GenomicSetStatus,
    GenomicJob,
    GenomicManifestTypes
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
    with GenomicJobController(GenomicJob.METRICS_INGESTION) as controller:
        controller.ingest_gc_metrics()


def reconcile_metrics_vs_manifest():
    """
    Entrypoint for GC Metrics File reconciliation
    against Manifest subprocess of genomic_pipeline.
    """
    with GenomicJobController(GenomicJob.RECONCILE_MANIFEST) as controller:
        controller.run_reconciliation_to_manifest()


def reconcile_metrics_vs_sequencing():
    """
    Entrypoint for GC Metrics File reconciliation
    against Sequencing Files subprocess of genomic_pipeline.
    """
    with GenomicJobController(GenomicJob.RECONCILE_SEQUENCING) as controller:
        controller.run_reconciliation_to_sequencing()


def new_participant_workflow():
    """
    Entrypoint for New Participant Workflow,
    Sources from newly-created biobank_stored_samples
    from the BiobankSamplesPipeline.
    """
    with GenomicJobController(GenomicJob.NEW_PARTICIPANT_WORKFLOW) as controller:
        controller.run_new_participant_workflow()


def create_cvl_reconciliation_report():
    """
    Entrypoint for CVL reconciliation workflow
    Sources from genomic_set_member and produces CVL reconciliation report CSV
    """
    with GenomicJobController(GenomicJob.CVL_RECONCILIATION_REPORT) as controller:
        controller.run_cvl_reconciliation_report()


def create_cvl_manifests():
    """
    Entrypoint for CVL Manifest workflow
    Sources from list of biobank_ids from CVL reconciliation report
    """
    with GenomicJobController(GenomicJob.CREATE_CVL_MANIFESTS) as controller:
        controller.generate_manifest(GenomicManifestTypes.DRC_CVL_WGS)
        controller.generate_manifest(GenomicManifestTypes.DRC_CVL_ARR)
