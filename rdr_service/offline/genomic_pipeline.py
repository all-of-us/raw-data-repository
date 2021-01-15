import logging
from rdr_service.services.system_utils import JSONObject

from rdr_service.dao.genomics_dao import GenomicSetDao
from rdr_service.genomic import (
    genomic_biobank_manifest_handler,
    genomic_set_file_handler,
    validation,
    genomic_center_manifest_handler
)
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.participant_enums import (
    GenomicSetStatus,
    GenomicJob,
    GenomicManifestTypes,
    GenomicSubProcessResult)
import rdr_service.config as config


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
            genomic_biobank_manifest_handler.create_and_upload_genomic_biobank_manifest_file(genomic_set_id)
            logging.info("Validation passed, generate biobank manifest file successfully.")
        else:
            logging.info("Validation failed.")
        genomic_set_file_handler.create_genomic_set_status_result_file(genomic_set_id)
    else:
        logging.info("No file found or nothing read from genomic set file")

    genomic_biobank_manifest_handler.process_genomic_manifest_result_file_from_bucket()
    genomic_center_manifest_handler.process_genotyping_manifest_files()


def new_participant_workflow():
    """
    Entrypoint for New Participant Workflow (Cohort 3),
    Sources from newly-created biobank_stored_samples
    from the BiobankSamplesPipeline.
    """
    with GenomicJobController(GenomicJob.NEW_PARTICIPANT_WORKFLOW) as controller:
        controller.run_new_participant_workflow()


def c2_participant_workflow():
    """
    Entrypoint for Cohort 2 Participant Workflow,
    Sources from Cohort 2 participants that have reconsented.
    """
    with GenomicJobController(GenomicJob.C2_PARTICIPANT_WORKFLOW) as controller:
        controller.run_c2_participant_workflow()


def c1_participant_workflow():
    """
    Entrypoint for Cohort 1 Participant Workflow,
    Sources from Cohort 1 participants that have reconsented.
    """
    with GenomicJobController(GenomicJob.C1_PARTICIPANT_WORKFLOW) as controller:
        controller.run_c1_participant_workflow()


def genomic_centers_manifest_workflow():
    """
    Entrypoint for Ingestion:
        Biobank to Genomic Centers Manifest (AW1)
    """
    with GenomicJobController(GenomicJob.AW1_MANIFEST,
                              bucket_name=None,
                              bucket_name_list=config.GENOMIC_CENTER_BUCKET_NAME,
                              sub_folder_tuple=config.GENOMIC_AW1_SUBFOLDERS
                              ) as controller:
        controller.run_genomic_centers_manifest_workflow()


def genomic_centers_aw1f_manifest_workflow():
    """
        Entrypoint for Ingestion:
            Failure Manifest (AW1F)
        """
    with GenomicJobController(GenomicJob.AW1F_MANIFEST,
                              bucket_name=None,
                              bucket_name_list=config.GENOMIC_CENTER_BUCKET_NAME,
                              sub_folder_tuple=config.GENOMIC_AW1F_SUBFOLDERS
                              ) as controller:
        controller.run_aw1f_manifest_workflow()


def ingest_aw1c_manifest():
    """
    Entrypoint for CVL AW1C Manifest Ingestion workflow
    """
    with GenomicJobController(GenomicJob.AW1C_INGEST,
                              bucket_name=None,
                              bucket_name_list=config.GENOMIC_CENTER_BUCKET_NAME,
                              sub_folder_name=config.GENOMIC_CVL_AW1C_MANIFEST_SUBFOLDER) as controller:
        controller.run_aw1c_workflow()


def ingest_aw1cf_manifest_workflow():
    """
    Entrypoint for CVL Failure Manifest (AW1CF) Ingestion
    """
    with GenomicJobController(GenomicJob.AW1CF_INGEST,
                              bucket_name=None,
                              bucket_name_list=config.GENOMIC_CENTER_BUCKET_NAME,
                              sub_folder_tuple=config.GENOMIC_CVL_AW1CF_MANIFEST_SUBFOLDER
                              ) as controller:
        controller.run_aw1cf_manifest_workflow()


def aw1cf_alerts_workflow():
    """
        Entrypoint for Accessioning Alerts:
            CVL Failure Manifest (AW1CF)
        """
    with GenomicJobController(GenomicJob.AW1CF_ALERTS,
                              bucket_name=None,
                              bucket_name_list=config.GENOMIC_CENTER_BUCKET_NAME,
                              sub_folder_tuple=config.GENOMIC_CVL_AW1CF_MANIFEST_SUBFOLDER
                              ) as controller:
        controller.process_failure_manifests_for_alerts()


def genomic_centers_accessioning_failures_workflow():
    """
        Entrypoint for Accessioning Alerts:
            Failure Manifest (AW1F)
        """
    with GenomicJobController(GenomicJob.AW1F_ALERTS,
                              bucket_name=None,
                              bucket_name_list=config.GENOMIC_CENTER_BUCKET_NAME,
                              sub_folder_tuple=config.GENOMIC_AW1F_SUBFOLDERS
                              ) as controller:
        controller.process_failure_manifests_for_alerts()


def ingest_genomic_centers_metrics_files(provider=None):
    """
    Entrypoint for GC Metrics File Ingestion subprocess of genomic_pipeline.
    """
    with GenomicJobController(GenomicJob.METRICS_INGESTION,
                              bucket_name=None,
                              bucket_name_list=config.GENOMIC_CENTER_DATA_BUCKET_NAME,
                              sub_folder_tuple=config.GENOMIC_AW2_SUBFOLDERS,
                              storage_provider=provider) as controller:
        controller.ingest_gc_metrics()


def reconcile_metrics_vs_genotyping_data(provider=None):
    """
    Entrypoint for GC Metrics File reconciliation
    Genotyping Files (Array) vs Listed in Manifest.
    """
    with GenomicJobController(GenomicJob.RECONCILE_GENOTYPING_DATA,
                              storage_provider=provider,
                              bucket_name_list=config.GENOMIC_CENTER_DATA_BUCKET_NAME) as controller:
        controller.run_reconciliation_to_genotyping_data()


def reconcile_metrics_vs_sequencing_data(provider=None):
    """
    Entrypoint for GC Metrics File reconciliation
    Sequencing Files (WGS) vs Listed in Manifest.
    """
    with GenomicJobController(GenomicJob.RECONCILE_SEQUENCING_DATA,
                              storage_provider=provider,
                              bucket_name_list=config.GENOMIC_CENTER_DATA_BUCKET_NAME) as controller:
        controller.run_reconciliation_to_sequencing_data()


def aw3_array_manifest_workflow():
    """
    Entrypoint for AW3 Array Workflow
    """
    with GenomicJobController(GenomicJob.AW3_ARRAY_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME) as controller:
        controller.generate_manifest(GenomicManifestTypes.AW3_ARRAY, _genome_type=config.GENOME_TYPE_ARRAY)


def aw3_wgs_manifest_workflow():
    """
    Entrypoint for AW3 WGS Workflow
    """
    with GenomicJobController(GenomicJob.AW3_WGS_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME) as controller:
        controller.generate_manifest(GenomicManifestTypes.AW3_WGS, _genome_type=config.GENOME_TYPE_WGS)


def aw4_array_manifest_workflow():
    """
    Entrypoint for AW4 Array Workflow
    """
    with GenomicJobController(GenomicJob.AW4_ARRAY_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME,
                              sub_folder_name=config.getSetting(config.DRC_BROAD_AW4_SUBFOLDERS[0])
                              ) as controller:
        controller.run_general_ingestion_workflow()


def aw4_wgs_manifest_workflow():
    """
    Entrypoint for AW4 WGS Workflow
    """
    with GenomicJobController(GenomicJob.AW4_WGS_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME,
                              sub_folder_name=config.getSetting(config.DRC_BROAD_AW4_SUBFOLDERS[1])
                              ) as controller:
        controller.run_general_ingestion_workflow()


def gem_a1_manifest_workflow():
    """
    Entrypoint for GEM A1 Workflow
    First workflow in GEM Workflow
    """
    with GenomicJobController(GenomicJob.GEM_A1_MANIFEST,
                              bucket_name=config.GENOMIC_GEM_BUCKET_NAME) as controller:
        controller.reconcile_report_states(_genome_type=config.GENOME_TYPE_ARRAY)
        controller.generate_manifest(GenomicManifestTypes.GEM_A1, _genome_type=config.GENOME_TYPE_ARRAY)


def gem_a2_manifest_workflow():
    """
    Entrypoint for GEM A2 Workflow
    """
    with GenomicJobController(GenomicJob.GEM_A2_MANIFEST,
                              bucket_name=config.GENOMIC_GEM_BUCKET_NAME) as controller:
        controller.reconcile_report_states(_genome_type=config.GENOME_TYPE_ARRAY)
        controller.run_general_ingestion_workflow()


def gem_a3_manifest_workflow():
    """
    Entrypoint for GEM A3 Workflow
    """
    with GenomicJobController(GenomicJob.GEM_A3_MANIFEST,
                              bucket_name=config.GENOMIC_GEM_BUCKET_NAME) as controller:
        controller.reconcile_report_states(_genome_type=config.GENOME_TYPE_ARRAY)
        controller.generate_manifest(GenomicManifestTypes.GEM_A3, _genome_type=config.GENOME_TYPE_ARRAY)


def gem_metrics_ingest():
    """
    Entrypoint for the GEM Metrics ingestion from Color
    """
    with GenomicJobController(GenomicJob.GEM_METRICS_INGEST,
                              bucket_name=config.GENOMIC_GEM_BUCKET_NAME) as controller:
        controller.run_general_ingestion_workflow()


def create_cvl_reconciliation_report():
    """
    Entrypoint for CVL reconciliation workflow
    Sources from genomic_set_member and produces CVL reconciliation report CSV
    """
    with GenomicJobController(GenomicJob.CVL_RECONCILIATION_REPORT) as controller:
        controller.run_cvl_reconciliation_report()


def create_cvl_w1_manifest():
    """
    Entrypoint for CVL Manifest workflow
    Sources from list of biobank_ids from CVL reconciliation report
    """
    with GenomicJobController(GenomicJob.CREATE_CVL_W1_MANIFESTS,
                              bucket_name=config.GENOMIC_CVL_BUCKET_NAME) as controller:
        controller.generate_manifest(GenomicManifestTypes.CVL_W1, _genome_type=config.GENOME_TYPE_WGS)


def ingest_cvl_w2_manifest():
    """
    Entrypoint for CVL W2 Manifest Ingestion workflow
    Sources from list of biobank_ids from CVL reconciliation report
    """
    with GenomicJobController(GenomicJob.W2_INGEST,
                              bucket_name=config.GENOMIC_CVL_BUCKET_NAME,
                              sub_folder_name=config.CVL_W2_MANIFEST_SUBFOLDER) as controller:
        controller.run_general_ingestion_workflow()


def create_cvl_w3_manifest():
    """
    Entrypoint for CVL W3 Manifest workflow
    """
    with GenomicJobController(GenomicJob.W3_MANIFEST,
                              bucket_name=config.GENOMIC_CVL_BUCKET_NAME,) as controller:
        controller.generate_manifest(GenomicManifestTypes.CVL_W3, _genome_type=config.GENOME_TYPE_CVL)


def scan_and_complete_feedback_records():
    """
    Entrypoint for AW2F Manifest workflow
    """
    with GenomicJobController(GenomicJob.FEEDBACK_SCAN) as controller:
        # Get feedback records that are complete
        fb_recs = controller.get_feedback_complete_records()

        for f in fb_recs:
            create_aw2f_manifest(f)


def create_aw2f_manifest(feedback_record):
    with GenomicJobController(GenomicJob.AW2F_MANIFEST,
                              bucket_name=config.BIOBANK_SAMPLES_BUCKET_NAME,
                              ) as controller:
        controller.generate_manifest(GenomicManifestTypes.AW2F,
                                     _genome_type=config.GENOME_TYPE_ARRAY,
                                     feedback_record=feedback_record)


def execute_genomic_manifest_file_pipeline(_task_data: dict, project_id=None):
    """
    Entrypoint for new genomic manifest file pipelines
    Sets up the genomic manifest file record and begin pipeline
    :param project_id:
    :param _task_data: dictionary of metadata needed by the controller
    """
    task_data = JSONObject(_task_data)

    if not hasattr(task_data, 'job'):
        raise AttributeError("job are required to execute manifest file pipeline")

    if not hasattr(task_data, 'bucket'):
        raise AttributeError("bucket is required to execute manifest file pipeline")

    if not hasattr(task_data, 'file_data'):
        raise AttributeError("file_data is required to execute manifest file pipeline")

    with GenomicJobController(GenomicJob.GENOMIC_MANIFEST_FILE_TRIGGER,
                              task_data=task_data, bq_project_id=project_id) as controller:
        manifest_file = controller.insert_genomic_manifest_file_record()

        if task_data.file_data.create_feedback_record:
            controller.insert_genomic_manifest_feedback_record(manifest_file)

        controller.job_result = GenomicSubProcessResult.SUCCESS

    if task_data.job:
        task_data.manifest_file = manifest_file
        dispatch_genomic_job_from_task(task_data)

    else:
        return manifest_file


def dispatch_genomic_job_from_task(_task_data: JSONObject):
    """
    Entrypoint for new genomic manifest file pipelines
    Sets up the genomic manifest file record and begin pipeline
    :param _task_data: dictionary of metadata needed by the controller
    """
    if _task_data.job in (GenomicJob.AW1_MANIFEST, GenomicJob.METRICS_INGESTION):

        with GenomicJobController(_task_data.job,
                                  task_data=_task_data) as controller:

            controller.bucket_name = _task_data.bucket
            file_name = '/'.join(_task_data.file_data.file_path.split('/')[1:])

            controller.ingest_specific_manifest(file_name)

    else:
        logging.error(f'No task for {_task_data.job}')
