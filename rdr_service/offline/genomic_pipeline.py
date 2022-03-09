import logging
from rdr_service.services.system_utils import JSONObject
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessResult, GenomicManifestTypes
import rdr_service.config as config


def run_genomic_cron_job(val):
    def inner_decorator(f):
        def wrapped(*args, **kwargs):
            if not config.getSettingJson(config.GENOMIC_CRON_JOBS).get(val):
                logging.info(f'Cron job for {val} is currently disabled')
                raise RuntimeError
            return f(*args, **kwargs)
        return wrapped
    return inner_decorator


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


def reconcile_metrics_vs_array_data(provider=None):
    """
    Entrypoint for GC Metrics File reconciliation
    Array Files vs Listed in Manifest.
    """
    with GenomicJobController(GenomicJob.RECONCILE_ARRAY_DATA,
                              storage_provider=provider,
                              bucket_name_list=config.GENOMIC_CENTER_DATA_BUCKET_NAME) as controller:
        controller.run_reconciliation_to_data(
            genome_type=config.GENOME_TYPE_ARRAY,
        )


def reconcile_metrics_vs_wgs_data(provider=None):
    """
    Entrypoint for GC Metrics File reconciliation
    WGS Files vs Listed in Manifest.
    """
    with GenomicJobController(GenomicJob.RECONCILE_WGS_DATA,
                              storage_provider=provider,
                              bucket_name_list=config.GENOMIC_CENTER_DATA_BUCKET_NAME) as controller:
        controller.run_reconciliation_to_data(
            genome_type=config.GENOME_TYPE_WGS
        )


def aw3_array_manifest_workflow():
    """
    Entrypoint for AW3 Array Workflow
    """
    with GenomicJobController(GenomicJob.AW3_ARRAY_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME,
                              max_num=config.getSetting(config.GENOMIC_MAX_NUM_GENERATE, default=4000)) as controller:
        controller.generate_manifest(
            GenomicManifestTypes.AW3_ARRAY,
            _genome_type=config.GENOME_TYPE_ARRAY,
        )

        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading AW3 Array Raw Data: {manifest['file_path']}")

            # Call pipeline function to load raw
            load_awn_manifest_into_raw_table(manifest['file_path'], "aw3")


def aw3_wgs_manifest_workflow():
    """
    Entrypoint for AW3 WGS Workflow
    """
    with GenomicJobController(GenomicJob.AW3_WGS_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME,
                              max_num=config.getSetting(config.GENOMIC_MAX_NUM_GENERATE, default=4000)) as controller:
        controller.generate_manifest(
            GenomicManifestTypes.AW3_WGS,
            _genome_type=config.GENOME_TYPE_WGS,
        )

        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading AW3 Array Raw Data: {manifest['file_path']}")

            # Call pipeline function to load raw
            load_awn_manifest_into_raw_table(manifest['file_path'], "aw3")


def cvl_w3sr_manifest_workflow():
    """
    Entrypoint for CVL W3SR Workflow
    """
    for site in config.GENOMIC_CVL_SITES:
        with GenomicJobController(
            GenomicJob.CVL_W3SR_WORKFLOW,
            bucket_name=config.BIOBANK_SAMPLES_BUCKET_NAME
        ) as controller:

            controller.cvl_site_id = site
            controller.generate_manifest(
                GenomicManifestTypes.CVL_W3SR,
                _genome_type=config.GENOME_TYPE_WGS,
            )

            for manifest in controller.manifests_generated:
                logging.info(
                    f"Loading W3SR Investigation Raw Data: {manifest['file_path']}")

                # Call pipeline function to load raw
                load_awn_manifest_into_raw_table(manifest['file_path'], "w3sr")


def aw3_array_investigation_workflow():
    """
    Entrypoint for AW3 Array Workflow
    """
    with GenomicJobController(GenomicJob.AW3_ARRAY_INVESTIGATION_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME,
                              max_num=config.getSetting(config.GENOMIC_MAX_NUM_GENERATE, default=4000)) as controller:
        controller.generate_manifest(
            GenomicManifestTypes.AW3_ARRAY,
            _genome_type="aou_array_investigation",
        )

        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading AW3 Array Investigation Raw Data: {manifest['file_path']}")

            # Call pipeline function to load raw
            load_awn_manifest_into_raw_table(manifest['file_path'], "aw3")


def aw3_wgs_investigation_workflow():
    """
    Entrypoint for AW3 WGS Workflow
    """
    with GenomicJobController(GenomicJob.AW3_WGS_INVESTIGATION_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME,
                              max_num=config.getSetting(config.GENOMIC_MAX_NUM_GENERATE, default=4000)) as controller:
        controller.generate_manifest(
            GenomicManifestTypes.AW3_WGS,
            _genome_type="aou_wgs_investigation",
        )

        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading AW3 WGS Investigation Raw Data: {manifest['file_path']}")

            # Call pipeline function to load raw
            load_awn_manifest_into_raw_table(manifest['file_path'], "aw3")


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
                              bucket_name=config.GENOMIC_GEM_BUCKET_NAME,
                              sub_folder_name=config.GENOMIC_GEM_A2_MANIFEST_SUBFOLDER) as controller:
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


def update_report_state_for_consent_removal():
    """
    Comprehensive update for report states without gRoR or Primary Consent
    :return:
    """
    with GenomicJobController(GenomicJob.UPDATE_REPORT_STATES_FOR_CONSENT_REMOVAL) as controller:
        controller.reconcile_report_states()


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
        fb_recs = controller.get_feedback_records_to_send()
        for f in fb_recs:
            create_aw2f_manifest(f)


def send_remainder_contamination_manifests():
    """
    Entrypoint for AW2F Manifest monthly remainder
    """
    with GenomicJobController(GenomicJob.GENERATE_AW2F_REMAINDER) as controller:
        # Get feedback records that have been sent and have new data
        feedback_records = controller.get_aw2f_remainder_records()
        for feedback_record in feedback_records:
            create_aw2f_manifest(feedback_record)


def feedback_record_reconciliation():
    with GenomicJobController(GenomicJob.FEEDBACK_RECORD_RECONCILE) as controller:
        controller.reconcile_feedback_records()


def genomic_missing_files_clean_up(num_days=90):
    with GenomicJobController(GenomicJob.MISSING_FILES_CLEANUP) as controller:
        controller.gc_missing_files_record_clean_up(num_days)


def genomic_missing_files_resolve():
    with GenomicJobController(GenomicJob.RESOLVE_MISSING_FILES) as controller:
        controller.resolve_missing_gc_files()


def update_members_state_resolved_data_files():
    with GenomicJobController(GenomicJob.UPDATE_MEMBERS_STATE_RESOLVED_DATA_FILES) as controller:
        controller.update_member_aw2_missing_states_if_resolved()


def update_members_blocklists():
    with GenomicJobController(GenomicJob.UPDATE_MEMBERS_BLOCKLISTS) as controller:
        controller.update_members_blocklists()


def reconcile_informing_loop_responses():
    with GenomicJobController(GenomicJob.RECONCILE_INFORMING_LOOP_RESPONSES) as controller:
        controller.reconcile_informing_loop_responses()


def delete_old_gp_user_events(days=7):
    with GenomicJobController(GenomicJob.DELETE_OLD_GP_USER_EVENT_METRICS) as controller:
        controller.delete_old_gp_user_event_metrics(days=days)


def reconcile_gc_data_file_to_table():
    with GenomicJobController(GenomicJob.RECONCILE_GC_DATA_FILE_TO_TABLE) as controller:
        controller.reconcile_gc_data_file_to_table()


def reconcile_raw_to_aw1_ingested():
    with GenomicJobController(GenomicJob.RECONCILE_RAW_AW1_INGESTED) as controller:
        controller.reconcile_raw_to_aw1_ingested()


def reconcile_raw_to_aw2_ingested():
    with GenomicJobController(GenomicJob.RECONCILE_RAW_AW2_INGESTED) as controller:
        controller.reconcile_raw_to_aw2_ingested()


def reconcile_pdr_data():
    with GenomicJobController(GenomicJob.RECONCILE_PDR_DATA) as controller:
        controller.reconcile_pdr_data()


def retry_manifest_ingestions():
    with GenomicJobController(GenomicJob.RETRY_MANIFEST_INGESTIONS) as controller:
        controller.retry_manifest_ingestions()


def create_aw2f_manifest(feedback_record):
    with GenomicJobController(GenomicJob.AW2F_MANIFEST,
                              bucket_name=config.BIOBANK_SAMPLES_BUCKET_NAME,
                              ) as controller:
        controller.generate_manifest(GenomicManifestTypes.AW2F,
                                     _genome_type=None,
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
                              task_data=task_data,
                              bq_project_id=project_id) as controller:
        manifest_file = controller.insert_genomic_manifest_file_record()

        if task_data.file_data.create_feedback_record:
            controller.insert_genomic_manifest_feedback_record(manifest_file)

        controller.job_result = GenomicSubProcessResult.SUCCESS

    if task_data.job:
        task_data.manifest_file = manifest_file
        dispatch_genomic_job_from_task(task_data)
    else:
        return manifest_file


def dispatch_genomic_job_from_task(_task_data: JSONObject, project_id=None):
    """
    Entrypoint for new genomic manifest file pipelines
    Sets up the genomic manifest file record and begin pipeline
    :param project_id:
    :param _task_data: dictionary of metadata needed by the controller
    """

    ingestion_workflows = (
        GenomicJob.AW1_MANIFEST,
        GenomicJob.AW1F_MANIFEST,
        GenomicJob.METRICS_INGESTION,
        GenomicJob.AW4_ARRAY_WORKFLOW,
        GenomicJob.AW4_WGS_WORKFLOW,
        GenomicJob.AW5_ARRAY_MANIFEST,
        GenomicJob.AW5_WGS_MANIFEST,
        GenomicJob.CVL_W2SC_WORKFLOW
    )

    if _task_data.job in ingestion_workflows:
        # Ingestion Job
        with GenomicJobController(_task_data.job,
                                  task_data=_task_data,
                                  sub_folder_name=_task_data.subfolder if hasattr(_task_data, 'subfolder') else None,
                                  bq_project_id=project_id,
                                  max_num=config.getSetting(config.GENOMIC_MAX_NUM_INGEST, default=1000)
                                  ) as controller:

            controller.bucket_name = _task_data.bucket
            file_name = '/'.join(_task_data.file_data.file_path.split('/')[1:])
            controller.ingest_specific_manifest(file_name)

        if _task_data.job == GenomicJob.AW1_MANIFEST:
            # count records for AW1 manifest in new job
            _task_data.job = GenomicJob.CALCULATE_RECORD_COUNT_AW1
            dispatch_genomic_job_from_task(_task_data)

    if _task_data.job == GenomicJob.CALCULATE_RECORD_COUNT_AW1:
        # Calculate manifest record counts job
        with GenomicJobController(_task_data.job,
                                  bq_project_id=project_id) as controller:

            logging.info("Calculating record count for AW1 manifest...")

            rec_count = controller.manifest_file_dao.count_records_for_manifest_file(
                _task_data.manifest_file
            )

            controller.manifest_file_dao.update_record_count(
                _task_data.manifest_file,
                rec_count
            )


def load_awn_manifest_into_raw_table(
    file_path,
    manifest_type,
    project_id=None,
    provider=None
):
    jobs = {
        "aw1": GenomicJob.LOAD_AW1_TO_RAW_TABLE,
        "aw2": GenomicJob.LOAD_AW2_TO_RAW_TABLE,
        "aw3": GenomicJob.LOAD_AW3_TO_RAW_TABLE,
        "aw4": GenomicJob.LOAD_AW4_TO_RAW_TABLE,
        "w2sc": GenomicJob.LOAD_CVL_W2SC_TO_RAW_TABLE,
        "w3sr": GenomicJob.LOAD_CVL_W3SR_TO_RAW_TABLE
    }
    job_id = jobs.get(manifest_type)

    if not job_id:
        return

    with GenomicJobController(job_id,
                              bq_project_id=project_id,
                              storage_provider=provider) as controller:
        controller.load_raw_awn_data_from_filepath(file_path)
