import logging

from rdr_service import clock, config
from rdr_service.dao.genomics_dao import GenomicAW1RawDao, GenomicAW2RawDao, GenomicAW3RawDao, \
    GenomicAW4RawDao, GenomicJobRunDao, GenomicW2SCRawDao, GenomicW3SRRawDao, GenomicW4WRRawDao, GenomicW3SCRawDao, \
    GenomicW3NSRawDao, GenomicW5NFRawDao, GenomicW3SSRawDao, GenomicW2WRawDao, GenomicW1ILRawDao
from rdr_service.genomic.genomic_cvl_reconciliation import GenomicCVLReconcile
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessResult, GenomicManifestTypes
from rdr_service.services.system_utils import JSONObject


def run_genomic_cron_job(val):
    def inner_decorator(f):
        def wrapped(*args, **kwargs):
            if not config.getSettingJson(config.GENOMIC_CRON_JOBS).get(val):
                raise RuntimeError(f'Cron job for {val} is currently disabled')
            return f(*args, **kwargs)
        return wrapped
    return inner_decorator


def interval_run_schedule(job_id, run_type):
    def inner_decorator(f):
        def wrapped(*args, **kwargs):
            interval_run_map = {
                'skip_week': 14
            }
            today = clock.CLOCK.now()
            day_interval = interval_run_map.get(run_type)

            job_run_dao = GenomicJobRunDao()
            last_run = job_run_dao.get_last_successful_runtime(job_id)

            if last_run and ((today.date() - last_run.date()).days < day_interval):
                raise RuntimeError(f'Cron job for {job_id.name} is currently disabled for this time')
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


def cvl_w1il_manifest_workflow(cvl_site_bucket_map, module_type):
    for site_id in config.GENOMIC_CVL_SITES:
        cvl_bucket_name_key = cvl_site_bucket_map[site_id]
        manifest_type = {
            'pgx': GenomicManifestTypes.CVL_W1IL_PGX,
            'hdr': GenomicManifestTypes.CVL_W1IL_HDR
        }[module_type]

        with GenomicJobController(
            GenomicJob.CVL_W1IL_WORKFLOW,
            bucket_name=cvl_bucket_name_key,
            cvl_site_id=site_id
        ) as controller:
            controller.generate_manifest(
                manifest_type=manifest_type,
                _genome_type=config.GENOME_TYPE_WGS
            )
            for manifest in controller.manifests_generated:
                logging.info(
                    f"Loading W1IL Manifest Raw Data: {manifest['file_path']}")

                # Call pipeline function to load raw
                load_awn_manifest_into_raw_table(
                    manifest['file_path'],
                    "w1il",
                    cvl_site_id=site_id
                )


def cvl_w2w_manifest_workflow(cvl_site_bucket_map):
    for site_id in config.GENOMIC_CVL_SITES:
        cvl_bucket_name_key = cvl_site_bucket_map[site_id]
        with GenomicJobController(
            GenomicJob.CVL_W2W_WORKFLOW,
            bucket_name=cvl_bucket_name_key,
            cvl_site_id=site_id
        ) as controller:
            controller.generate_manifest(
                manifest_type=GenomicManifestTypes.CVL_W2W,
                _genome_type=config.GENOME_TYPE_WGS
            )
            for manifest in controller.manifests_generated:
                logging.info(
                    f"Loading W2W Manifest Raw Data: {manifest['file_path']}")

                # Call pipeline function to load raw
                load_awn_manifest_into_raw_table(
                    manifest['file_path'],
                    "w2w",
                    cvl_site_id=site_id
                )


def cvl_w3sr_manifest_workflow():
    """
    Entrypoint for CVL W3SR Workflow
    """
    for site_id in config.GENOMIC_CVL_SITES:
        with GenomicJobController(
            GenomicJob.CVL_W3SR_WORKFLOW,
            bucket_name=config.BIOBANK_SAMPLES_BUCKET_NAME,
            cvl_site_id=site_id
        ) as controller:
            controller.generate_manifest(
                manifest_type=GenomicManifestTypes.CVL_W3SR,
                _genome_type=config.GENOME_TYPE_WGS,
            )
            for manifest in controller.manifests_generated:
                logging.info(
                    f"Loading W3SR Manifest Raw Data: {manifest['file_path']}")

                # Call pipeline function to load raw
                load_awn_manifest_into_raw_table(
                    manifest['file_path'],
                    "w3sr",
                    cvl_site_id=site_id
                )


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


def update_members_state_resolved_data_files():
    with GenomicJobController(GenomicJob.UPDATE_MEMBERS_STATE_RESOLVED_DATA_FILES) as controller:
        controller.update_member_aw2_missing_states_if_resolved()


def update_members_blocklists():
    with GenomicJobController(GenomicJob.UPDATE_MEMBERS_BLOCKLISTS) as controller:
        controller.update_members_blocklists()


def reconcile_informing_loop_responses():
    with GenomicJobController(GenomicJob.RECONCILE_INFORMING_LOOP_RESPONSES) as controller:
        controller.reconcile_informing_loop_responses()


# Disabling job until further notice
# def reconcile_raw_to_aw1_ingested():
#     with GenomicJobController(GenomicJob.RECONCILE_RAW_AW1_INGESTED) as controller:
#         controller.reconcile_raw_to_aw1_ingested()


def reconcile_raw_to_aw2_ingested():
    with GenomicJobController(GenomicJob.RECONCILE_RAW_AW2_INGESTED) as controller:
        controller.reconcile_raw_to_aw2_ingested()


def reconcile_pdr_data():
    with GenomicJobController(GenomicJob.RECONCILE_PDR_DATA) as controller:
        controller.reconcile_pdr_data()


def retry_manifest_ingestions():
    with GenomicJobController(GenomicJob.RETRY_MANIFEST_INGESTIONS) as controller:
        controller.retry_manifest_ingestions()


def calculate_informing_loop_ready_flags():
    with GenomicJobController(GenomicJob.CALCULATE_INFORMING_LOOP_READY) as controller:
        controller.calculate_informing_loop_ready_flags()


def create_aw2f_manifest(feedback_record):
    with GenomicJobController(GenomicJob.AW2F_MANIFEST,
                              bucket_name=config.BIOBANK_SAMPLES_BUCKET_NAME,
                              ) as controller:
        controller.generate_manifest(GenomicManifestTypes.AW2F,
                                     _genome_type=None,
                                     feedback_record=feedback_record)


def reconcile_cvl_results(reconcile_job_type):
    with GenomicJobController(reconcile_job_type) as controller:
        cvl_reconciler = GenomicCVLReconcile(
            reconcile_type=reconcile_job_type
        )
        cvl_reconciler.run_reconcile()
        controller.job_result = GenomicSubProcessResult.SUCCESS


def results_pipeline_withdrawals():
    with GenomicJobController(GenomicJob.RESULTS_PIPELINE_WITHDRAWALS) as controller:
        controller.check_results_withdrawals()


def gem_results_to_report_state():
    with GenomicJobController(GenomicJob.GEM_RESULT_REPORTS) as controller:
        controller.gem_results_to_report_state()


def reconcile_appointment_events_from_metrics():
    with GenomicJobController(GenomicJob.APPOINTMENT_METRICS_RECONCILE) as controller:
        controller.reconcile_appointment_events_from_metrics()


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
        GenomicJob.CVL_W2SC_WORKFLOW,
        GenomicJob.CVL_W3NS_WORKFLOW,
        GenomicJob.CVL_W3SC_WORKFLOW,
        GenomicJob.CVL_W3SS_WORKFLOW,
        GenomicJob.CVL_W4WR_WORKFLOW,
        GenomicJob.CVL_W5NF_WORKFLOW
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
    provider=None,
    cvl_site_id=None
):
    raw_jobs_map = {
        "aw1": {
            'job_id': GenomicJob.LOAD_AW1_TO_RAW_TABLE,
            'dao': GenomicAW1RawDao
        },
        "aw2": {
            'job_id': GenomicJob.LOAD_AW2_TO_RAW_TABLE,
            'dao': GenomicAW2RawDao
        },
        "aw3": {
            'job_id': GenomicJob.LOAD_AW3_TO_RAW_TABLE,
            'dao': GenomicAW3RawDao
        },
        "aw4": {
            'job_id': GenomicJob.LOAD_AW4_TO_RAW_TABLE,
            'dao': GenomicAW4RawDao
        },
        "w1il": {
            'job_id': GenomicJob.LOAD_CVL_W1IL_TO_RAW_TABLE,
            'dao': GenomicW1ILRawDao
        },
        "w2sc": {
            'job_id': GenomicJob.LOAD_CVL_W2SC_TO_RAW_TABLE,
            'dao': GenomicW2SCRawDao
        },
        "w2w": {
            'job_id': GenomicJob.LOAD_CVL_W2W_TO_RAW_TABLE,
            'dao': GenomicW2WRawDao
        },
        "w3ns": {
            'job_id': GenomicJob.LOAD_CVL_W3NS_TO_RAW_TABLE,
            'dao': GenomicW3NSRawDao
        },
        "w3sc": {
            'job_id': GenomicJob.LOAD_CVL_W3SC_TO_RAW_TABLE,
            'dao': GenomicW3SCRawDao
        },
        "w3ss": {
            'job_id': GenomicJob.LOAD_CVL_W3SS_TO_RAW_TABLE,
            'dao': GenomicW3SSRawDao
        },
        "w3sr": {
            'job_id': GenomicJob.LOAD_CVL_W3SR_TO_RAW_TABLE,
            'dao': GenomicW3SRRawDao
        },
        "w4wr": {
            'job_id': GenomicJob.LOAD_CVL_W4WR_TO_RAW_TABLE,
            'dao': GenomicW4WRRawDao
        },
        "w5nf": {
            'job_id': GenomicJob.LOAD_CVL_W5NF_TO_RAW_TABLE,
            'dao': GenomicW5NFRawDao
        },
    }

    raw_job = raw_jobs_map.get(manifest_type)
    if not raw_job:
        return

    with GenomicJobController(raw_job.get('job_id'),
                              bq_project_id=project_id,
                              storage_provider=provider) as controller:
        controller.load_raw_awn_data_from_filepath(
            file_path,
            raw_job.get('dao'),
            cvl_site_id=cvl_site_id
        )


def notify_email_group_of_w1il_gror_resubmit_participants(since_datetime):
    with GenomicJobController(GenomicJob.CHECK_FOR_W1IL_GROR_RESUBMIT) as controller:
        controller.check_w1il_gror_resubmit(since_datetime=since_datetime)


def notify_aw3_ready_missing_data_files():
    with GenomicJobController(GenomicJob.AW3_MISSING_DATA_FILE_REPORT) as controller:
        controller.check_aw3_ready_missing_files()
