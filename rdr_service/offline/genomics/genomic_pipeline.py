import logging

from rdr_service import config
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic.genomic_storage_class import GenomicStorageClass
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessResult, GenomicManifestTypes
from rdr_service.offline.genomics.genomic_dispatch import load_manifest_into_raw_table


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
            genome_type=config.GENOME_TYPE_ARRAY,
        )

        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading AW3 Array Raw Data: {manifest['file_path']}")

            # Call pipeline function to load raw
            load_manifest_into_raw_table(manifest['file_path'], "aw3")


def aw3_wgs_manifest_workflow(**kwargs):
    """
    Entrypoint for AW3 WGS Workflow
    """
    with GenomicJobController(GenomicJob.AW3_WGS_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME,
                              max_num=config.getSetting(config.GENOMIC_MAX_NUM_GENERATE, default=4000)) as controller:
        controller.generate_manifest(
            GenomicManifestTypes.AW3_WGS,
            genome_type=config.GENOME_TYPE_WGS,
            pipeline_id=kwargs.get('pipeline_id')
        )

        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading AW3 Array Raw Data: {manifest['file_path']}")

            # Call pipeline function to load raw
            load_manifest_into_raw_table(manifest['file_path'], "aw3")


def aw3_array_investigation_workflow():
    """
    Entrypoint for AW3 Array Workflow
    """
    with GenomicJobController(GenomicJob.AW3_ARRAY_INVESTIGATION_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME,
                              max_num=config.getSetting(config.GENOMIC_MAX_NUM_GENERATE, default=4000)) as controller:
        controller.generate_manifest(
            GenomicManifestTypes.AW3_ARRAY,
            genome_type="aou_array_investigation",
        )

        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading AW3 Array Investigation Raw Data: {manifest['file_path']}")

            # Call pipeline function to load raw
            load_manifest_into_raw_table(manifest['file_path'], "aw3")


def aw3_wgs_investigation_workflow(**kwargs):
    """
    Entrypoint for AW3 WGS Workflow
    """
    with GenomicJobController(GenomicJob.AW3_WGS_INVESTIGATION_WORKFLOW,
                              bucket_name=config.DRC_BROAD_BUCKET_NAME,
                              max_num=config.getSetting(config.GENOMIC_MAX_NUM_GENERATE, default=4000)) as controller:
        controller.generate_manifest(
            GenomicManifestTypes.AW3_WGS,
            genome_type="aou_wgs_investigation",
            pipeline_id=kwargs.get('pipeline_id')
        )

        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading AW3 WGS Investigation Raw Data: {manifest['file_path']}")

            # Call pipeline function to load raw
            load_manifest_into_raw_table(manifest['file_path'], "aw3")


def gem_a1_manifest_workflow():
    """
    Entrypoint for GEM A1 Workflow
    First workflow in GEM Workflow
    """
    with GenomicJobController(GenomicJob.GEM_A1_MANIFEST,
                              bucket_name=config.GENOMIC_GEM_BUCKET_NAME) as controller:
        controller.reconcile_report_states(genome_type=config.GENOME_TYPE_ARRAY)
        controller.generate_manifest(
            GenomicManifestTypes.GEM_A1,
            genome_type=config.GENOME_TYPE_ARRAY
        )


# def gem_a2_manifest_workflow():
#     """
#     Entrypoint for GEM A2 Workflow
#     """
#     with GenomicJobController(GenomicJob.GEM_A2_MANIFEST,
#                               bucket_name=config.GENOMIC_GEM_BUCKET_NAME,
#                               sub_folder_name=config.GENOMIC_GEM_A2_MANIFEST_SUBFOLDER) as controller:
#         controller.reconcile_report_states(genome_type=config.GENOME_TYPE_ARRAY)
#         controller.run_general_ingestion_workflow()


def gem_a3_manifest_workflow():
    """
    Entrypoint for GEM A3 Workflow
    """
    with GenomicJobController(GenomicJob.GEM_A3_MANIFEST,
                              bucket_name=config.GENOMIC_GEM_BUCKET_NAME) as controller:
        controller.reconcile_report_states(genome_type=config.GENOME_TYPE_ARRAY)
        controller.generate_manifest(
            GenomicManifestTypes.GEM_A3,
            genome_type=config.GENOME_TYPE_ARRAY
        )


def update_report_state_for_consent_removal():
    """
    Comprehensive update for report states without gRoR or Primary Consent
    :return:
    """
    with GenomicJobController(GenomicJob.UPDATE_REPORT_STATES_FOR_CONSENT_REMOVAL) as controller:
        controller.reconcile_report_states()


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
        controller.generate_manifest(
            GenomicManifestTypes.AW2F,
            genome_type=None,
            feedback_record=feedback_record
        )


def results_pipeline_withdrawals():
    with GenomicJobController(GenomicJob.RESULTS_PIPELINE_WITHDRAWALS) as controller:
        controller.check_results_withdrawals()


def gem_results_to_report_state():
    with GenomicJobController(GenomicJob.GEM_RESULT_REPORTS) as controller:
        controller.gem_results_to_report_state()


def genomic_update_storage_class(storage_job_type):
    with GenomicJobController(storage_job_type) as controller:
        genomic_storage = GenomicStorageClass(
            storage_job_type=storage_job_type
        )
        genomic_storage.run_storage_update()
        controller.job_result = GenomicSubProcessResult.SUCCESS


def notify_aw3_ready_missing_data_files():
    with GenomicJobController(GenomicJob.AW3_MISSING_DATA_FILE_REPORT) as controller:
        controller.check_aw3_ready_missing_files()


