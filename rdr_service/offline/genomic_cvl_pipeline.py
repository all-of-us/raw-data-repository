import logging

from rdr_service import config
from rdr_service.genomic.genomic_cvl_reconciliation import GenomicCVLReconcile
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessResult, GenomicManifestTypes
from rdr_service.offline.genomic_pipeline import load_awn_manifest_into_raw_table


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
                genome_type=config.GENOME_TYPE_WGS
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
                genome_type=config.GENOME_TYPE_WGS
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
                genome_type=config.GENOME_TYPE_WGS,
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


def reconcile_message_broker_results_ready():
    with GenomicJobController(GenomicJob.RECONCILE_MESSAGE_BROKER_CVL_RESULTS_READY) as controller:
        controller.reconcile_message_broker_results_ready()


def reconcile_message_broker_results_viewed():
    with GenomicJobController(GenomicJob.RECONCILE_MESSAGE_BROKER_CVL_RESULTS_VIEWED) as controller:
        controller.reconcile_message_broker_results_viewed()


def calculate_informing_loop_ready_flags():
    with GenomicJobController(GenomicJob.CALCULATE_INFORMING_LOOP_READY) as controller:
        controller.calculate_informing_loop_ready_flags()


def reconcile_cvl_results(reconcile_job_type):
    with GenomicJobController(reconcile_job_type) as controller:
        cvl_reconciler = GenomicCVLReconcile(
            reconcile_type=reconcile_job_type
        )
        cvl_reconciler.run_reconcile()
        controller.job_result = GenomicSubProcessResult.SUCCESS


def reconcile_appointment_events_from_metrics():
    with GenomicJobController(GenomicJob.APPOINTMENT_METRICS_RECONCILE) as controller:
        controller.reconcile_appointment_events_from_metrics()


def notify_email_group_of_w1il_gror_resubmit_participants(since_datetime):
    with GenomicJobController(GenomicJob.CHECK_FOR_W1IL_GROR_RESUBMIT) as controller:
        controller.check_w1il_gror_resubmit(since_datetime=since_datetime)


def notify_appointment_gror_changed():
    with GenomicJobController(GenomicJob.CHECK_APPOINTMENT_GROR_CHANGED) as controller:
        controller.check_appointments_gror_changed()


def check_gcr_appointment_escalation():
    with GenomicJobController(GenomicJob.CHECK_GCR_OUTREACH_ESCALATION) as controller:
        controller.check_gcr_14day_escalation()
