"""The main API definition file for endpoints that trigger MapReduces and batch tasks."""

from rdr_service.genomic_enums import GenomicJob

import json
import logging
import traceback
from datetime import datetime, timedelta

from flask import Flask, got_request_exception, request
from sqlalchemy.exc import DBAPIError
from werkzeug.exceptions import BadRequest

from rdr_service import app_util, config
from rdr_service.api_util import EXPORTER, RDR
from rdr_service.app_util import nonprod
from rdr_service.clock import CLOCK
from rdr_service.dao.base_dao import BaseDao
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao import database_factory
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.metric_set_dao import AggregateMetricsDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.requests_log import RequestsLog
from rdr_service.offline import biobank_samples_pipeline, genomic_pipeline, sync_consent_files, update_ehr_status, \
    antibody_study_pipeline, genomic_data_quality_pipeline, export_va_workqueue
from rdr_service.offline.ce_health_data_reconciliation_pipeline import CeHealthDataReconciliationPipeline
from rdr_service.offline.base_pipeline import send_failure_alert
from rdr_service.offline.bigquery_sync import sync_bigquery_handler, \
    daily_rebuild_bigquery_handler, rebuild_bigquery_handler
from rdr_service.offline.import_deceased_reports import DeceasedReportImporter
from rdr_service.offline.import_hpo_lite_pairing import HpoLitePairingImporter
from rdr_service.offline.enrollment_check import check_enrollment
from rdr_service.offline.genomic_pipeline import run_genomic_cron_job, interval_run_schedule
from rdr_service.offline.participant_counts_over_time import calculate_participant_metrics
from rdr_service.offline.retention_eligible_import import calculate_retention_eligible_metrics
from rdr_service.offline.participant_maint import skew_duplicate_last_modified
from rdr_service.offline.patient_status_backfill import backfill_patient_status
from rdr_service.offline.public_metrics_export import LIVE_METRIC_SET_ID, PublicMetricsExport
from rdr_service.offline.requests_log_migrator import RequestsLogMigrator
from rdr_service.offline.response_validation import ResponseValidationController
from rdr_service.offline.service_accounts import ServiceAccountKeyManager
from rdr_service.offline.sync_consent_files import ConsentSyncController
from rdr_service.offline.table_exporter import TableExporter
from rdr_service.repository.obfuscation_repository import ObfuscationRepository
from rdr_service.services.consent.validation import ConsentValidationController, ReplacementStoringStrategy,\
    StoreResultStrategy
from rdr_service.services.data_quality import DataQualityChecker
from rdr_service.services.hpro_consent import HealthProConsentFile
from rdr_service.services.flask import OFFLINE_PREFIX, flask_start, flask_stop
from rdr_service.services.gcp_logging import begin_request_logging, end_request_logging,\
    flask_restful_log_exception_error
from rdr_service.services.ghost_check_service import GhostCheckService
from rdr_service.services.response_duplication_detector import ResponseDuplicationDetector
from rdr_service.storage import GoogleCloudStorageProvider


def _alert_on_exceptions(func):
    """
    Sends e-mail alerts for any failure of the decorated function.
    This handles Biobank DataErrors specially.
    This must be the innermost (bottom) decorator in order to discover the wrapped function's name.
    """
    def alert_on_exceptions_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except biobank_samples_pipeline.DataError as e:
            # This is for CSVs older than 24h; we only want to send alerts in prod, where we expect
            # regular CSV uploads. In other environments, it's OK to just abort the CSV import if there's
            # no new data.
            biobank_recipients = config.getSettingList(config.BIOBANK_STATUS_MAIL_RECIPIENTS, default=[])

            # send_failure_alert() is deprecated as of migration to App Engine 2, making this if/else
            # a no-op.  Leaving code in place in case email alerting is re-enabled later.
            if not e.external or (e.external and biobank_recipients):
                send_failure_alert(
                    func.__name__,
                    "Data error in Biobank samples pipeline: %s" % e,
                    log_exc_info=True,
                    extra_recipients=biobank_recipients,
                )
            else:
                pass
                # Don't alert for stale CSVs except in prod (where external recipients are configured).
                # logging.info(f'Not alerting on external-only DataError')

            # DA-1591: Since email alerting is deprecated, always log DataErrors and trigger GAE default
            # exception handling/500 response so cron job is marked "failed" on the GCP (cron jobs) console
            logging.error(f"Data error in Biobank samples pipeline: {e}")
            raise
        except:
            send_failure_alert(func.__name__, "Exception in cron: %s" % traceback.format_exc())
            raise

    return alert_on_exceptions_wrapper


@app_util.auth_required_cron
def recalculate_public_metrics():
    logging.info("generating public metrics")
    aggs = PublicMetricsExport.export(LIVE_METRIC_SET_ID)
    client_aggs = AggregateMetricsDao.to_client_json(aggs)

    # summing all counts for one metric yields a total qualified participant count
    participant_count = 0
    if len(client_aggs) > 0:
        participant_count = sum([a["count"] for a in client_aggs[0]["values"]])
    logging.info(
        "persisted public metrics: {} aggregations over " "{} participants".format(len(client_aggs), participant_count)
    )

    # Same format returned by the metric sets API.
    return json.dumps({"metrics": client_aggs})


@app_util.auth_required_cron
def run_ce_health_data_reconciliation():
    ce_health_data_reconciliation_pipeline = CeHealthDataReconciliationPipeline()
    logging.info("Starting read ce manifest files.")
    ce_health_data_reconciliation_pipeline.process_ce_manifest_files()
    logging.info("Read complete, generating missing report.")
    ce_health_data_reconciliation_pipeline.generate_missing_report()
    logging.info("Generated missing report.")
    return '{"success": "true"}'


@app_util.auth_required_cron
def run_biobank_samples_pipeline():
    # Note that crons always have a 10 minute deadline instead of the normal 60s; additionally our
    # offline service uses basic scaling with has no deadline.
    logging.info("Starting samples import.")
    written, timestamp = biobank_samples_pipeline.upsert_from_latest_csv()
    logging.info("Import complete %(written)d, generating report.", written)

    # iterate new list and write reports
    biobank_samples_pipeline.write_reconciliation_report(timestamp)
    logging.info("Generated reconciliation report.")
    return '{"success": "true"}'


@app_util.auth_required_cron
def biobank_monthly_reconciliation_report():
    # make sure this cron job is executed after import_biobank_samples
    sample_file_path, sample_file, timestamp = biobank_samples_pipeline.get_last_biobank_sample_file_info(monthly=True)
    logging.info(f"Generating reconciliation report from {sample_file_path}, {sample_file}")
    # iterate new list and write reports
    biobank_samples_pipeline.write_reconciliation_report(timestamp, "monthly")
    logging.info("Generated monthly reconciliation report.")
    return json.dumps({"monthly-reconciliation-report": "generated"})


@app_util.auth_required_cron
@_alert_on_exceptions
def import_covid_antibody_study_data():
    logging.info("Starting biobank covid antibody study manifest file import.")
    antibody_study_pipeline.import_biobank_covid_manifest_files()
    logging.info("Import biobank covid antibody study manifest files complete.")

    logging.info("Starting quest covid antibody study files import.")
    antibody_study_pipeline.import_quest_antibody_files()
    logging.info("Import quest covid antibody study files complete.")

    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def sync_covid_antibody_study_compliant_reports():
    logging.info("Starting CLIA compliant reports sync.")
    antibody_study_pipeline.sync_clia_compliance_pdf_files()
    logging.info("CLIA compliant reports sync complete.")

    return '{"success": "true"}'


@app_util.auth_required(EXPORTER)
def export_tables():
    resource = request.get_data()
    resource_json = json.loads(resource)
    database = resource_json.get("database")
    tables = resource_json.get("tables")
    instance_name = resource_json.get("instance_name")
    if not database:
        raise BadRequest("database is required")
    if not tables or not isinstance(tables, list):
        raise BadRequest("tables is required")
    directory = resource_json.get("directory")
    if not directory:
        raise BadRequest("directory is required")

    # Ensure this has a boolean value to avoid downstream issues.
    deidentify = resource_json.get("deidentify") is True

    return json.dumps(TableExporter.export_tables(database, tables, directory, deidentify, instance_name))


@app_util.auth_required_cron
@_alert_on_exceptions
def skew_duplicates():
    skew_duplicate_last_modified()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def delete_old_keys():
    manager = ServiceAccountKeyManager()
    manager.expire_old_keys()
    return '{"success": "true"}'


@app_util.auth_required_cron
def delete_expired_obfuscations():
    with ParticipantSummaryDao().session() as session:
        ObfuscationRepository.delete_expired_data(session=session)
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def participant_counts_over_time():
    calculate_participant_metrics()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def update_retention_eligible_metrics():
    # This for for lower env only
    calculate_retention_eligible_metrics()
    return '{"success": "true"}'


@app_util.auth_required_cron
def find_ghosts():
    credentials_config = config.getSettingJson('ptsc_api_config', default=None)
    if not credentials_config:
        logging.error('No API credentials found')
        return '{"success": "false"}'

    start_date = CLOCK.now() - timedelta(weeks=5)
    with database_factory.get_database().session() as session:
        service = GhostCheckService(
            session=session,
            logger=logging.getLogger(),
            ptsc_config=credentials_config
        )
        service.run_ghost_check(start_date=start_date)
    return '{"success": "true"}'


def _build_validation_controller(session, consent_dao):
    return ConsentValidationController(
        consent_dao=consent_dao,
        participant_summary_dao=ParticipantSummaryDao(),
        hpo_dao=HPODao(),
        storage_provider=GoogleCloudStorageProvider(),
        session=session
    )


@app_util.auth_required_cron
def check_for_consent_corrections():
    validation_controller = _build_validation_controller()
    with validation_controller.consent_dao.session() as session:
        validation_controller.check_for_corrections(session)
    return '{"success": "true"}'


@app_util.auth_required_cron
def validate_consent_files():
    consent_dao = ConsentDao()
    with consent_dao.session() as session, StoreResultStrategy(
        session=session,
        consent_dao=consent_dao
    ) as store_strategy:
        validation_controller = _build_validation_controller(
            session=session,
            consent_dao=consent_dao
        )
        validation_controller.validate_consent_uploads(store_strategy)
    return '{"success": "true"}'


@app_util.auth_required_cron
def run_sync_consent_files():
    controller = ConsentSyncController(
        consent_dao=ConsentDao(),
        participant_dao=ParticipantDao(),
        storage_provider=GoogleCloudStorageProvider()
    )
    controller.sync_ready_files()
    return '{"success": "true"}'


@app_util.auth_required(RDR)
def manually_trigger_validation():
    consent_dao = ConsentDao()
    with consent_dao.session() as session, ReplacementStoringStrategy(
        session=session,
        consent_dao=consent_dao
    ) as output_strategy:
        controller = ConsentValidationController(
            consent_dao=consent_dao,
            participant_summary_dao=ParticipantSummaryDao(),
            hpo_dao=HPODao(),
            storage_provider=GoogleCloudStorageProvider(),
            session=session
        )
        for participant_id in request.json.get('ids'):
            controller.validate_all_for_participant(participant_id=participant_id, output_strategy=output_strategy)
    return '{"success": "true"}'


@app_util.auth_required(RDR)
def manually_trigger_consent_sync():
    request_json = request.json

    # do_sync_consent_files will filter by any kwargs passed to it, even if they're None.
    # So if something like start_date is passed in as None, it will try to filter by comparing to a start_date of none.
    parameters = {}
    for field_name in ['all_va', 'start_date', 'end_date', 'ids']:
        if field_name in request_json:
            parameters[field_name] = request_json.get(field_name)

    sync_consent_files.do_sync_consent_files(zip_files=request.json.get('zip_files'), **parameters)
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def update_ehr_status_organization():
    update_ehr_status.update_ehr_status_organization()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def update_ehr_status_participant():
    update_ehr_status.update_ehr_status_participant()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def bigquery_rebuild_cron():
    """ this should always be a manually run job, but we have to schedule it at least once a year. """
    now = datetime.utcnow()
    if now.day == 0o1 and now.month == 0o1:
        logging.info("skipping the scheduled run.")
        return '{"success": "true"}'
    rebuild_bigquery_handler()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def bigquery_daily_rebuild_cron():
    daily_rebuild_bigquery_handler()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def bigquery_sync():
    sync_bigquery_handler()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def patient_status_backfill():
    # this should always be a manually run job, but we have to schedule it.
    now = datetime.utcnow()
    if now.day == 0o1 and now.month == 0o1:
        logging.info("skipping the scheduled run.")
        return '{"success": "true"}'
    backfill_patient_status()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def check_enrollment_status():
    check_enrollment()
    return '{ "success": "true" }'


@app_util.auth_required_cron
def flag_response_duplication():
    detector = ResponseDuplicationDetector()
    detector.flag_duplicate_responses()
    return '{ "success": "true" }'


@app_util.auth_required_cron
def validate_responses():
    a_day_ago = CLOCK.now() - timedelta(days=1)
    since_date = datetime(year=a_day_ago.year, month=a_day_ago.month, day=a_day_ago.day)

    slack_webhooks = config.getSettingJson(config.RDR_SLACK_WEBHOOKS)

    dao = BaseDao(None)
    with dao.session() as session:
        controller = ResponseValidationController(
            session=session,
            since_date=since_date,
            slack_webhook=slack_webhooks[config.RDR_VALIDATION_WEBHOOK]
        )
        controller.run_validation()

    return '{ "success": "true" }'


@app_util.auth_required_cron
@_alert_on_exceptions
def import_deceased_reports():
    importer = DeceasedReportImporter(config.get_config())
    importer.import_reports()
    return '{ "success": "true" }'


@app_util.auth_required_cron
@_alert_on_exceptions
def import_hpo_lite_pairing():
    importer = HpoLitePairingImporter()
    importer.import_pairing_data()
    return '{ "success": "true" }'


@app_util.auth_required_cron
@_alert_on_exceptions
def clean_up_request_logs():
    dao = BaseDao(None)
    with dao.session() as session:
        six_months_ago = datetime.utcnow() - timedelta(days=180)
        session.query(RequestsLog).filter(
            RequestsLog.created < six_months_ago
        ).delete(synchronize_session=False)
    return '{ "success": "true" }'


@app_util.auth_required_cron
def check_data_quality():
    # The cron job is scheduled for every week, so check data since the last run (with a little overlap)
    eight_days_ago = datetime.utcnow() - timedelta(days=8)

    dao = BaseDao(None)
    with dao.session() as session:
        checker = DataQualityChecker(session)
        checker.run_data_quality_checks(for_data_since=eight_days_ago)

    return '{ "success": "true" }'


@app_util.auth_required_cron
@nonprod
def migrate_requests_logs(target_db):
    migrator = RequestsLogMigrator(target_instance_name=target_db)
    migrator.migrate_latest_requests_logs()
    return '{ "success": "true" }'


@app_util.auth_required_cron
def transfer_hpro_consents():
    hpro_consents = HealthProConsentFile()
    hpro_consents.transfer_limit = config.getSetting(config.HEALTHPRO_CONSENTS_TRANSFER_LIMIT, default=1000)
    hpro_consents.initialize_consent_transfer()
    return '{ "success": "true" }'


@app_util.auth_required_cron
@run_genomic_cron_job('aw0_manifest_workflow')
def genomic_new_participant_workflow():
    genomic_pipeline.new_participant_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
def genomic_c2_participant_workflow():
    genomic_pipeline.c2_participant_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
def genomic_c1_participant_workflow():
    genomic_pipeline.c1_participant_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
def genomic_gc_manifest_workflow():
    genomic_pipeline.genomic_centers_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
def genomic_data_manifest_workflow():
    genomic_pipeline.ingest_genomic_centers_metrics_files()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('aw2f_manifest_workflow')
def genomic_scan_feedback_records():
    genomic_pipeline.scan_and_complete_feedback_records()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('aw2f_remainder_workflow')
def genomic_aw2f_remainder_workflow():
    genomic_pipeline.send_remainder_contamination_manifests()
    return '{"success": "true"}'

@app_util.auth_required_cron
@run_genomic_cron_job('a1_manifest_workflow')
def genomic_gem_a1_workflow():
    genomic_pipeline.gem_a1_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('a2_manifest_workflow')
def genomic_gem_a2_workflow():
    genomic_pipeline.gem_a2_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('a3_manifest_workflow')
def genomic_gem_a3_workflow():
    genomic_pipeline.gem_a3_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('update_report_state_for_consent_removal')
def update_report_state_for_consent_removal():
    genomic_pipeline.update_report_state_for_consent_removal()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('aw3_array_manifest_workflow')
def genomic_aw3_array_workflow():
    genomic_pipeline.aw3_array_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('aw3_wgs_manifest_workflow')
def genomic_aw3_wgs_workflow():
    genomic_pipeline.aw3_wgs_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('cvl_w1il_pgx_manifest_workflow')
def genomic_cvl_w1il_pgx_workflow():
    genomic_pipeline.cvl_w1il_manifest_workflow(
        cvl_site_bucket_map=config.getSettingJson(config.GENOMIC_CVL_SITE_BUCKET_MAP),
        module_type='pgx'
    )
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('cvl_w1il_hdr_manifest_workflow')
def genomic_cvl_w1il_hdr_workflow():
    genomic_pipeline.cvl_w1il_manifest_workflow(
        cvl_site_bucket_map=config.getSettingJson(config.GENOMIC_CVL_SITE_BUCKET_MAP),
        module_type='hdr'
    )
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('cvl_w2w_manifest_workflow')
@interval_run_schedule(GenomicJob.CVL_W2W_WORKFLOW, 'skip_week')
def genomic_cvl_w2w_workflow():
    genomic_pipeline.cvl_w2w_manifest_workflow(
        cvl_site_bucket_map=config.getSettingJson(config.GENOMIC_CVL_SITE_BUCKET_MAP)
    )
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('cvl_w3sr_manifest_workflow')
@interval_run_schedule(GenomicJob.CVL_W3SR_WORKFLOW, 'skip_week')
def genomic_cvl_w3sr_workflow():
    genomic_pipeline.cvl_w3sr_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('genomic_aw3_array_investigation_workflow')
def genomic_aw3_array_investigation_workflow():
    genomic_pipeline.aw3_array_investigation_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('genomic_aw3_wgs_investigation_workflow')
def genomic_aw3_wgs_investigation_workflow():
    genomic_pipeline.aw3_wgs_investigation_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('feedback_record_reconciliation_workflow')
def genomic_feedback_record_reconciliation():
    genomic_pipeline.feedback_record_reconciliation()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('missing_files_clean_up_workflow')
def genomic_missing_files_clean_up():
    genomic_pipeline.genomic_missing_files_clean_up()
    return '{"success": "true"}'


@app_util.auth_required_cron
def check_for_w1il_gror_resubmit_participants():
    a_week_ago = datetime.utcnow() - timedelta(weeks=1)
    genomic_pipeline.notify_email_group_of_w1il_gror_resubmit_participants(since_datetime=a_week_ago)
    return '{"success": "true"}'

# Disabling job until further notice
# @app_util.auth_required_cron
# @run_genomic_cron_job('reconcile_raw_to_aw1_ingested_workflow')
# def reconcile_raw_to_aw1_ingested():
#     genomic_pipeline.reconcile_raw_to_aw1_ingested()
#     return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('reconcile_raw_to_aw2_ingested_workflow')
def reconcile_raw_to_aw2_ingested():
    genomic_pipeline.reconcile_raw_to_aw2_ingested()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('members_state_resolved_workflow')
def genomic_members_state_resolved():
    genomic_pipeline.update_members_state_resolved_data_files()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('members_update_blocklists')
def genomic_members_update_blocklists():
    genomic_pipeline.update_members_blocklists()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('reconcile_informing_loop_responses')
def genomic_reconcile_informing_loop_responses():
    genomic_pipeline.reconcile_informing_loop_responses()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('reconcile_message_broker_results_ready')
def genomic_reconcile_message_broker_results_ready():
    genomic_pipeline.reconcile_message_broker_results_ready()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('reconcile_message_broker_results_viewed')
def genomic_reconcile_message_broker_results_viewed():
    genomic_pipeline.reconcile_message_broker_results_viewed()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('retry_manifest_ingestion_failures')
def genomic_retry_manifest_ingestion_failures():
    genomic_pipeline.retry_manifest_ingestions()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('calculate_informing_loops_ready_weekly')
def genomic_calculate_informing_loop_ready_flags_weekly():
    genomic_pipeline.calculate_informing_loop_ready_flags()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('calculate_informing_loops_ready_daily')
def genomic_calculate_informing_loop_ready_flags_daily():
    genomic_pipeline.calculate_informing_loop_ready_flags()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('reconcile_pdr_data')
def genomic_reconcile_pdr_data():
    genomic_pipeline.reconcile_pdr_data()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('reconcile_cvl_pgx_results')
def genomic_reconcile_cvl_pgx_results():
    genomic_pipeline.reconcile_cvl_results(
        reconcile_job_type=GenomicJob.RECONCILE_CVL_PGX_RESULTS
    )
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('reconcile_cvl_hdr_results')
def genomic_reconcile_cvl_hdr_results():
    genomic_pipeline.reconcile_cvl_results(
        reconcile_job_type=GenomicJob.RECONCILE_CVL_HDR_RESULTS
    )
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('reconcile_cvl_alerts')
def genomic_reconcile_cvl_alerts():
    genomic_pipeline.reconcile_cvl_results(
        reconcile_job_type=GenomicJob.RECONCILE_CVL_ALERTS
    )
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('reconcile_cvl_resolved')
def genomic_reconcile_cvl_resolve():
    genomic_pipeline.reconcile_cvl_results(
        reconcile_job_type=GenomicJob.RECONCILE_CVL_RESOLVE
    )
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('results_pipeline_withdrawals')
def genomic_results_pipeline_withdrawals():
    genomic_pipeline.results_pipeline_withdrawals()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('gem_results_to_report_state')
def genomic_gem_result_reports():
    genomic_pipeline.gem_results_to_report_state()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('reconcile_appointment_events_from_metrics')
def genomic_reconcile_appointment_events():
    genomic_pipeline.reconcile_appointment_events_from_metrics()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('daily_ingestion_summary')
def genomic_data_quality_daily_ingestion_summary():
    genomic_data_quality_pipeline.data_quality_workflow(GenomicJob.DAILY_SUMMARY_REPORT_INGESTIONS)
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('daily_incident_summary')
def genomic_data_quality_daily_incident_summary():
    genomic_data_quality_pipeline.data_quality_workflow(GenomicJob.DAILY_SUMMARY_REPORT_INCIDENTS)
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('daily_validation_emails')
def genomic_data_quality_validation_emails():
    genomic_data_quality_pipeline.data_quality_workflow(GenomicJob.DAILY_SEND_VALIDATION_EMAILS)
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('daily_validation_fails_resolved')
def genomic_data_quality_validation_fails_resolved():
    genomic_data_quality_pipeline.data_quality_workflow(GenomicJob.DAILY_SUMMARY_VALIDATION_FAILS_RESOLVED)
    return '{"success": "true"}'


@app_util.auth_required_cron
def export_va_workqueue_report():
    export_va_workqueue.generate_workqueue_report()
    return '{"success": "true"}'


@app_util.auth_required_cron
def delete_old_va_workqueue_reports():
    export_va_workqueue.delete_old_reports()
    return '{"success": "true"}'


@app_util.auth_required_cron
@run_genomic_cron_job('aw3_ready_missing_files_report')
def genomic_aw3_ready_missing_files_report():
    genomic_pipeline.notify_aw3_ready_missing_data_files()
    return '{"success": "true"}'

@app_util.auth_required_cron
@run_genomic_cron_job('notify_appointment_gror_changed')
def genomic_appointment_gror_changed():
    genomic_pipeline.notify_appointment_gror_changed()
    return '{"success": "true"}'


def _build_pipeline_app():
    """Configure and return the app with non-resource pipeline-triggering endpoints."""
    offline_app = Flask(__name__)
    offline_app.config['TRAP_HTTP_EXCEPTIONS'] = True

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "EnrollmentStatusCheck",
        endpoint="enrollmentStatusCheck",
        view_func=check_enrollment_status,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "FlagResponseDuplication",
        endpoint="flagResponseDuplication",
        view_func=flag_response_duplication,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "ResponseValidation",
        endpoint="responseValidation",
        view_func=validate_responses,
        methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "CeHealthDataReconciliation",
        endpoint="ceHealthDataReconciliation",
        view_func=run_ce_health_data_reconciliation,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "BiobankSamplesPipeline",
        endpoint="biobankSamplesPipeline",
        view_func=run_biobank_samples_pipeline,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "MonthlyReconciliationReport",
        endpoint="monthlyReconciliationReport",
        view_func=biobank_monthly_reconciliation_report,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "CovidAntibodyStudyImport",
        endpoint="covidAntibodyStudyImport",
        view_func=import_covid_antibody_study_data,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "CovidAntibodyStudyCompliantReportSync",
        endpoint="covidAntibodyStudyCompliantReportSync",
        view_func=sync_covid_antibody_study_compliant_reports,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "SkewDuplicates", endpoint="skew_duplicates", view_func=skew_duplicates, methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "PublicMetricsRecalculate",
        endpoint="public_metrics_recalc",
        view_func=recalculate_public_metrics,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "ExportTables", endpoint="ExportTables", view_func=export_tables, methods=["POST"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "DeleteOldKeys", endpoint="delete_old_keys", view_func=delete_old_keys, methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + 'DeleteExpiredObfuscation',
        endpoint='delete_expired_obfuscation',
        view_func=delete_expired_obfuscations,
        methods=['GET']
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "ParticipantCountsOverTime",
        endpoint="participant_counts_over_time",
        view_func=participant_counts_over_time,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "UpdateRetentionEligibleMetrics",
        endpoint="update_retention_eligible_metrics",
        view_func=update_retention_eligible_metrics,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "MarkGhostParticipants", endpoint="find_ghosts", view_func=find_ghosts, methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "ManuallyValidateFiles",
        endpoint="manually_validate_files",
        view_func=manually_trigger_validation,
        methods=["POST"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "ManuallySyncConsentFiles",
        endpoint="manually_sync_consent_files",
        view_func=manually_trigger_consent_sync,
        methods=["POST"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "ValidateConsentFiles", endpoint="validate_consent_files", view_func=validate_consent_files,
        methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "CorrectConsentFiles", endpoint="correct_consent_files",
        view_func=check_for_consent_corrections, methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "SyncConsentFiles", endpoint="sync_consent_files", view_func=run_sync_consent_files,
        methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "UpdateEhrStatusOrganization",
        endpoint="update_ehr_status_organization", view_func=update_ehr_status_organization,
        methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "UpdateEhrStatusParticipant",
        endpoint="update_ehr_status_participant", view_func=update_ehr_status_participant,
        methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "TransferHealthProConsents",
        endpoint="transfer_hpro_consents", view_func=transfer_hpro_consents,
        methods=["GET"]
    )

    # BEGIN Genomic Pipeline Jobs
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicC3AW0Workflow",
        endpoint="genomic_new_participant_workflow",
        view_func=genomic_new_participant_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicC2AW0Workflow",
        endpoint="genomic_c2_aw0_workflow",
        view_func=genomic_c2_participant_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicC1AW0Workflow",
        endpoint="genomic_c1_aw0_workflow",
        view_func=genomic_c1_participant_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGCManifestWorkflow",
        endpoint="genomic_gc_manifest_workflow",
        view_func=genomic_gc_manifest_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicDataManifestWorkflow",
        endpoint="genomic_data_manifest_workflow",
        view_func=genomic_data_manifest_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicAW2FManifestWorkflow",
        endpoint="genomic_scan_feedback_records",
        view_func=genomic_scan_feedback_records,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicAW2FRemainderWorkflow",
        endpoint="genomic_aw2f_remainder_workflow",
        view_func=genomic_aw2f_remainder_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGemA1Workflow",
        endpoint="genomic_gem_a1_workflow",
        view_func=genomic_gem_a1_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGemA2Workflow",
        endpoint="genomic_gem_a2_workflow",
        view_func=genomic_gem_a2_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGemA3Workflow",
        endpoint="genomic_gem_a3_workflow",
        view_func=genomic_gem_a3_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicUpdateReportStateForConsentRemoval",
        endpoint="update_report_state_for_consent_removal",
        view_func=update_report_state_for_consent_removal,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicAW3ArrayWorkflow",
        endpoint="genomic_aw3_array_workflow",
        view_func=genomic_aw3_array_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicAW3WGSWorkflow",
        endpoint="genomic_aw3_wgs_workflow",
        view_func=genomic_aw3_wgs_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicCVLW1ILPgxWorkflow",
        endpoint="genomic_cvl_w1il_pgx_workflow",
        view_func=genomic_cvl_w1il_pgx_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicCVLW1ILHdrWorkflow",
        endpoint="genomic_cvl_w1il_hdr_workflow",
        view_func=genomic_cvl_w1il_hdr_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicCVLW2WWorkflow",
        endpoint="genomic_cvl_w2w_workflow",
        view_func=genomic_cvl_w2w_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicCVLW3SRWorkflow",
        endpoint="genomic_cvl_w3sr_workflow",
        view_func=genomic_cvl_w3sr_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicAW3ArrayInvestigationWorkflow",
        endpoint="genomic_aw3_array_investigation_workflow",
        view_func=genomic_aw3_array_investigation_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicAW3WGSInvestigationWorkflow",
        endpoint="genomic_aw3_wgs_investigation_workflow",
        view_func=genomic_aw3_wgs_investigation_workflow,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicFeedbackRecordReconciliation",
        endpoint="genomic_feedback_record_reconciliation",
        view_func=genomic_feedback_record_reconciliation,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicMissingFilesCleanUp",
        endpoint="genomic_missing_files_clean_up",
        view_func=genomic_missing_files_clean_up,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "CheckForW1ilGrorResubmitParticipants",
        endpoint="check_for_w1il_gror_resubmit",
        view_func=check_for_w1il_gror_resubmit_participants,
        methods=["GET"]
    )
    # Disabling job until further notice
    # offline_app.add_url_rule(
    #     OFFLINE_PREFIX + "ReconcileRawToAw1Ingested",
    #     endpoint="reconcile_raw_to_aw1_ingested",
    #     view_func=reconcile_raw_to_aw1_ingested,
    #     methods=["GET"]
    # )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "ReconcileRawToAw2Ingested",
        endpoint="reconcile_raw_to_aw2_ingested",
        view_func=reconcile_raw_to_aw2_ingested,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicUpdateMembersStateResolved",
        endpoint="genomic_members_state_resolved",
        view_func=genomic_members_state_resolved,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicUpdateMembersBlocklists",
        endpoint="genomic_members_update_blocklists",
        view_func=genomic_members_update_blocklists,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicReconcileInformingLoopResponses",
        endpoint="reconcile_informing_loop_responses",
        view_func=genomic_reconcile_informing_loop_responses,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicReconcileMessageBrokerResultsReady",
        endpoint="genomic_reconcile_message_broker_results_ready",
        view_func=genomic_reconcile_message_broker_results_ready,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicReconcileMessageBrokerResultsViewed",
        endpoint="genomic_reconcile_message_broker_results_viewed",
        view_func=genomic_reconcile_message_broker_results_viewed,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicDataPdrReconcile",
        endpoint="genomic_data_pdr_reconcile",
        view_func=genomic_reconcile_pdr_data, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicRetryManifestIngestions",
        endpoint="retry_manifest_ingestion_failures",
        view_func=genomic_retry_manifest_ingestion_failures,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "CalculateInformingLoopReadyStatusWeekly",
        endpoint="informing_loop_ready_flags_weekly",
        view_func=genomic_calculate_informing_loop_ready_flags_weekly,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "CalculateInformingLoopReadyStatusDaily",
        endpoint="informing_loop_ready_flags_daily",
        view_func=genomic_calculate_informing_loop_ready_flags_daily,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicReconcilePGXResults",
        endpoint="genomic_reconcile_pgx_results",
        view_func=genomic_reconcile_cvl_pgx_results,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicReconcileHDRResults",
        endpoint="genomic_reconcile_hdr_results",
        view_func=genomic_reconcile_cvl_hdr_results,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicCVLReconciliationAlerts",
        endpoint="genomic_cvl_reconcile_alerts",
        view_func=genomic_reconcile_cvl_alerts,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicCVLResolveSamples",
        endpoint="genomic_cvl_resolve_samples",
        view_func=genomic_reconcile_cvl_resolve,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicResultsPipelineWithdrawals",
        endpoint="genomic_results_pipeline_withdrawals",
        view_func=genomic_results_pipeline_withdrawals,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGemResultReports",
        endpoint="genomic_gem_result_reports",
        view_func=genomic_gem_result_reports,
        methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicReconcileAppointmentEvents",
        endpoint="genomic_reconcile_appointment_events",
        view_func=genomic_reconcile_appointment_events,
        methods=["GET"]
    )
    # END Genomic Pipeline Jobs

    # BEGIN Genomic Data Quality Jobs
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicDataQualityDailyIngestionSummary",
        endpoint="genomic_data_quality_daily_ingestion_summary",
        view_func=genomic_data_quality_daily_ingestion_summary, methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicDataQualityDailyIncidentSummary",
        endpoint="genomic_data_quality_daily_incident_summary",
        view_func=genomic_data_quality_daily_incident_summary, methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicDataQualityDailyValidationEmails",
        endpoint="genomic_data_quality_validation_emails",
        view_func=genomic_data_quality_validation_emails, methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicDataQualityDailyValidationFailsResolved",
        endpoint="genomic_data_quality_validation_fails_resolved",
        view_func=genomic_data_quality_validation_fails_resolved, methods=["GET"]
    )
    # END Genomic Data Quality Jobs

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "BigQueryRebuild", endpoint="bigquery_rebuild", view_func=bigquery_rebuild_cron,
        methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "BigQueryDailyRebuild", endpoint="bigquery_daily_rebuild",
        view_func=bigquery_daily_rebuild_cron,
        methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "BigQuerySync", endpoint="bigquery_sync", view_func=bigquery_sync, methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "PatientStatusBackfill",
        endpoint="patient_status_backfill",
        view_func=patient_status_backfill,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "DeceasedReportImport",
        endpoint="deceased_report_import",
        view_func=import_deceased_reports,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "HpoLitePairingImport",
        endpoint="hpo_lite_pairing_import",
        view_func=import_hpo_lite_pairing,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "CleanUpRequestLogs",
        endpoint="request_log_cleanup",
        view_func=clean_up_request_logs,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "DataQualityChecks",
        endpoint="data_quality_checks",
        view_func=check_data_quality,
        methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + 'MigrateRequestsLog/<string:target_db>',
        endpoint='migrate_requests_log',
        view_func=migrate_requests_logs,
        methods=['GET']
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + 'DeleteOldVaWorkQueueReports',
        endpoint='delete_old_va_workqueue_reports',
        view_func=delete_old_va_workqueue_reports,
        methods=['GET']
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + 'ExportVaWorkQueue',
        endpoint='export_va_workqueue_report',
        view_func=export_va_workqueue_report,
        methods=['GET']
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + 'GenomicAW3ReadyMissingDataFilesReport',
        endpoint='genomic_aw3_ready_missing_files_report',
        view_func=genomic_aw3_ready_missing_files_report,
        methods=['GET']
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + 'GenomicNotifyAppointmentGRoRChanged',
        endpoint='genomic_appointment_gror_changed',
        view_func=genomic_appointment_gror_changed,
        methods=['GET']
    )

    offline_app.add_url_rule('/_ah/start', endpoint='start', view_func=flask_start, methods=["GET"])
    offline_app.add_url_rule("/_ah/stop", endpoint="stop", view_func=flask_stop, methods=["GET"])

    offline_app.before_request(begin_request_logging)  # Must be first before_request() call.
    offline_app.before_request(app_util.request_logging)

    offline_app.after_request(app_util.add_headers)
    offline_app.after_request(end_request_logging)  # Must be last after_request() call.

    offline_app.register_error_handler(DBAPIError, app_util.handle_database_disconnect)

    got_request_exception.connect(flask_restful_log_exception_error, offline_app)

    return offline_app


app = _build_pipeline_app()
