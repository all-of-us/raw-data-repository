"""The main API definition file for endpoints that trigger MapReduces and batch tasks."""
import rdr_service.activate_debugger  # pylint: disable=unused-import

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
from rdr_service.dao.base_dao import BaseDao
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.metric_set_dao import AggregateMetricsDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.requests_log import RequestsLog
from rdr_service.offline import biobank_samples_pipeline, genomic_pipeline, sync_consent_files, update_ehr_status, \
    antibody_study_pipeline, genomic_data_quality_pipeline
from rdr_service.offline.base_pipeline import send_failure_alert
from rdr_service.offline.bigquery_sync import sync_bigquery_handler, \
    daily_rebuild_bigquery_handler, rebuild_bigquery_handler
from rdr_service.offline.import_deceased_reports import DeceasedReportImporter
from rdr_service.offline.import_hpo_lite_pairing import HpoLitePairingImporter
from rdr_service.offline.enrollment_check import check_enrollment
from rdr_service.offline.exclude_ghost_participants import mark_ghost_participants
from rdr_service.offline.participant_counts_over_time import calculate_participant_metrics
from rdr_service.offline.participant_maint import skew_duplicate_last_modified
from rdr_service.offline.patient_status_backfill import backfill_patient_status
from rdr_service.offline.public_metrics_export import LIVE_METRIC_SET_ID, PublicMetricsExport
from rdr_service.offline.requests_log_migrator import RequestsLogMigrator
from rdr_service.offline.service_accounts import ServiceAccountKeyManager
from rdr_service.offline.sync_consent_files import ConsentSyncController
from rdr_service.offline.table_exporter import TableExporter
from rdr_service.services.consent.validation import ConsentValidationController, StoreResultStrategy
from rdr_service.services.data_quality import DataQualityChecker
from rdr_service.services.flask import OFFLINE_PREFIX, flask_start, flask_stop
from rdr_service.services.gcp_logging import begin_request_logging, end_request_logging,\
    flask_restful_log_exception_error
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
@_alert_on_exceptions
def participant_counts_over_time():
    calculate_participant_metrics()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def exclude_ghosts():
    mark_ghost_participants()
    return '{"success": "true"}'


def _build_validation_controller():
    return ConsentValidationController(
        consent_dao=ConsentDao(),
        participant_summary_dao=ParticipantSummaryDao(),
        hpo_dao=HPODao(),
        storage_provider=GoogleCloudStorageProvider()
    )


@app_util.auth_required_cron
def check_for_consent_corrections():
    validation_controller = _build_validation_controller()
    with validation_controller.consent_dao.session() as session:
        validation_controller.check_for_corrections(session)
    return '{"success": "true"}'


@app_util.auth_required_cron
def validate_consent_files():
    min_authored_timestamp = datetime.utcnow() - timedelta(hours=26)  # Overlap just to make sure we don't miss anything
    validation_controller = _build_validation_controller()
    with validation_controller.consent_dao.session() as session, StoreResultStrategy(
        session=session,
        consent_dao=validation_controller.consent_dao
    ) as store_strategy:
        validation_controller.validate_recent_uploads(session, store_strategy, min_consent_date=min_authored_timestamp)
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
def genomic_new_participant_workflow():
    genomic_pipeline.new_participant_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_c2_participant_workflow():
    genomic_pipeline.c2_participant_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_c1_participant_workflow():
    genomic_pipeline.c1_participant_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_gc_manifest_workflow():
    genomic_pipeline.genomic_centers_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_aw1f_failures_workflow():
    genomic_pipeline.genomic_centers_aw1f_manifest_workflow()
    genomic_pipeline.genomic_centers_accessioning_failures_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def aw1c_manifest_workflow():
    """Temporarily running this manually for E2E Testing"""
    now = datetime.utcnow()
    if now.day == 0o1 and now.month == 0o1:
        logging.info("skipping the scheduled run.")
        return '{"success": "true"}'
    genomic_pipeline.ingest_aw1c_manifest()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def aw1cf_failures_workflow():
    """Temporarily running this manually for E2E Testing"""
    now = datetime.utcnow()
    if now.day == 0o1 and now.month == 0o1:
        logging.info("skipping the scheduled run.")
        return '{"success": "true"}'
    genomic_pipeline.ingest_aw1cf_manifest_workflow()
    genomic_pipeline.aw1cf_alerts_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_data_manifest_workflow():
    genomic_pipeline.ingest_genomic_centers_metrics_files()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_array_data_reconciliation_workflow():
    genomic_pipeline.reconcile_metrics_vs_array_data()
    return '{"success": "true"}'

@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_wgs_data_reconciliation_workflow():
    genomic_pipeline.reconcile_metrics_vs_wgs_data()
    return '{"success": "true"}'

@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_scan_feedback_records():
    genomic_pipeline.scan_and_complete_feedback_records()
    return '{"success": "true"}'

@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_gem_a1_workflow():
    genomic_pipeline.gem_a1_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_gem_a2_workflow():
    genomic_pipeline.gem_a2_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_gem_a3_workflow():
    genomic_pipeline.gem_a3_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_cvl_w1_workflow():
    """Temporarily running this manually for E2E Testing"""
    now = datetime.utcnow()
    if now.day == 0o1 and now.month == 0o1:
        logging.info("skipping the scheduled run.")
        return '{"success": "true"}'
    genomic_pipeline.create_cvl_w1_manifest()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_cvl_w2_workflow():
    """Temporarily running this manually for E2E Testing"""
    now = datetime.utcnow()
    if now.day == 0o1 and now.month == 0o1:
        logging.info("skipping the scheduled run.")
        return '{"success": "true"}'
    genomic_pipeline.ingest_cvl_w2_manifest()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_cvl_w3_workflow():
    """Temporarily running this manually for E2E Testing"""
    now = datetime.utcnow()
    if now.day == 0o1 and now.month == 0o1:
        logging.info("skipping the scheduled run.")
        return '{"success": "true"}'
    genomic_pipeline.create_cvl_w3_manifest()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_aw3_array_workflow():
    genomic_pipeline.aw3_array_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_aw3_wgs_workflow():
    """Temporarily running this manually for E2E Testing"""
    now = datetime.utcnow()
    if now.day == 0o1 and now.month == 0o1:
        logging.info("skipping the scheduled run.")
        return '{"success": "true"}'
    genomic_pipeline.aw3_wgs_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_aw4_workflow():
    genomic_pipeline.aw4_array_manifest_workflow()
    genomic_pipeline.aw4_wgs_manifest_workflow()
    return '{"success": "true"}'

@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_feedback_record_reconciliation():
    genomic_pipeline.feedback_record_reconciliation()
    return '{"success": "true"}'

@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_missing_files_clean_up():
    genomic_pipeline.genomic_missing_files_clean_up()
    return '{"success": "true"}'

# @app_util.auth_required_cron
# @_alert_on_exceptions
# def genomic_feedback_record_reconciliation():
#     genomic_pipeline.feedback_record_reconciliation()
#     return '{"success": "true"}'

@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_data_quality_daily_ingestion_summary():
    genomic_data_quality_pipeline.data_quality_workflow(GenomicJob.DAILY_SUMMARY_REPORT_INGESTIONS)
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_data_quality_daily_incident_summary():
    genomic_data_quality_pipeline.data_quality_workflow(GenomicJob.DAILY_SUMMARY_REPORT_INCIDENTS)
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
        OFFLINE_PREFIX + "ParticipantCountsOverTime",
        endpoint="participant_counts_over_time",
        view_func=participant_counts_over_time,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "MarkGhostParticipants", endpoint="exclude_ghosts", view_func=exclude_ghosts, methods=["GET"]
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

    # BEGIN Genomic Pipeline Jobs
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicC3AW0Workflow",
        endpoint="genomic_new_participant_workflow",
        view_func=genomic_new_participant_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicC2AW0Workflow",
        endpoint="genomic_c2_aw0_workflow",
        view_func=genomic_c2_participant_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicC1AW0Workflow",
        endpoint="genomic_c1_aw0_workflow",
        view_func=genomic_c1_participant_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGCManifestWorkflow",
        endpoint="genomic_gc_manifest_workflow",
        view_func=genomic_gc_manifest_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicFailuresWorkflow",
        endpoint="genomic_aw1f_failures_workflow",
        view_func=genomic_aw1f_failures_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicAW1CManifestWorkflow",
        endpoint="aw1c_manifest_workflow",
        view_func=aw1c_manifest_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicCVLFailuresWorkflow",
        endpoint="aw1cf_failures_workflow",
        view_func=aw1cf_failures_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicDataManifestWorkflow",
        endpoint="genomic_data_manifest_workflow",
        view_func=genomic_data_manifest_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicArrayReconciliationWorkflow",
        endpoint="genomic_array_data_reconciliation_workflow",
        view_func=genomic_array_data_reconciliation_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicWGSReconciliationWorkflow",
        endpoint="genomic_wgs_data_reconciliation_workflow",
        view_func=genomic_wgs_data_reconciliation_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicAW2ManifestWorkflow",
        endpoint="genomic_scan_feedback_records",
        view_func=genomic_scan_feedback_records, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGemA1Workflow",
        endpoint="genomic_gem_a1_workflow",
        view_func=genomic_gem_a1_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGemA2Workflow",
        endpoint="genomic_gem_a2_workflow",
        view_func=genomic_gem_a2_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGemA3Workflow",
        endpoint="genomic_gem_a3_workflow",
        view_func=genomic_gem_a3_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicCvlW1Workflow",
        endpoint="genomic_cvl_w1_workflow",
        view_func=genomic_cvl_w1_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicCvlW2Workflow",
        endpoint="genomic_cvl_w2_workflow",
        view_func=genomic_cvl_w2_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicCvlW3Workflow",
        endpoint="genomic_cvl_w3_workflow",
        view_func=genomic_cvl_w3_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicAW3ArrayWorkflow",
        endpoint="genomic_aw3_array_workflow",
        view_func=genomic_aw3_array_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicAW3WGSWorkflow",
        endpoint="genomic_aw3_wgs_workflow",
        view_func=genomic_aw3_wgs_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicFeedbackRecordReconciliation",
        endpoint="genomic_feedback_record_reconciliation",
        view_func=genomic_feedback_record_reconciliation, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicMissingFilesCleanUp",
        endpoint="genomic_missing_files_clean_up",
        view_func=genomic_missing_files_clean_up, methods=["GET"]
    )
    # offline_app.add_url_rule(
    #     OFFLINE_PREFIX + "GenomicMissingFilesResolved",
    #     endpoint="genomic_missing_files_resolved",
    #     view_func=genomic_missing_files_resolved, methods=["GET"]
    # )

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
