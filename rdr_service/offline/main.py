"""The main API definition file for endpoints that trigger MapReduces and batch tasks."""
import json
import logging
import traceback
from datetime import datetime

from flask import Flask, got_request_exception, request
from sqlalchemy.exc import DBAPIError
from werkzeug.exceptions import BadRequest

from rdr_service import app_util, config
from rdr_service.api_util import EXPORTER
from rdr_service.dao.metric_set_dao import AggregateMetricsDao
from rdr_service.offline import biobank_samples_pipeline, genomic_pipeline, sync_consent_files, update_ehr_status
from rdr_service.offline.base_pipeline import send_failure_alert
from rdr_service.offline.bigquery_sync import rebuild_bigquery_handler, sync_bigquery_handler, \
    daily_rebuild_bigquery_handler
from rdr_service.offline.enrollment_check import check_enrollment
from rdr_service.offline.exclude_ghost_participants import mark_ghost_participants
from rdr_service.offline.participant_counts_over_time import calculate_participant_metrics
from rdr_service.offline.participant_maint import skew_duplicate_last_modified
from rdr_service.offline.patient_status_backfill import backfill_patient_status
from rdr_service.offline.public_metrics_export import LIVE_METRIC_SET_ID, PublicMetricsExport
from rdr_service.offline.sa_key_remove import delete_service_account_keys
from rdr_service.offline.table_exporter import TableExporter
from rdr_service.services.flask import OFFLINE_PREFIX, flask_start, flask_stop
from rdr_service.services.gcp_logging import begin_request_logging, end_request_logging, \
    flask_restful_log_exception_error


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


# @app_util.auth_required_cron
# @_alert_on_exceptions
# def recalculate_metrics():
#     # TODO: This should be refactored or removed.
#     in_progress = MetricsVersionDao().get_version_in_progress()
#     if in_progress:
#         logging.info("=========== Metrics pipeline already running ============")
#         return '{"metrics-pipeline-status": "running"}'
#     else:
#         bucket_name = app_identity.get_default_gcs_bucket_name()  # pylint: disable=undefined-variable
#         logging.info("=========== Starting metrics export ============")
#         MetricsExport.start_export_tasks(bucket_name, int(config.getSetting(config.METRICS_SHARDS, 1)))
#         return '{"metrics-pipeline-status": "started"}'


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
@_alert_on_exceptions
def import_biobank_samples():
    # Note that crons always have a 10 minute deadline instead of the normal 60s; additionally our
    # offline service uses basic scaling with has no deadline.
    logging.info("Starting samples import.")
    written, _ = biobank_samples_pipeline.upsert_from_latest_csv()
    logging.info("Import complete %(written)d, generating report.", written)
    return json.dumps({"written": written})


@app_util.auth_required_cron
@_alert_on_exceptions
def biobank_daily_reconciliation_report():
    # TODO: setup to only run after import_biobank_samples completion instead of 1hr after start.
    timestamp = biobank_samples_pipeline.get_last_biobank_sample_file_info()[2]
    logging.info("Generating reconciliation report.")
    # iterate new list and write reports
    biobank_samples_pipeline.write_reconciliation_report(timestamp)
    logging.info("Generated reconciliation report.")
    return '{"success": "true"}'

@app_util.auth_required_cron
@_alert_on_exceptions
def biobank_monthly_reconciliation_report():
    # make sure this cron job is executed after import_biobank_samples
    timestamp = biobank_samples_pipeline.get_last_biobank_sample_file_info(monthly=True)[2]

    logging.info("Generating monthly reconciliation report.")
    # iterate new list and write reports
    biobank_samples_pipeline.write_reconciliation_report(timestamp, "monthly")
    logging.info("Generated monthly reconciliation report.")
    return json.dumps({"monthly-reconciliation-report": "generated"})


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
    delete_service_account_keys()
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


@app_util.auth_required_cron
@_alert_on_exceptions
def run_sync_consent_files():
    sync_consent_files.do_sync_recent_consent_files()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def run_va_sync_consent_files():
    sync_consent_files.do_sync_recent_consent_files(all_va=True, zip_files=True)
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def update_ehr_status_cron():
    update_ehr_status.update_ehr_status()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_new_participant_workflow():
    genomic_pipeline.new_participant_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_gc_manifest_workflow():
    genomic_pipeline.genomic_centers_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_data_manifest_workflow():
    genomic_pipeline.ingest_genomic_centers_metrics_files()
    genomic_pipeline.reconcile_metrics_vs_manifest()
    genomic_pipeline.reconcile_metrics_vs_genotyping_data()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_gem_a1_a2_workflow():
    genomic_pipeline.gem_a1_manifest_workflow()
    genomic_pipeline.gem_a2_manifest_workflow()
    return '{"success": "true"}'


@app_util.auth_required_cron
@_alert_on_exceptions
def genomic_gem_a3_workflow():
    genomic_pipeline.gem_a3_manifest_workflow()
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


def _build_pipeline_app():
    """Configure and return the app with non-resource pipeline-triggering endpoints."""
    offline_app = Flask(__name__)

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "EnrollmentStatusCheck",
        endpoint="enrollmentStatusCheck",
        view_func=check_enrollment_status,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "BiobankSamplesImport",
        endpoint="biobankSamplesImport",
        view_func=import_biobank_samples,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "DailyReconciliationReport",
        endpoint="dailyReconciliationReport",
        view_func=biobank_daily_reconciliation_report,
        methods=["GET"],
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "MonthlyReconciliationReport",
        endpoint="monthlyReconciliationReport",
        view_func=biobank_monthly_reconciliation_report,
        methods=["GET"],
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "SkewDuplicates", endpoint="skew_duplicates", view_func=skew_duplicates, methods=["GET"]
    )

    # offline_app.add_url_rule(
    #     PREFIX + "MetricsRecalculate", endpoint="metrics_recalc", view_func=recalculate_metrics, methods=["GET"]
    # )

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
        OFFLINE_PREFIX + "SyncConsentFiles", endpoint="sync_consent_files", view_func=run_sync_consent_files,
        methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "SyncVaConsentFiles",
        endpoint="sync_va_consent_files",
        view_func=run_va_sync_consent_files,
        methods=["GET"]
    )

    offline_app.add_url_rule(
        OFFLINE_PREFIX + "UpdateEhrStatus", endpoint="update_ehr_status", view_func=update_ehr_status_cron,
        methods=["GET"]
    )

    # BEGIN Genomic Pipeline Jobs
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicNewParticipantWorkflow",
        endpoint="genomic_new_participant_workflow",
        view_func=genomic_new_participant_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGCManifestWorkflow",
        endpoint="genomic_gc_manifest_workflow",
        view_func=genomic_gc_manifest_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicDataManifestWorkflow",
        endpoint="genomic_data_manifest_workflow",
        view_func=genomic_data_manifest_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGemA1A2Workflow",
        endpoint="genomic_gem_a1_a2_workflow",
        view_func=genomic_gem_a1_a2_workflow, methods=["GET"]
    )
    offline_app.add_url_rule(
        OFFLINE_PREFIX + "GenomicGemA3Workflow",
        endpoint="genomic_gem_a3_workflow",
        view_func=genomic_gem_a3_workflow, methods=["GET"]
    )
    # END Genomic Pipeline Jobs

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
