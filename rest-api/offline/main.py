"""The main API definition file for endpoints that trigger MapReduces and batch tasks."""

import json
import logging
import traceback

import app_util
import config
from api_util import EXPORTER
from dao.metric_set_dao import AggregateMetricsDao
from dao.metrics_dao import MetricsVersionDao
from flask import Flask, request
from google.appengine.api import app_identity
from offline import biobank_samples_pipeline
from offline.base_pipeline import send_failure_alert
from offline.exclude_ghost_participants import mark_ghost_participants
from offline.metrics_export import MetricsExport
from offline.participant_counts_over_time import calculate_participant_metrics
from offline.public_metrics_export import PublicMetricsExport, LIVE_METRIC_SET_ID
from offline.sa_key_remove import delete_service_account_keys
from offline.table_exporter import TableExporter
from sqlalchemy.exc import DBAPIError
from werkzeug.exceptions import BadRequest


PREFIX = '/offline/'


def _alert_on_exceptions(func):
  """Sends e-mail alerts for any failure of the decorated function.

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
      if not e.external or (e.external and biobank_recipients):
        send_failure_alert(
            func.__name__,
            'Data error in Biobank samples pipeline: %s' % e,
            log_exc_info=True,
            extra_recipients=biobank_recipients)
      else:
        # Don't alert for stale CSVs except in prod (where external recipients are configured).
        logging.info('Not alerting on external-only DataError (%s).', e)
      return json.dumps({'data_error': str(e)})
    except:
      send_failure_alert(func.__name__, 'Exception in cron: %s' % traceback.format_exc())
      raise
  return alert_on_exceptions_wrapper


@app_util.auth_required_cron
@_alert_on_exceptions
def recalculate_metrics():
  in_progress = MetricsVersionDao().get_version_in_progress()
  if in_progress:
    logging.info("=========== Metrics pipeline already running ============")
    return '{"metrics-pipeline-status": "running"}'
  else:
    bucket_name = app_identity.get_default_gcs_bucket_name()
    logging.info("=========== Starting metrics export ============")
    MetricsExport.start_export_tasks(bucket_name,
                                     int(config.getSetting(config.METRICS_SHARDS, 1)))
    return '{"metrics-pipeline-status": "started"}'


@app_util.auth_required_cron
def recalculate_public_metrics():
  logging.info('generating public metrics')
  aggs = PublicMetricsExport.export(LIVE_METRIC_SET_ID)
  client_aggs = AggregateMetricsDao.to_client_json(aggs)

  # summing all counts for one metric yields a total qualified participant count
  participant_count = 0
  if len(client_aggs) > 0:
    participant_count = sum([a['count'] for a in client_aggs[0]['values']])
  logging.info('persisted public metrics: {} aggregations over '
               '{} participants'.format(len(client_aggs), participant_count))

  # Same format returned by the metric sets API.
  return json.dumps({
      'metrics': client_aggs
  })


@app_util.auth_required_cron
@_alert_on_exceptions
def import_biobank_samples():
  # Note that crons always have a 10 minute deadline instead of the normal 60s; additionally our
  # offline service uses basic scaling with has no deadline.
  logging.info('Starting samples import.')
  written, timestamp = biobank_samples_pipeline.upsert_from_latest_csv()
  logging.info(
      'Import complete (%d written), generating report.', written)

  logging.info('Generating reconciliation report.')
  biobank_samples_pipeline.write_reconciliation_report(timestamp)
  logging.info('Generated reconciliation report.')
  return json.dumps({'written': written})

@app_util.auth_required_cron
@_alert_on_exceptions
def biobank_monthly_reconciliation_report():
  # make sure this cron job is executed after import_biobank_samples
  timestamp = biobank_samples_pipeline.get_last_biobank_sample_file_info()[2]

  logging.info('Generating monthly reconciliation report.')
  biobank_samples_pipeline.write_reconciliation_report(timestamp, 'monthly')
  logging.info('Generated monthly reconciliation report.')
  return json.dumps({'monthly-reconciliation-report': 'generated'})

@app_util.auth_required(EXPORTER)
def export_tables():
  resource = request.get_data()
  resource_json = json.loads(resource)
  database = resource_json.get('database')
  tables = resource_json.get('tables')
  instance_name = resource_json.get('instance_name')
  if not database:
    raise BadRequest("database is required")
  if not tables or type(tables) is not list:
    raise BadRequest("tables is required")
  directory = resource_json.get('directory')
  if not directory:
    raise BadRequest("directory is required")

  # Ensure this has a boolean value to avoid downstream issues.
  deidentify = resource_json.get('deidentify') is True

  return json.dumps(TableExporter.export_tables(database, tables, directory, deidentify,
                                                instance_name))

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

def _build_pipeline_app():
  """Configure and return the app with non-resource pipeline-triggering endpoints."""
  offline_app = Flask(__name__)

  offline_app.add_url_rule(
      PREFIX + 'BiobankSamplesImport',
      endpoint='biobankSamplesImport',
      view_func=import_biobank_samples,
      methods=['GET'])

  offline_app.add_url_rule(
    PREFIX + 'MonthlyReconciliationReport',
    endpoint='monthlyReconciliationReport',
    view_func=biobank_monthly_reconciliation_report,
    methods=['GET'])

  offline_app.add_url_rule(
      PREFIX + 'MetricsRecalculate',
      endpoint='metrics_recalc',
      view_func=recalculate_metrics,
      methods=['GET'])

  offline_app.add_url_rule(
      PREFIX + 'PublicMetricsRecalculate',
      endpoint='public_metrics_recalc',
      view_func=recalculate_public_metrics,
      methods=['GET'])

  offline_app.add_url_rule(
      PREFIX + 'ExportTables',
      endpoint='ExportTables',
      view_func=export_tables,
      methods=['POST'])

  offline_app.add_url_rule(
    PREFIX + 'DeleteOldKeys',
    endpoint='delete_old_keys',
    view_func=delete_old_keys,
    methods=['GET'])

  offline_app.add_url_rule(
    PREFIX + 'ParticipantCountsOverTime',
    endpoint='participant_counts_over_time',
    view_func=participant_counts_over_time,
    methods=['GET'])

  offline_app.add_url_rule(
    PREFIX + 'MarkGhostParticipants',
    endpoint='exclude_ghosts',
    view_func=exclude_ghosts,
    methods=['GET'])

  offline_app.after_request(app_util.add_headers)
  offline_app.before_request(app_util.request_logging)
  offline_app.register_error_handler(DBAPIError, app_util.handle_database_disconnect)

  return offline_app


app = _build_pipeline_app()
