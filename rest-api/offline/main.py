"""The main API definition file for endpoints that trigger MapReduces and batch tasks."""

import api_util
import app_util
import config
import json
import logging

from dao.metrics_dao import MetricsVersionDao
from flask import Flask
from google.appengine.api import app_identity
from offline import biobank_samples_pipeline
from offline.metrics_export import MetricsExport

PREFIX = '/offline/'


@api_util.auth_required_cron
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


@api_util.auth_required_cron
def import_biobank_samples():
  # Note that crons have a 10 minute deadline instead of the normal 60s.
  logging.info('Starting samples import.')
  timestamp = None
  try:
    written, timestamp = biobank_samples_pipeline.upsert_from_latest_csv()
    logging.info(
        'Import complete (%d written), generating report.', written)
  except biobank_samples_pipeline.DataError:
    written = None
    logging.error('Import failed: bad data in CSV.', exc_info=True)

  logging.info('Generating reconciliation report.')
  biobank_samples_pipeline.write_reconciliation_report(timestamp)
  logging.info('Generated reconciliation report.')
  return json.dumps({'written': written})


def _build_pipeline_app():
  """Configure and return the app with non-resource pipeline-triggering endpoints."""
  offline_app = Flask(__name__)

  offline_app.add_url_rule(
      PREFIX + 'BiobankSamplesImport',
      endpoint='biobankSamplesImport',
      view_func=import_biobank_samples,
      methods=['GET'])

  offline_app.add_url_rule(
      PREFIX + 'MetricsRecalculate',
      endpoint='metrics_recalc',
      view_func=recalculate_metrics,
      methods=['GET'])

  offline_app.after_request(app_util.add_headers)
  offline_app.before_request(app_util.request_logging)
  return offline_app


app = _build_pipeline_app()
