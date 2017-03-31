"""The main API definition file for endpoints that trigger MapReduces and batch tasks."""

import api_util
import app_util
import config
import json
import logging

from dao.metrics_dao import MetricsVersionDao
from flask import Flask
from flask_restful import Api
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
  written, skipped = biobank_samples_pipeline.upsert_from_latest_csv()
  return json.dumps({'written': written, 'skipped': skipped})


app = Flask(__name__)
#
# The REST-ful resources that are the bulk of the API.
#

api = Api(app)

#
# Non-resource pipeline-trigger endpoints
#

app.add_url_rule(PREFIX + 'BiobankSamplesImport',
                 endpoint='biobankSamplesImport',
                 view_func=import_biobank_samples,
                 methods=['GET'])

app.add_url_rule(PREFIX + 'MetricsRecalculate',
                 endpoint='metrics_recalc',
                 view_func=recalculate_metrics,
                 methods=['GET'])

app.after_request(app_util.add_headers)
app.before_request(app_util.request_logging)
