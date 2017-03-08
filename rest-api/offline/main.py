"""The main API definition file for endpoints that trigger MapReduces.
"""
import api_util
import app_util
import config
import datetime
import metrics
import offline.biobank_samples_pipeline
import offline.metrics_pipeline

import logging

from flask import Flask
from flask_restful import Api
from google.appengine.api import app_identity

PREFIX = '/offline/'

@api_util.auth_required_cron
def recalculate_metrics():
  in_progress = metrics.get_in_progress_version()
  if in_progress:
    logging.info("=========== Metrics pipeline already running ============")
    return '{"metrics-pipeline-status": "running"}'
  else:
    bucket_name = app_identity.get_default_gcs_bucket_name()
    logging.info("=========== Starting metrics export ============")
    export = MetricsExport.start_export_tasks(bucket_name, datetime.datetime.utcnow())
    export.start()
    return '{"metrics-pipeline-status": "started"}'

@api_util.auth_required_cron
def reload_biobank_samples():
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME, None)
  if not bucket_name:
    logging.error("No bucket configured for %s", config.BIOBANK_SAMPLES_BUCKET_NAME)
    return '{"biobank-samples-pipeline-status": "error: no bucket configured"}'
  logging.info("=========== Starting biobank samples pipeline ============")
  pipeline = offline.biobank_samples_pipeline.BiobankSamplesPipeline(bucket_name)
  pipeline.start(queue_name='biobank-samples-pipeline')
  return '{"biobank-samples-pipeline-status": "started"}'


app = Flask(__name__)
#
# The REST-ful resources that are the bulk of the API.
#

api = Api(app)

#
# Non-resource pipeline-trigger endpoints
#

app.add_url_rule(PREFIX + 'BiobankSamplesReload',
                 endpoint='biobankSamplesReload',
                 view_func=reload_biobank_samples,
                 methods=['GET'])

app.add_url_rule(PREFIX + 'MetricsRecalculate',
                 endpoint='metrics_recalc',
                 view_func=recalculate_metrics,
                 methods=['GET'])

app.after_request(app_util.add_headers)
app.before_request(app_util.request_logging)
