"""The main API definition file for endpoints that trigger MapReduces.
"""
import api_util
import app_util
import config
import datetime
import metrics
import offline.age_range_pipeline
import offline.biobank_samples_pipeline
import offline.metrics_pipeline
import offline.participant_summary_pipeline

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
    logging.info("=========== Starting metrics pipeline ============")
    pipeline = offline.metrics_pipeline.MetricsPipeline(bucket_name, datetime.datetime.utcnow())
    pipeline.start(queue_name='metrics-pipeline')
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

@api_util.auth_required_cron
def regenerate_participant_summaries():
  # TODO(danrodney): check to see if it's already running?
  logging.info("=========== Starting participant summary regeneration pipeline ============")
  pipeline = offline.participant_summary_pipeline.ParticipantSummaryPipeline()
  pipeline.start(queue_name='participant-summary-pipeline')
  return '{"metrics-pipeline-status": "started"}'


@api_util.auth_required_cron
def update_participant_summary_age_ranges():
  # TODO(danrodney): check to see if it's already running?
  logging.info("=========== Starting age range update pipeline ============")
  pipeline = offline.age_range_pipeline.AgeRangePipeline(datetime.datetime.utcnow())
  pipeline.start(queue_name='age-range-pipeline')
  return '{"metrics-pipeline-status": "started"}'


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

app.add_url_rule(PREFIX + 'AgeRangeUpdate',
                 endpoint='ageRangeUpdate',
                 view_func=update_participant_summary_age_ranges,
                 methods=['GET'])

app.add_url_rule(PREFIX + 'RegenerateParticipantSummaries',
                 endpoint='regenerateParticipantSummaries',
                 view_func=regenerate_participant_summaries,
                 methods=['GET'])

app.after_request(app_util.add_headers)
app.before_request(app_util.request_logging)
