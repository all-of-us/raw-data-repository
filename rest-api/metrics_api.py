"""The API definition for the metrics API.

This defines the APIs and the handlers for the APIs.
"""

import api_util
import metrics
import json
import offline.metrics_pipeline

from api_util import HEALTHPRO
from protorpc import protojson
from flask import request

@api_util.auth_required_cron_or_admin
def get():
  in_progress = metrics.get_in_progress_version()
  if in_progress:
    print "=========== Metrics pipeline already running ============"
    return '{"metrics-pipeline-status": "running"}'
  else:
    print "=========== Starting metrics pipeline ============"
    offline.metrics_pipeline.MetricsPipeline().start()
    return '{"metrics-pipeline-status": "started"}'


@api_util.auth_required(HEALTHPRO)
def post():
  resource = request.get_data()
  metrics_request = protojson.decode_message(metrics.MetricsRequest, resource)
  metrics_response = metrics.SERVICE.get_metrics(metrics_request)

  return protojson.encode_message(metrics_response)
