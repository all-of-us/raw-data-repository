"""The API definition for the metrics API.

This defines the APIs and the handlers for the APIs.
"""

import api_util
import metrics
import json
import offline.metrics_pipeline

from protorpc import protojson
from flask import request
from flask.ext.restful import Resource

class MetricsApi(Resource):
  @api_util.auth_required_cron_or_admin
  def get(self):
    in_progress = metrics.get_in_progress_version()
    if in_progress:
      print "=========== Metircs pipeline already running ============"
      return "Pipeline already running."
    else:
      print "=========== Starting metrics pipeline ============"
      offline.metrics_pipeline.MetricsPipeline().start()


  @api_util.auth_required()
  def post(self):
    resource = request.get_data()
    metrics_request = protojson.decode_message(metrics.MetricsRequest, resource)
    metrics_response = metrics.SERVICE.get_metrics(metrics_request)

    return json.loads(protojson.encode_message(metrics_response))
