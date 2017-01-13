"""The API definition for the metrics API.

This defines the APIs and the handlers for the APIs.
"""

import api_util
import datetime
import metrics
import json
import offline.metrics_pipeline

from api_util import HEALTHPRO
from protorpc import protojson
from flask import request, Response
from flask.ext.restful import Resource
from werkzeug.exceptions import NotFound

@api_util.auth_required_cron
def get():
  in_progress = metrics.get_in_progress_version()
  if in_progress:
    print "=========== Metrics pipeline already running ============"
    return '{"metrics-pipeline-status": "running"}'
  else:
    bucket_name = app_identity.get_default_gcs_bucket_name()    
    print "=========== Starting metrics pipeline ============"
    offline.metrics_pipeline.MetricsPipeline(bucket_name, datetime.datetime.now()).start()
    return '{"metrics-pipeline-status": "started"}'

class MetricsAPI(Resource):
  @api_util.auth_required(HEALTHPRO)
  def post(self):
    resource = request.get_data()
    metrics_request = protojson.decode_message(metrics.MetricsRequest, resource)
    serving_version = metrics.get_serving_version()
    if not serving_version:
      raise NotFound(
         'No Metrics with data version {} calculated yet.'.format(metrics.SERVING_METRICS_DATA_VERSION))
    return Response(to_json_list(metrics.SERVICE.get_metrics(metrics_request, serving_version)), 
                    content_type='application/json') 
    
class MetricsFieldsAPI(Resource):
  @api_util.auth_required(HEALTHPRO)
  def get(self):
    return metrics.SERVICE.get_metrics_fields()
    
def to_json_list(entries):
  yield '['
  first = True
  for entry in entries:
    if first:
      first = False
    else:
      yield ','
    yield entry
  yield ']'
