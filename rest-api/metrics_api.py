"""The API definition for the metrics API.

This defines the APIs and the handlers for the APIs.
"""

import api_util
import datetime
import logging
import metrics

from api_util import HEALTHPRO
from google.appengine.api import app_identity
from protorpc import protojson
from flask import request, Response
from flask.ext.restful import Resource
from werkzeug.exceptions import NotFound

class MetricsAPI(Resource):
  @api_util.auth_required(HEALTHPRO)
  def post(self):
    resource = request.get_data()
    metrics_request = protojson.decode_message(metrics.MetricsRequest, resource)
    serving_version = metrics.get_serving_version()
    if not serving_version:
      raise NotFound(
         'No Metrics with data version %r calculated yet.' % metrics.SERVING_METRICS_DATA_VERSION)
    return Response(to_json_list(metrics.SERVICE.get_metrics(metrics_request, serving_version)), 
                    content_type='application/json') 
    
class MetricsFieldsAPI(Resource):
  @api_util.auth_required(HEALTHPRO)
  def get(self):
    return metrics.SERVICE.get_metrics_fields()
    
# Because we want to stream the JSON to the client, rather than load
# it all into memory at once, we use this function (instead of json.dumps).
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
