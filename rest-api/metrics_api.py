"""The API definition for the metrics API.

This defines the APIs and the handlers for the APIs.
"""

import api_util
import config
import metrics

from protorpc import protojson
from flask import request
from flask.ext.restful import Resource

class MetricsApi(Resource):
  def post(self):
    api_util.check_auth()
    resource = request.get_data()
    metrics_request = protojson.decode_message(metrics.MetricsRequest, resource)

    metrics_response = metrics.SERVICE.get_metrics(metrics_request)

    return protojson.encode_message(metrics_response)
