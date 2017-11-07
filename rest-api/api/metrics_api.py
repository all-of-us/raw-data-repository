import app_util
import datetime
import json

from api_util import HEALTHPRO
from dao.metrics_dao import MetricsBucketDao
from flask import request
from flask.ext.restful import Resource
from werkzeug.exceptions import BadRequest

DATE_FORMAT = '%Y-%m-%d'
DAYS_LIMIT = 7

class MetricsApi(Resource):

  @app_util.auth_required(HEALTHPRO)
  def post(self):
    dao = MetricsBucketDao()
    resource = request.get_data()
    if resource:
      resource_json = json.loads(resource)
      start_date_str = resource_json.get('start_date')
      end_date_str = resource_json.get('end_date')
      if not start_date_str or not end_date_str:
        raise BadRequest("Start date and end date should not be empty")
      try:
        start_date = datetime.datetime.strptime(start_date_str, DATE_FORMAT).date()
      except ValueError:
        raise BadRequest("Invalid start date: %s" % start_date_str)
      try:
        end_date = datetime.datetime.strptime(end_date_str, DATE_FORMAT).date()
      except ValueError:
        raise BadRequest("Invalid end date: %s" % end_date_str)
      date_diff = abs((end_date - start_date).days)
      if date_diff > DAYS_LIMIT:
        raise BadRequest("Difference between start date and end date "\
          "should not be greater than %s days" % DAYS_LIMIT)
      buckets = dao.get_active_buckets(start_date, end_date)
      if buckets is None:
        return []
      return [dao.to_client_json(bucket) for bucket in buckets]
    else:
      raise BadRequest("Request data is empty")
