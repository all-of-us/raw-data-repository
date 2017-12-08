import app_util

from api_util import STOREFRONT
from flask.ext.restful import Resource
from werkzeug import exceptions


class MetricSetsApi(Resource):

  @app_util.auth_required(STOREFRONT)
  def get(self):
    raise exceptions.NotImplementedError()

  @app_util.auth_required(STOREFRONT)
  def get(self, ms_id):
    #pylint: disable=unused-argument
    raise exceptions.NotImplementedError()
