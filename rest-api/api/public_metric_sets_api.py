import app_util

from api_util import STOREFRONT
from flask.ext.restful import Resource
from werkzeug import exceptions


class PublicMetricSetsApi(Resource):

  @app_util.auth_required(STOREFRONT)
  def get(self):
    raise exceptions.NotImplementedError()
