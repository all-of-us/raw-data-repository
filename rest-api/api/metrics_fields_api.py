import api_util
import offline.metrics_config

from api.base_api import BaseApi
from api_util import HEALTHPRO
from flask.ext.restful import Resource

class MetricsFieldsApi(Resource):
  """API that returns the names and valid values for metric fields."""

  @api_util.auth_required(HEALTHPRO)
  def get(self):
    return offline.metrics_config.get_fields()