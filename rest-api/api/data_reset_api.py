import json

from flask import request
from flask.ext.restful import Resource
from werkzeug.exceptions import BadRequest

from api_util import nonprod
from config_api import auth_required_config_admin
from dao.database_utils import reset_for_tests

_REALLY = 'really_delete_everything'


class DataResetApi(Resource):
  @auth_required_config_admin
  @nonprod
  def post(self):
    resource = request.get_data()
    resource_json = json.loads(resource)
    if not resource_json.get(_REALLY):
      raise BadRequest('Please send {%r: True} if you really want to reset.' % _REALLY)
    reset_for_tests()
    return {'OK': True}
