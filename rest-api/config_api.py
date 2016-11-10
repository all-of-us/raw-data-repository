"""The API definition for the config API."""

import api_util
import base_api
import config

from flask import request
from flask.ext.restful import Resource

from werkzeug.exceptions import BadRequest

class ConfigApi(base_api.BaseAdminApi):
  """Api handlers for retrieving and setting config values."""

  def __init__(self):
    super(ConfigApi, self).__init__(config.DAO)

  def get(self, key=None):
    if not key:
      # Return the only config.
      return super(ConfigApi, self).get(config.CONFIG_SINGLETON_KEY)
    else:
      result = config.get_config_that_was_active_at(api_util.parse_date(key))
      return self.make_response_for_resource(self.dao.to_json(result))

  def put(self, key=None):
    return super(ConfigApi, self).put(config.CONFIG_SINGLETON_KEY)

  def validate_object(self, obj, a_id=None):
    super(ConfigApi, self).validate_object(obj, a_id)
    config_obj = obj.configuration
    # make sure all required keys are present and that the values are the right type.
    for k in config.REQUIRED_CONFIG_KEYS:
      if k not in config_obj:
        raise BadRequest('Missing required config key {}'.format(k))

    for k, val in config_obj.iteritems():
      if not isinstance(val, list) or [v for v in val if not isinstance(v, basestring)]:
        raise BadRequest('Config for {} must be a list of strings'.format(k))
