"""The API definition for the config API."""

import api_util
import config
import json

from protorpc import messages
from protorpc import protojson
from flask import request
from flask.ext.restful import Resource

class ConfigResponse(messages.Message):
  key = messages.StringField(1)
  values = messages.StringField(2, repeated=True)

class ConfigApi(Resource):
  """Api handlers for retrieving and setting config values."""
  @api_util.auth_required_cron_or_admin
  def get(self, key=None):
    if not key:
      ret = [_json_encode_config(k) for k in config.list_keys()]
    else:
      ret = _json_encode_config(key)
    return ret

  @api_util.auth_required_cron_or_admin
  def post(self, key):
    resource = request.get_json(force=True)
    values = resource['values']
    config.replace_config(key, values)
    # Because of eventual consistency, it is hard to get all config values for
    # the given key right away.  Just return the request instead of reading the
    # values from the datastore.
    return json.loads(protojson.encode_message(ConfigResponse(key=key, values=values)))


def _json_encode_config(key):
  """Creates a protojson encoded reporesentation of a ConfigResponse for the given key."""
  values = config.getSettingList(key, [])
  json_encoded = protojson.encode_message(ConfigResponse(key=key, values=values))
  return json.loads(json_encoded)
