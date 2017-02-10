"""The API definition for the config API."""

import api_util
import base_api
import config
import json
import logging
import os

from flask import request

from google.appengine.api import app_identity

from werkzeug.exceptions import Unauthorized, BadRequest

# Read bootstrap config admin service account configuration
CONFIG_ADMIN_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'config',
                                 'config_admins.json')
with open(CONFIG_ADMIN_FILE) as config_file:
  try:
    CONFIG_ADMIN_MAP = json.load(config_file)
  except IOError:
    logging.error('Unable to load config admin file %r.', CONFIG_ADMIN_FILE)
    CONFIG_ADMIN_MAP = {}


def auth_required_config_admin(func):
  """A decorator that checks that the caller is a config admin for the app."""
  def wrapped(*args, **kwargs):
    check_config_admin()
    return func(*args, **kwargs)
  return wrapped


def check_config_admin():
  """Raises Unauthorized unless the caller is a config admin."""
  app_id = app_identity.get_application_id()
  user_email = api_util.get_oauth_id()

  # Allow clients to simulate an unauthentiated request (for testing)
  # becaues we haven't found another way to create an unauthenticated request
  # when using dev_appserver. When client tests are checking to ensure that an
  # unauthenticated requests gets rejected, they helpfully add this header.
  # The `application_id` check ensures this feature only works in dev_appserver.
  if app_id == "None" and request.headers.get('unauthenticated'):
    user_email = None

  if user_email:
    config_admin = CONFIG_ADMIN_MAP.get(
        app_id,
        'configurator@{}.iam.gserviceaccount.com'.format(app_id))
    if user_email == config_admin:
      logging.info('User %r ALLOWED for config endpoint on %r' % (user_email, app_id))
      return
  logging.info('User %r NOT ALLOWED for config endpoint on %r.' % (user_email, app_id))
  raise Unauthorized('Forbidden.')


class ConfigApi(base_api.BaseApi):
  """Api handlers for retrieving and setting config values."""

  method_decorators = [auth_required_config_admin]

  def __init__(self):
    super(ConfigApi, self).__init__(config.DAO())

  def get_config_by_date(self, key, date):
    result = config.get_config_that_was_active_at(key, api_util.parse_date(date))
    return self.make_response_for_resource(self.dao.to_json(result))

  def get(self, key=None):
    date = request.args.get('date')
    key = key or config.CONFIG_SINGLETON_KEY
    if not date:
      # Return the live config.
      return super(ConfigApi, self).get(key)
    else:
      return self.get_config_by_date(key, date)

  # Insert or update to configuration
  def post(self, key=None):
    key = key or config.CONFIG_SINGLETON_KEY
    resource = request.get_json(force=True)
    m = self.dao.from_json(resource, None, key)
    self.validate_object(m, None)
    self.dao.store(m)
    return self.make_response_for_resource(self.dao.to_json(m))

  def put(self, key=None):
    return super(ConfigApi, self).put(key or config.CONFIG_SINGLETON_KEY)

  def validate_object(self, obj, a_id=None):
    super(ConfigApi, self).validate_object(obj, a_id)
    if obj.key.id() != config.CONFIG_SINGLETON_KEY:
      return

    config_obj = obj.configuration
    # make sure all required keys are present and that the values are the right type.
    for k in config.REQUIRED_CONFIG_KEYS:
      if k not in config_obj:
        raise BadRequest('Missing required config key {}'.format(k))
      val = config_obj[k]
      if not isinstance(val, list) or [v for v in val if not isinstance(v, basestring)]:
        raise BadRequest('Config for {} must be a list of strings'.format(k))
