"""The API definition for the config API."""
import json
import logging
import os

from flask import request
from flask.ext.restful import Resource
from google.appengine.api import app_identity
from google.appengine.ext import ndb
from werkzeug.exceptions import BadRequest, Forbidden, NotFound

import app_util
import config
from api_util import unix_time_millis, parse_date

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


def is_config_admin(user_email):
  app_id = app_identity.get_application_id()

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
      return True
  return False


def check_config_admin():
  """Raises Unauthorized unless the caller is a config admin."""
  user_email = app_util.get_oauth_id()
  if is_config_admin(user_email):
    logging.info('User %r ALLOWED for config endpoint' % user_email)
    return
  logging.info('User %r NOT ALLOWED for config endpoint' % user_email)
  raise Forbidden()


class ConfigApi(Resource):
  """Api handlers for retrieving and setting config values."""
  method_decorators = [auth_required_config_admin]

  def get(self, key=config.CONFIG_SINGLETON_KEY):
    date = request.args.get('date')
    if date is not None:
      date = parse_date(date)
    model = config.load(key, date=date)
    data = model.configuration or {}
    return self.make_response_for_resource(data)

  def post(self, key=config.CONFIG_SINGLETON_KEY):
    resource = request.get_json(force=True)
    keypath = [config.Configuration, key]
    model = config.Configuration(key=ndb.Key(flat=keypath))
    model.populate(configuration=resource)
    self.validate(model)
    config.store(model)
    return self.make_response_for_resource(model.configuration)

  def put(self, key=config.CONFIG_SINGLETON_KEY):
    resource = request.get_json(force=True)
    keypath = [config.Configuration, key]
    model = config.Configuration(key=ndb.Key(flat=keypath))
    model.populate(configuration=resource)

    self.validate(model)
    if not model.key.get():
      raise NotFound('{} with key {} does not exist'.format('Configuration', model.key))

    date = None
    if config.getSettingJson(config.ALLOW_NONPROD_REQUESTS, False):
      date = request.headers.get('x-pretend-date', None)
      if date:
        date = parse_date(date)

    client_id = app_util.get_oauth_id()
    config.store(model, date=date, client_id=client_id)
    return self.make_response_for_resource(model.configuration)

  def make_response_for_resource(self, data):
    last_modified = data.pop('last_modified', None)
    if last_modified:
      version_id = 'W/"{}"'.format(unix_time_millis(last_modified))
      data['meta'] = {'versionId': version_id}
      return data, 200, {'ETag': version_id}
    return data

  def validate(self, model):
    if model.key.id() != config.CONFIG_SINGLETON_KEY:
      return

    config_obj = model.configuration
    # make sure all required keys are present and that the values are the right type.
    for k in config.REQUIRED_CONFIG_KEYS:
      if k not in config_obj:
        raise BadRequest('Missing required config key {}'.format(k))
      val = config_obj[k]
      if not isinstance(val, list) or [v for v in val if not isinstance(v, basestring)]:
        raise BadRequest('Config for {} must be a list of strings'.format(k))
