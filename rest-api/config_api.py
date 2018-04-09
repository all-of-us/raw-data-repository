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
from api_util import parse_date

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
    return model.configuration

  def post(self, key=config.CONFIG_SINGLETON_KEY):
    model = config.Configuration(key=ndb.Key(config.Configuration, key),
                                 configuration=request.get_json(force=True))
    self.validate(model)
    config.store(model)
    return model.configuration

  def put(self, key=config.CONFIG_SINGLETON_KEY):
    model_key = ndb.Key(config.Configuration, key)
    old_model = model_key.get()
    if not old_model:
      raise NotFound('{} with key {} does not exist'.format('Configuration', key))
    # the history mechanism doesn't work unless we make a copy.  So a put is always a clone, never
    # an actual update.
    model = config.Configuration(**old_model.to_dict())
    model.key = model_key
    model.configuration = request.get_json(force=True)
    self.validate(model)

    date = None
    if config.getSettingJson(config.ALLOW_NONPROD_REQUESTS, False):
      date = request.headers.get('x-pretend-date', None)
    if date is not None:
      date = parse_date(date)

    client_id = app_util.get_oauth_id()

    config.store(model, date=date, client_id=client_id)
    return model.configuration

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
