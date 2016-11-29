"""The API definition for the config API."""

import api_util
import base_api
import config

from google.appengine.api import app_identity

from werkzeug.exceptions import Unauthorized, BadRequest

# Read bootstrap config admin service account configuration
CONFIG_ADMIN_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'config',
                                 'config_admin.json')
with open(CONFIG_ADMIN_FILE) as config_file:
  CONFIG_ADMIN_MAP = json.load(config_file)


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
  if user_email:
    config_admin = CONFIG_ADMIN_MAP.get(
        app_id,
        'configurator@{}.iam.gserviceaccount.com'.format(app_id))
    if user_email == config_admin:
      logging.info('User {} ALLOWED for config endpoint'.format(user_email))
      return
  logging.info('User {} NOT ALLOWED for config endpoint',format(user_email))
  raise Unauthorized('Forbidden.')


class ConfigApi(base_api.BaseApi):
  """Api handlers for retrieving and setting config values."""

  method_decorators = [auth_required_config_admin]

  def __init__(self):
    super(ConfigApi, self).__init__(config.DAO)

  def get_config_by_date(self, date):
    result = config.get_config_that_was_active_at(api_util.parse_date(date))
    return self.make_response_for_resource(self.dao.to_json(result))

  def get(self, key=None):
    if not key:
      # Return the only config.
      return super(ConfigApi, self).get(config.CONFIG_SINGLETON_KEY)
    else:
      return self.get_config_by_date(key)

  def put(self, key=None):
    ret = super(ConfigApi, self).put(config.CONFIG_SINGLETON_KEY)
    config.invalidate()
    return ret

  def validate_object(self, obj, a_id=None):
    super(ConfigApi, self).validate_object(obj, a_id)
    config_obj = obj.configuration
    # make sure all required keys are present and that the values are the right type.
    for k in config.REQUIRED_CONFIG_KEYS:
      if k not in config_obj:
        raise BadRequest('Missing required config key {}'.format(k))
      val = config_obj[k]
      if not isinstance(val, list) or [v for v in val if not isinstance(v, basestring)]:
        raise BadRequest('Config for {} must be a list of strings'.format(k))
