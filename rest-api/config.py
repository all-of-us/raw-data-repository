"""Configuration parameters.

Contains things such as the accounts allowed access to the system.
"""

from google.appengine.ext import ndb


class Config(ndb.Model):
  config_key = ndb.StringProperty()
  value = ndb.StringProperty()

_CONFIG_INITIALIZED = 'initialized'

ALLOWED_USER = 'allowed_user'
ALLOW_FAKE_HISTORY_DATES = 'allow_fake_history_dates'
METRICS_SHARDS = 'metrics_shards'


class MissingConfigException(BaseException):
  """Exception raised if the setting does not exist"""

class InvalidConfigException(BaseException):
  """Exception raised when the config setting is a not in the expected form."""

_initialized = False

def getSettingList(key, default=None):
  """Gets all config settings for a given key."""
  check_initialized()
  query = Config.query(Config.config_key==key)
  iterator = query.iter()
  if not iterator.has_next():
    if default is not None:
      return default
    raise MissingConfigException(
        'Config key "{}" is not in datastore.'.format(key))

  return [config.value for config in iterator]

def getSetting(key, default=None):
  """Gets a config where there is only be a single setting for a given key.

  If no default is specified, a InvalidConfigException will be raised if the key
     is not found in the datastore.

  Args:
    key: The config key to look up.
    default: If the config key is not found in the datastore, this will be
      returned.
  """
  if default:
    default = [default]
  settings_list = getSettingList(key, default)

  if len(settings_list) != 1:
    raise InvalidConfigException(
        'Config key {} has multiple entries in datastore.'.format(key))
  return settings_list[0]


def check_initialized():
  global _initialized
  if _initialized:
    return
  _initialized = True
  # Create the config 'table' if it doesn't exist.
  print "Checking the config datastore is initialized..."
  try:
    setting = getSetting(_CONFIG_INITIALIZED)
  except MissingConfigException:
    print "Creating and setting sane defaults for development..."
    Config(config_key=_CONFIG_INITIALIZED, value='True').put()
    Config(config_key=METRICS_SHARDS, value='2').put()
    Config(config_key=ALLOWED_USER,
           value='pmi-hpo-staging@appspot.gserviceaccount.com').put()
    Config(config_key=ALLOWED_USER,
           value='test-client@pmi-rdr-api-test.iam.gserviceaccount.com').put()
