"""Configuration parameters.

Contains things such as the database to connect to.
"""

from google.appengine.ext import ndb


class Config(ndb.Model):
  config_key = ndb.StringProperty()
  value = ndb.StringProperty()


CLOUDSQL_INSTANCE = 'cloudsql_instance'
CLOUDSQL_USER = 'cloudsql_user'
CLOUDSQL_PASSWORD = 'cloudsql_password'

ALLOWED_USER = 'allowed_user'
ALLOWED_CLIENT_ID = 'allowed_client_id'

class MissingConfigException(BaseException):
  """Exception raised if the setting does not exist"""

class InvalidConfigException(BaseException):
  """Exception raised when the config setting is a not in the expected form."""

_initialized = False

def getSettingList(key):
  """Gets all config settings for a given key."""
  check_initialized()
  query = Config.query(Config.config_key==key)
  iterator = query.iter()
  if not iterator.has_next():
    raise MissingConfigException(
        'Config key "{}" is not in datastore.'.format(key))

  return [config.value for config in iterator]

def getSetting(key):
  """Gets a config where there is only be a single setting for a given key."""
  settings_list = getSettingList(key)
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
    setting = getSetting('initialized')
  except MissingConfigException:
    print "Creating and setting sane defaults for development..."
    Config(config_key='initialized', value='True').put()
    Config(config_key='allowed_user',
           value='pmi-hpo-staging@appspot.gserviceaccount.com').put()
    Config(config_key='allowed_user',
           value='test-client@pmi-rdr-api-test.iam.gserviceaccount.com').put()
  _initialized = True
