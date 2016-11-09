"""Configuration parameters.

Contains things such as the accounts allowed access to the system.
"""

from google.appengine.api import app_identity
from google.appengine.ext import ndb

class Config(ndb.Model):
  config_key = ndb.StringProperty()
  value = ndb.StringProperty()

_CONFIG_INITIALIZED = 'initialized'

ALLOWED_USER = 'allowed_user'
ALLOWED_IP = 'allowed_ip'
ALLOW_FAKE_HISTORY_DATES = 'allow_fake_history_dates'
METRICS_SHARDS = 'metrics_shards'
BIOBANK_SAMPLES_SHARDS = 'biobank_samples_shards'
BIOBANK_SAMPLES_BUCKET_NAME = 'biobank_samples_bucket_name'

class MissingConfigException(BaseException):
  """Exception raised if the setting does not exist"""

class InvalidConfigException(BaseException):
  """Exception raised when the config setting is a not in the expected form."""

_initialized = False

def list_keys():
  """Returns all config settings in the datastore"""
  all_configs = Config.query().fetch()
  return set(c.config_key for c in all_configs)

def replace_config(key, value_list):
  """Replaces all config entries with the given key."""
  existing_configs = list(Config.query(Config.config_key == key).fetch())

  existing_values = set(c.value for c in existing_configs)
  new_values = set(value_list)

  values_to_delete = existing_values - new_values

  for existing_config in existing_configs:
    if existing_config.value in values_to_delete:
      existing_config.key.delete()

  for value in new_values - existing_values:
    insert_config(key, value)

def getSettingList(key, default=None):
  """Gets all config settings for a given key.

  Args:
    key: The config key to retrieve entries for.
    default: What to return if the key does not exist in the datastore.

  Returns:
    A list of all config entries matching this key.

  Raises:
    MissingConfigException: If the config key does not exist in the datastore,
      and a default is not provided.
  """
  check_initialized()
  query = Config.query(Config.config_key == key)
  iterator = query.iter()
  if not iterator.has_next():
    if default is not None:
      return default
    raise MissingConfigException(
        'Config key "{}" is not in datastore.'.format(key))

  return [config.value for config in iterator]

def getSetting(key, default=None):
  """Gets a config where there is only a single setting for a given key.

  Args:
    key: The config key to look up.
    default: If the config key is not found in the datastore, this will be
      returned.

  Raises:
    InvalidConfigException: If the key has multiple entries in the datastore.
    MissingConfigException: If the config key does not exist in the datastore,
     and a default is not provided.
  """
  if default:
    default = [default]
  settings_list = getSettingList(key, default)

  if len(settings_list) != 1:
    raise InvalidConfigException(
        'Config key {} has multiple entries in datastore.'.format(key))
  return settings_list[0]

def insert_config(key, value):
  Config(config_key=key, value=value).put()

def check_initialized():
  global _initialized
  if _initialized:
    return
  _initialized = True
  # Create the config 'table' if it doesn't exist.
  print "Checking the config datastore is initialized..."
  try:
    getSetting(_CONFIG_INITIALIZED)
  except MissingConfigException:
    print "Creating and setting sane defaults for development..."
    insert_config(_CONFIG_INITIALIZED, 'True')
    insert_config(METRICS_SHARDS, '2')
    insert_config(BIOBANK_SAMPLES_SHARDS, '2')
    insert_config(BIOBANK_SAMPLES_BUCKET_NAME, app_identity.get_default_gcs_bucket_name())
    insert_config(ALLOWED_USER, 'pmi-hpo-staging@appspot.gserviceaccount.com')
    insert_config(ALLOWED_USER, 'test-client@pmi-rdr-api-test.iam.gserviceaccount.com')
    insert_config(ALLOWED_IP, '{"ip6": ["::1/64"], "ip4": ["127.0.0.1/32"]}')
