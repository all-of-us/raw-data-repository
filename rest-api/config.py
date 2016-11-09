"""Configuration parameters.

Contains things such as the accounts allowed access to the system.


In order to have strong consistency, we have a model where we can use ancestor
queries to load all config values for a given key.  The parent object is a
ConfigKey which uses an ndb.Key based on the config key.  The child object is a
ConfigValue.
"""

from google.appengine.ext import ndb


class ConfigKey(ndb.Model):
  config_key = ndb.StringProperty()

class ConfigValue(ndb.Model):
  value = ndb.StringProperty()

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

def list_keys():
  """Returns all config settings in the datastore"""
  all_configs = ConfigKey.query().fetch()
  return set(c.config_key for c in all_configs)

def replace_config(key, value_list):
  """Replaces all config entries with the given key."""
  parent_key = ndb.Key(ConfigKey, key)
  existing_configs = list(ConfigValue.query(ancestor=parent_key).fetch())

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
  config_key = ndb.Key(ConfigKey, key).get()

  config_values = []
  if config_key:
    config_values = ConfigValue.query(ancestor=config_key.key).fetch()

  if not config_key or not config_values:
    if default is not None:
      return default
    raise MissingConfigException(
        'Config key "{}" has no values in the datastore.'.format(key))

  return [config.value for config in config_values]

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
  parent_key = ndb.Key(ConfigKey, key)
  parent = parent_key.get()
  if not parent:
    parent = ConfigKey(key=parent_key, config_key=key)
    parent.put()

  ConfigValue(parent=parent.key, value=value).put()
