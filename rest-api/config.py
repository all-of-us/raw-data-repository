"""Configuration parameters.

Contains things such as the accounts allowed access to the system.


In order to have strong consistency, we have a model where we can use ancestor
queries to load all config values for a given key.  The parent object is a
ConfigKey which uses an ndb.Key based on the config key.  The child object is a
ConfigValue.
"""
import data_access_object

from google.appengine.ext import ndb

from werkzeug.exceptions import NotFound

CONFIG_SINGLETON_KEY = 'current_config'

ALLOWED_USER = 'allowed_user'
ALLOWED_IP = 'allowed_ip'
ALLOW_FAKE_HISTORY_DATES = 'allow_fake_history_dates'
METRICS_SHARDS = 'metrics_shards'
BIOBANK_SAMPLES_SHARDS = 'biobank_samples_shards'
BIOBANK_SAMPLES_BUCKET_NAME = 'biobank_samples_bucket_name'

REQUIRED_CONFIG_KEYS = [ALLOWED_USER, ALLOWED_IP, BIOBANK_SAMPLES_BUCKET_NAME]

class MissingConfigException(BaseException):
  """Exception raised if the setting does not exist"""


class InvalidConfigException(BaseException):
  """Exception raised when the config setting is a not in the expected form."""


class Configuration(ndb.Model):
  configuration = ndb.JsonProperty()


class ConfigurationDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ConfigurationDAO, self).__init__(Configuration)

  def properties_from_json(self, dict_, ancestor_id, id_):
    return {
        "configuration": dict_
    }

  def properties_to_json(self, dict_):
    return dict_.get('configuration', {})

  def load_if_present(self, id_, ancestor_id=None):
    obj = super(ConfigurationDAO, self).load_if_present(id_, ancestor_id)
    if not obj:
      # A side-effect of this call is that it will create an empty configuration.
      getSettingList('foo', ['foo'])
      obj = super(ConfigurationDAO, self).load_if_present(id_, ancestor_id)
    return obj

  def list(self, participant_id):
    super(ConfigurationDAO, self).list(participant_id)
    # return the current config here.

  def allocate_id(self):
    return CONFIG_SINGLETON_KEY


DAO = ConfigurationDAO()

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
  conf_ndb_key = ndb.Key(Configuration, CONFIG_SINGLETON_KEY)
  conf_model = conf_ndb_key.get()

  if not conf_model:
    # Initalize an empty configuration.
    Configuration(key=conf_ndb_key, configuration={}).put()
    print 'Setting an empty configuration.'
    config_values = default
  else:
    configuration = conf_model.configuration
    config_values = configuration.get(key, default)

  if not config_values:
    raise MissingConfigException('Config key "{}" has no values.'.format(key))

  return config_values

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


def insert_config(key, value_list):
  """Updates a config key.  Used for tests"""
  conf = DAO.load(CONFIG_SINGLETON_KEY)
  conf.configuration[key] = value_list
  DAO.store(conf)

def get_config_that_was_active_at(date):
  q = DAO.history_model.query(DAO.history_model.date < date).order(-DAO.history_model.date)
  h = q.fetch(limit=1)
  if not h:
    raise NotFound('No history object active at {}.'.format(date))
  return h[0].obj
