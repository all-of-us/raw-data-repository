"""Configuration parameters.

Contains things such as the accounts allowed access to the system.
"""
import logging

from google.appengine.ext import ndb
from werkzeug.exceptions import NotFound

import clock
import singletons


# Key that the main server configuration is stored under
CONFIG_SINGLETON_KEY = 'current_config'

# Key that the database configuration is stored under
DB_CONFIG_KEY = 'db_config'

LAST_MODIFIED_BUFFER_SECONDS = 60
CONFIG_CACHE_TTL_SECONDS = 60
BIOBANK_ID_PREFIX = 'biobank_id_prefix'
METRICS_SHARDS = 'metrics_shards'
PARTICIPANT_SUMMARY_SHARDS = 'participant_summary_shards'
AGE_RANGE_SHARDS = 'age_range_shards'
BIOBANK_SAMPLES_SHARDS = 'biobank_samples_shards'
BIOBANK_SAMPLES_BUCKET_NAME = 'biobank_samples_bucket_name'
CONSENT_PDF_BUCKET = 'consent_pdf_bucket'
USER_INFO = 'user_info'
SYNC_SHARDS_PER_CHANNEL = 'sync_shards_per_channel'
MEASUREMENTS_ENTITIES_PER_SYNC = 'measurements_entities_per_sync'
BASELINE_PPI_QUESTIONNAIRE_FIELDS = 'baseline_ppi_questionnaire_fields'
PPI_QUESTIONNAIRE_FIELDS = 'ppi_questionnaire_fields'
BASELINE_SAMPLE_TEST_CODES = 'baseline_sample_test_codes'
DNA_SAMPLE_TEST_CODES = 'dna_sample_test_codes'

# Allow requests which are never permitted in production. These include fake
# timestamps for reuqests, unauthenticated requests to create fake data, etc.
ALLOW_NONPROD_REQUESTS = 'allow_nonprod_requests'

# Settings for e-mail alerts for failed jobs.
INTERNAL_STATUS_MAIL_SENDER = 'internal_status_email_sender'
INTERNAL_STATUS_MAIL_RECIPIENTS = 'internal_status_email_recipients'
BIOBANK_STATUS_MAIL_RECIPIENTS = 'biobank_status_mail_recipients'

# True if we should add codes referenced in questionnaires that
# aren't in the code book; false if we should reject the questionnaires.
ADD_QUESTIONNAIRE_CODES_IF_MISSING = 'add_questionnaire_codes_if_missing'
REQUIRED_CONFIG_KEYS = [BIOBANK_SAMPLES_BUCKET_NAME]

# Service account key age
DAYS_TO_DELETE = 3

CONFIG_OVERRIDES = {}

class BaseConfig(object):

  def __init__(self, config_key):
    config_obj = ConfigurationDAO().load_if_present(config_key)
    if config_obj is None:
      raise KeyError('No config for %r.' % config_key)

    self.config_dict = config_obj.configuration


class MainConfig(BaseConfig):

  def __init__(self):
    super(MainConfig, self).__init__(CONFIG_SINGLETON_KEY)


class DbConfig(BaseConfig):

  def __init__(self):
    super(DbConfig, self).__init__(DB_CONFIG_KEY)

REQUIRED_CONFIG_KEYS = [BIOBANK_SAMPLES_BUCKET_NAME]


# Overrides for testing scenarios
CONFIG_OVERRIDES = {}

def override_setting(key, value):
  """Overrides a config setting. Used in tests."""
  CONFIG_OVERRIDES[key] = value

def store_current_config(config_json):
  conf_ndb_key = ndb.Key(Configuration, CONFIG_SINGLETON_KEY)
  conf = Configuration(key=conf_ndb_key, configuration=config_json)
  store(conf)

def insert_config(key, value_list):
  """Updates a config key.  Used for tests"""
  model = load(CONFIG_SINGLETON_KEY)
  model.configuration[key] = value_list
  store(model)


class MissingConfigException(Exception):
  """Exception raised if the setting does not exist"""


class InvalidConfigException(Exception):
  """Exception raised when the config setting is not in the expected form."""


class Configuration(ndb.Model):
  configuration = ndb.JsonProperty()


class ConfigurationHistory(ndb.Model):
  date = ndb.DateTimeProperty(auto_now_add=True)
  obj = ndb.StructuredProperty(Configuration, repeated=False)
  client_id = ndb.StringProperty()


def load(_id=CONFIG_SINGLETON_KEY, date=None):
  key = ndb.Key(Configuration, _id)
  if date is not None:
    history = (ConfigurationHistory
                .query(ancestor=ndb.Key('Configuration', _id))
                .filter(ConfigurationHistory.date <= date)
                .order(-ConfigurationHistory.date)
                .fetch(limit=1)
               )
    if not history:
      raise NotFound('No history object active at {}.'.format(date))
    return history[0].obj

  model = key.get()
  if model is None:
    if _id == CONFIG_SINGLETON_KEY:
      model = Configuration(key=key, configuration={})
      model.put()
      logging.info('Setting an empty configuration.')
    else:
      raise NotFound('No config for %r.' % _id)
  return model


@ndb.transactional
def store(model, date=None, client_id=None):
  date = date or clock.CLOCK.now()
  history = ConfigurationHistory(parent=model.key, obj=model, date=date)
  if client_id:
    history.populate(client_id=client_id)
  history.put()
  model.put()
  singletons.invalidate(singletons.DB_CONFIG_INDEX)
  singletons.invalidate(singletons.MAIN_CONFIG_INDEX)
  return model


_NO_DEFAULT = '_NO_DEFAULT'

def getSettingJson(key, default=_NO_DEFAULT):
  """Gets a config setting as an arbitrary JSON structure

  Args:
    key: The config key to retrieve entries for.
    default: What to return if the key does not exist in the datastore.

  Returns:
    The value from the Config store, or the default if not present

  Raises:
    MissingConfigException: If the config key does not exist in the datastore,
      and a default is not provided.
  """
  config_values = CONFIG_OVERRIDES.get(key)
  if config_values is not None:
    return config_values

  current_config = get_config()

  config_values = current_config.get(key, default)
  if config_values == _NO_DEFAULT:
    raise MissingConfigException('Config key "{}" has no values.'.format(key))

  return config_values


def getSettingList(key, default=_NO_DEFAULT):
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
  config_json = getSettingJson(key, default)
  if isinstance(config_json, list):
    return config_json

  raise InvalidConfigException(
      'Config key {} is a {} instead of a list'.format(key, type(config_json)))


def getSetting(key, default=_NO_DEFAULT):
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
  if default != _NO_DEFAULT:
    default = [default]
  settings_list = getSettingList(key, default)

  if len(settings_list) != 1:
    raise InvalidConfigException(
        'Config key {} has multiple entries in datastore.'.format(key))
  return settings_list[0]


def get_db_config():
  model = singletons.get(singletons.DB_CONFIG_INDEX,
                         lambda: load(DB_CONFIG_KEY),
                         cache_ttl_seconds=CONFIG_CACHE_TTL_SECONDS)
  return model.configuration

def get_config():
  model = singletons.get(singletons.MAIN_CONFIG_INDEX,
                         lambda: load(CONFIG_SINGLETON_KEY),
                         cache_ttl_seconds=CONFIG_CACHE_TTL_SECONDS)
  return model.configuration
