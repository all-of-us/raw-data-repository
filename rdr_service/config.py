"""Configuration parameters.

Contains things such as the accounts allowed access to the system.
"""
import base64
import logging  # pylint: disable=unused-import

import json
import os

from abc import ABC, abstractmethod

from google.cloud import datastore
from werkzeug.exceptions import NotFound  # pylint: disable=unused-import

from rdr_service import clock, singletons
from rdr_service.provider import Provider


# Get project name and credentials
if os.getenv('GAE_ENV', '').startswith('standard'):
    # Production in the standard environment
    import google.auth
    GAE_CREDENTIALS, GAE_PROJECT = google.auth.default()
    GAE_VERSION_ID = os.environ.get('GAE_VERSION')
else:
    GAE_CREDENTIALS = 'local@localhost.net'
    GAE_PROJECT = 'localhost'
    GAE_VERSION_ID = 'develop'


_NO_DEFAULT = "_NO_DEFAULT"


# Key that the main server configuration is stored under
CONFIG_SINGLETON_KEY = "current_config"

# Key that the database configuration is stored under
DB_CONFIG_KEY = "db_config"
LAST_MODIFIED_BUFFER_SECONDS = 60
CONFIG_CACHE_TTL_SECONDS = 60
BIOBANK_ID_PREFIX = "biobank_id_prefix"
METRICS_SHARDS = "metrics_shards"
PARTICIPANT_SUMMARY_SHARDS = "participant_summary_shards"
AGE_RANGE_SHARDS = "age_range_shards"
BIOBANK_SAMPLES_SHARDS = "biobank_samples_shards"
BIOBANK_SAMPLES_BUCKET_NAME = "biobank_samples_bucket_name"
QUEST_ANTIBODY_STUDY_BUCKET_NAME = "quest_antibody_study_bucket_name"
PTC_CLIA_COMPLIANT_REPORT_BUCKET_NAME = "ptc_clia_compliant_report_bucket_name"
CE_CLIA_COMPLIANT_REPORT_BUCKET_NAME = "ce_clia_compliant_report_bucket_name"
# how many days' data do we want to import, default is 1 day
ANTIBODY_DATA_IMPORT_DAYS = "antibody_data_import_days"
GENOMIC_CENTER_BUCKET_NAME = 'genomic_center_bucket_name'
GENOMIC_CENTER_DATA_BUCKET_NAME = 'genomic_center_data_bucket_name'
GENOMIC_SET_BUCKET_NAME = "genomic_set_bucket_name"
GENOMIC_GC_METRICS_BUCKET_NAME = 'genomic_gc_metrics_bucket_name'
GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME = "genomic_biobank_manifest_folder_name"
GENOMIC_BIOBANK_MANIFEST_RESULT_FOLDER_NAME = "genomic_biobank_manifest_result_folder_name"
GENOMIC_GENOTYPING_SAMPLE_MANIFEST_FOLDER_NAME = 'genotyping_sample_manifest_folder_name'
GENOMIC_CVL_RECONCILIATION_REPORT_SUBFOLDER = "genomic_cvl_reconciliation_report_subfolder"
GENOMIC_GC_PROCESSED_FOLDER_NAME = "processed_by_rdr"
A1_LIMIT = "a1_limit"
CVL_W1_MANIFEST_SUBFOLDER = "w1_wgs_cvl_manifests"
CVL_W2_MANIFEST_SUBFOLDER = "w2_wgs_cvl_manifests"
CVL_W3_MANIFEST_SUBFOLDER = "w3_wgs_cvl_manifests"
CVL_W1IL_HDR_MANIFEST_SUBFOLDER = "W1IL_manifests_hdr"
CVL_W1IL_PGX_MANIFEST_SUBFOLDER = "W1IL_manifests_pgx"
CVL_W2W_MANIFEST_SUBFOLDER = "W2W_manifests"
CVL_W3SR_MANIFEST_SUBFOLDER = "W3SR_manifests"
GENOMIC_AW2_SUBFOLDERS = ("aw2_wgs_data_manifest_subfolder_name", "aw2_genotyping_data_manifest_subfolder_name")
GENOMIC_AW5_WGS_SUBFOLDERS = "aw5_wgs_data_manifest_subfolder_name"
GENOMIC_AW5_ARRAY_SUBFOLDERS = "aw5_genotyping_data_manifest_subfolder_name"
GENOMIC_AW1F_SUBFOLDERS = ("aw1f_array_subfolder_name", "aw1f_wgs_subfolder_name")
GENOMIC_AW1_SUBFOLDERS = ("aw1_array_subfolder_name", "aw1_wgs_subfolder_name")
GENOMIC_GEM_BUCKET_NAME = "genomic_gem_bucket_name"
GENOMIC_CVL_BUCKET_NAME = "genomic_cvl_bucket_name"
GENOMIC_GEM_A1_MANIFEST_SUBFOLDER = "a1_manifests"
GENOMIC_GEM_A2_MANIFEST_SUBFOLDER = "a2_manifests"
GENOMIC_GEM_A3_MANIFEST_SUBFOLDER = "a3_manifests"
GENOMIC_CVL_AW1C_MANIFEST_SUBFOLDER = "AW1C_cvl_samples_manifests"
GENOMIC_CVL_AW1CF_MANIFEST_SUBFOLDER = ("AW1CF_cvl_accessioning_results/AW1CF_post_results",
                                        "AW1CF_cvl_accessioning_results/AW1CF_pre_results")
GENOMIC_AW1F_SUBFOLDER = "post_accessioning_results"
GENOME_TYPE_ARRAY = "aou_array"
GENOME_TYPE_WGS = "aou_wgs"
GENOME_TYPE_CVL = "aou_cvl"
GENOME_TYPE_ARRAY_INVESTIGATION = "aou_array_investigation"
GENOME_TYPE_WGS_INVESTIGATION = "aou_wgs_investigation"
AW1F_ALERT_RECIPIENTS = "aw1f_alert_recipients"
GENOMIC_AW3_ARRAY_SUBFOLDER = "AW3_array_manifest"
GENOMIC_AW3_WGS_SUBFOLDER = "AW3_wgs_manifest"
GENOMIC_INGESTION_REPORT_PATH = "/prod-rdr-genomics/daily_ingestion_reports/"
GENOMIC_INCIDENT_REPORT_PATH = "/prod-rdr-genomics/daily_incident_reports/"
GENOMIC_RESOLVED_REPORT_PATH = "/prod-rdr-genomics/daily_resolved_reports/"
GENOMIC_CRON_JOBS = "genomic_cron_jobs"
GENOMIC_MAX_NUM_GENERATE = "genomic_max_num_generate"
GENOMIC_MAX_NUM_INGEST = "genomic_max_num_ingest"
GENOMIC_DAILY_VALIDATION_EMAILS = "genomic_daily_validation_emails"
GENOMIC_CVL_RECONCILE_EMAILS = "genomic_cvl_reconcile_emails"
GENOMIC_CVL_RECONCILE_LIMITS = "genomic_cvl_reconcile_limits"
GENOMIC_MEMBER_BLOCKLISTS = "genomic_set_member_blocklists"
GENOMIC_INGESTIONS = "genomic_ingestions"
GENOMIC_SKIP_MISSING_FILETYPES = "genomic_skip_missing_file_types"
GENOMIC_GC_ID_MAPPING = "gc_name_to_id_mapping"
GENOMIC_PIPELINE_IDS = "genomic_pipeline_ids"
CALCULATE_READY_FLAG_LIMIT = 'calculate_ready_flag_limit'
DRC_BROAD_BUCKET_NAME = "drc_broad_bucket_name"
CO_BUCKET_NAME = "co_bucket_name"
UW_BUCKET_NAME = "uw_bucket_name"
BCM_BUCKET_NAME = "bcm_bucket_name"
DRC_BROAD_AW4_SUBFOLDERS = ("aw4_array_subfolder_name", "aw4_wgs_subfolder_name")
BIOBANK_AW2F_SUBFOLDER = "aw2f_manifests"
CONSENT_PDF_BUCKET = "consent_pdf_bucket"
USER_INFO = "user_info"
SYNC_SHARDS_PER_CHANNEL = "sync_shards_per_channel"
MEASUREMENTS_ENTITIES_PER_SYNC = "measurements_entities_per_sync"
BASELINE_PPI_QUESTIONNAIRE_FIELDS = "baseline_ppi_questionnaire_fields"
PPI_QUESTIONNAIRE_FIELDS = "ppi_questionnaire_fields"
BASELINE_SAMPLE_TEST_CODES = "baseline_sample_test_codes"
DNA_SAMPLE_TEST_CODES = "dna_sample_test_codes"
GHOST_ID_BUCKET = "ghost_id_bucket"
MAYOLINK_CREDS = "mayolink_creds"
JIRA_CREDS = "jira_creds"
READTHEDOCS_CREDS = "readthedocs_creds"
SENDGRID_KEY = "sendgrid_key"
SENDGRID_FROM_EMAIL = "no-reply@pmi-ops.org"
MAYOLINK_ENDPOINT = "mayolink_endpoint"
GENOMIC_API_MODES = ('gem', 'rhp', 'pgx', 'hdr')
GENOMIC_CVL_SITES = ('co', 'uw', 'bcm')
GENOMIC_CVL_SITE_BUCKET_MAP = 'cvl_site_bucket_map'
GENOMIC_CVL_SITE_PREFIX_MAP = 'cvl_site_prefix_map'
CONFIG_BUCKET = "all-of-us-rdr-sequestered-config-test"
EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT = "ehr_status_bigquery_view_participant"
EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION = "ehr_status_bigquery_view_organization"
HPO_REPORT_CONFIG_MIXIN_PATH = "hpo_report_config_mixin_path"
LOCALHOST_DEFAULT_BUCKET_NAME = 'local_bucket'
BIOBANK_SAMPLES_DAILY_INVENTORY_FILE_PATTERN = 'biobank_samples_daily_inventory_file_pattern'
BIOBANK_SAMPLES_MONTHLY_INVENTORY_FILE_PATTERN = 'biobank_samples_monthly_inventory_file_pattern'
BIOBANK_CUMULATIVE_RECEIVED_SCHEDULE = 'biobank_cumulative_received_schedule'
ENABLE_BIOBANK_MANIFEST_RECEIVED_FLAG = 'enable_biobank_manifest_received_flags'
COPE_FORM_ID_MAP = 'cope_form_id_map'
HPO_LITE_AWARDEE = 'hpo_lite_awardee'
HPO_LITE_REDCAP_PROJECT_TOKEN = 'hpo_lite_pairing_import_key'
HPO_LITE_ORG_NAME_MAPPING = 'hpo_lite_org_name_mapping'
DATA_BUCKET_SUBFOLDERS_PROD = 'data_bucket_subfolders_prod'
HEALTHPRO_CONSENT_BUCKET = 'hpro_consent_bucket'
HEALTHPRO_CONSENTS_TRANSFER_LIMIT = 'hpro_consents_transfer_limit'
CE_HEALTH_DATA_BUCKET_NAME = "ce_health_data_bucket_name"
VA_WORKQUEUE_BUCKET_NAME = 'va_workqueue_bucket_name'
VA_WORKQUEUE_SUBFOLDER = 'va_workqueue_subfolder'

# Buckets to listen for Pub/Sub notifications
PUBSUB_NOTIFICATION_BUCKETS_PROD = [
    "prod-genomics-baylor",
    "prod-genomics-color",
    "prod-genomics-broad",
    "prod-genomics-northwest",
    "prod-genomics-data-baylor",
    "prod-genomics-data-broad",
    "prod-genomics-data-color",
    "prod-genomics-data-northwest",
    "prod-drc-broad",
    "prod-color-rhp"
]

PUBSUB_NOTIFICATION_BUCKETS_STABLE = [
    "genomics-raw-baylor",
    "genomics-raw-color",
    "genomics-raw-northwest",
    "genomics-raw-broad",
    "genomics-baylor-dryrun",
    "genomics-color-dryrun",
    "genomics-northwest-dryrun",
    "genomics-broad-dryrun",
    "color-rhp-stable"
]

PUBSUB_NOTIFICATION_BUCKETS_SANDBOX = [
    "aou-rdr-sandbox-mock-data",
]

GENOMIC_INVESTIGATION_GENOME_TYPES = ['aou_array_investigation', 'aou_wgs_investigation']

# Questionnaire Codes
DNA_PROGRAM_CONSENT_UPDATE_CODE = 'dna_program_consent_update_code'

# For testing different account names locally
LOCAL_AUTH_USER = "example@example.com"

# Allow requests which are never permitted in production. These include fake
# timestamps for requests, unauthenticated requests to create fake data, etc.
ALLOW_NONPROD_REQUESTS = "allow_nonprod_requests"

# Settings for e-mail alerts for failed jobs.
INTERNAL_STATUS_MAIL_SENDER = "internal_status_email_sender"
INTERNAL_STATUS_MAIL_RECIPIENTS = "internal_status_email_recipients"
BIOBANK_STATUS_MAIL_RECIPIENTS = "biobank_status_mail_recipients"

REQUIRED_CONFIG_KEYS = [BIOBANK_SAMPLES_BUCKET_NAME]

DAYS_TO_DELETE_KEYS = "days_to_delete_keys"

# service accounts exception from key deletion
SERVICE_ACCOUNTS_WITH_LONG_LIVED_KEYS = "service_accounts_with_long_lived_keys"
DATA_OPS_SERVICE_ACCOUNTS_TO_MANAGE = "data_ops_service_accounts_to_manage"

CONSENT_SYNC_BUCKETS = "consent_sync_buckets"

DATA_DICTIONARY_DOCUMENT_ID = "data_dictionary_document_id"
BIOBANK_DATA_COMPARISON_DOCUMENT_ID = "biobank_data_comparison_document_id"

RDR_SLACK_WEBHOOKS = "rdr_slack_webhooks"
RDR_VALIDATION_WEBHOOK = "rdr_validation_webhook"

DECEASED_REPORT_FILTER_EXCEPTIONS = "deceased_report_filter_exceptions"

PTSC_SERVICE_DESK_EMAIL = "ptsc_service_desk_email"
RDR_GENOMICS_NOTIFICATION_EMAIL = "rdr_genomics_notification_email"

SENSITIVE_EHR_RELEASE_DATE = 'sensitive_ehr_release_date'

# Overrides for testing scenarios
CONFIG_OVERRIDES = {}


def override_setting(key, value):
    """Overrides a config setting. Used in tests."""
    CONFIG_OVERRIDES[key] = value


def store_current_config(config_json):
    store(CONFIG_SINGLETON_KEY, config_json)


def insert_config(key, value_list):
    """Updates a config key.  Used for tests"""
    config_data = load(CONFIG_SINGLETON_KEY)
    config_data[key] = value_list
    store(CONFIG_SINGLETON_KEY, config_data)


class MissingConfigException(Exception):
    """Exception raised if the setting does not exist"""


class InvalidConfigException(Exception):
    """Exception raised when the config setting is not in the expected form."""


class ConfigProvider(Provider, ABC):
    environment_variable_name = 'RDR_CONFIG_PROVIDER'

    @abstractmethod
    def load(self, name, date):
        pass

    @abstractmethod
    def store(self, name, config_dict):
        pass


class LocalFilesystemConfigProvider(ConfigProvider):
    DEFAULT_CONFIG_ROOT = os.path.join(os.path.dirname(__file__), '.configs')

    @classmethod
    def get_config_root(cls):
        return os.environ.get('RDR_CONFIG_ROOT', cls.DEFAULT_CONFIG_ROOT)

    def __init__(self):
        self._config_root = self.get_config_root()
        if not os.path.exists(self._config_root):
            os.mkdir(self._config_root)
        elif not os.path.isdir(self._config_root):
            raise NotADirectoryError('directory not found: {}'.format(self._config_root))

    def load(self, name=CONFIG_SINGLETON_KEY, date=None):
        config_path = os.path.join(self._config_root, '{}.json'.format(name))
        if os.path.exists(config_path):
            with open(config_path, 'r') as handle:
                return json.load(handle)
        return None

    def store(self, name, config_dict, **kwargs):  # pylint: disable=unused-argument
        config_path = os.path.join(self._config_root, '{}.json'.format(name))
        with open(config_path, 'w') as handle:
            json.dump(config_dict, handle)


class GoogleCloudDatastoreConfigProvider(ConfigProvider):

    def load(self, name=CONFIG_SINGLETON_KEY, date=None, project=None):
        datastore_client = datastore.Client(project=project if project else GAE_PROJECT)
        kind = 'Configuration'
        key = datastore_client.key(kind, name)
        if date is not None:
            history_query = (
                datastore_client.query(
                    kind='ConfigurationHistory',
                    order=['-date']
                )
                    .add_filter('ancestor', '=', key)
                    .add_filter('date', '<=', date)
                    .fetch(limit=1)
            )
            try:
                return next(iter(history_query)).obj
            except (StopIteration, AttributeError):
                return None
        entity = datastore_client.get(key=key)
        if entity:
            cdata = entity['configuration']
            if isinstance(cdata, (str, bytes)):
                try:
                    cdata = base64.b64decode(cdata).decode('utf-8')
                except UnicodeDecodeError:
                    # see if it was just a regular byte string and not encoded in base64.
                    cdata = cdata.decode('utf-8')
            config_data = json.loads(cdata)
        else:
            if name == CONFIG_SINGLETON_KEY:
                entity = datastore.Entity(key=key)
                entity['configuration'] = {}
                datastore_client.put(entity)
                config_data = entity['configuration']
            else:
                return None

        return config_data

    def store(self, name, config_dict, project=None, **kwargs):
        datastore_client = datastore.Client(project=project if project else GAE_PROJECT)
        date = clock.CLOCK.now()
        with datastore_client.transaction():
            key = datastore_client.key('Configuration', name)
            history_key = datastore_client.key('ConfigurationHistory', parent=key)
            entity = datastore_client.get(key)
            history_entity = datastore.Entity(key=history_key)
            history_entity['obj'] = entity
            history_entity['date'] = date
            for k, v in kwargs.items():
                history_entity[k] = v
            datastore_client.put(entity=history_entity)
            # https://stackoverflow.com/questions/56067244/encoding-a-string-to-base64-in-python-2-x-vs-python-3-x
            cdata = base64.b64encode(json.dumps(config_dict).encode()).decode()
            entity['configuration'] = cdata
            datastore_client.put(entity=entity)


def get_config_provider():
    # Set a good default and let the environment var be the override.
    if os.getenv('GAE_ENV', '').startswith('standard'):
        default_provider = GoogleCloudDatastoreConfigProvider
    else:
        default_provider = LocalFilesystemConfigProvider
    provider_class = ConfigProvider.get_provider(default=default_provider)
    return provider_class()


def load(name=CONFIG_SINGLETON_KEY, date=None):
    provider = get_config_provider()
    return provider.load(name=name, date=date)


def store(name, config_dict, **kwargs):
    provider = get_config_provider()
    provider.store(name, config_dict, **kwargs)
    singletons.invalidate(singletons.DB_CONFIG_INDEX)
    singletons.invalidate(singletons.MAIN_CONFIG_INDEX)
    return config_dict


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
        "Config key {} is a {} instead of a list".format(key, type(config_json))
    )


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
        raise InvalidConfigException("Config key {} has multiple entries in datastore.".format(key))
    return settings_list[0]


def get_db_config():
    config = singletons.get(
        singletons.DB_CONFIG_INDEX, lambda: load(DB_CONFIG_KEY), cache_ttl_seconds=CONFIG_CACHE_TTL_SECONDS
    )
    # allow overrides for unit testing.
    # pylint: disable=unused-variable
    for k, v in config.items():
        if k in CONFIG_OVERRIDES:
            config[k] = CONFIG_OVERRIDES[k]
    return config


def get_config():
    config = singletons.get(
        singletons.MAIN_CONFIG_INDEX, lambda: load(CONFIG_SINGLETON_KEY), cache_ttl_seconds=CONFIG_CACHE_TTL_SECONDS
    )
    return config or {}
