#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import base64
import datetime
import json
import os
import sys
from abc import ABC, abstractmethod
from json import JSONDecodeError

from google.cloud import datastore

from ..provider import Provider
from .gcp_utils import gcp_get_current_project
from .system_utils import JSONObject


# This the default datastore key
CONFIG_SINGLETON_KEY = "current_config"


class ConfigProvider(Provider, ABC):
    environment_variable_name = 'GCP_DATASTORE_PROVIDER'

    @abstractmethod
    def read(self, name, date):
        pass

    @abstractmethod
    def write(self, name, config_dict):
        pass


class LocalFilesystemDataStoreProvider(ConfigProvider):

    DEFAULT_LOCAL_GCP_DATASTORE_ROOT = os.path.join(os.path.dirname(sys.argv[0]), '.datastore')

    def __init__(self):
        self._config_root = os.environ.get('LOCAL_GCP_DATASTORE_ROOT', self.DEFAULT_LOCAL_GCP_DATASTORE_ROOT)
        if not os.path.exists(self._config_root):
            os.mkdir(self._config_root)
        elif not os.path.isdir(self._config_root):
            raise NotADirectoryError('directory not found: {}'.format(self._config_root))

    def read(self, name=CONFIG_SINGLETON_KEY, date=None):
        config_path = os.path.join(self._config_root, '{}.json'.format(name))
        if os.path.exists(config_path):
            with open(config_path, 'r') as handle:
                data = json.load(handle)
                return JSONObject(data)
        return None

    def write(self, name, config_dict, **kwargs):  # pylint: disable=unused-argument
        config_path = os.path.join(self._config_root, '{}.json'.format(name))
        with open(config_path, 'w') as handle:
            json.dump(config_dict, handle)


class GoogleCloudDatastoreConfigProvider(ConfigProvider):

    def read(self, name=CONFIG_SINGLETON_KEY, date=None, project=None):

        datastore_client = datastore.Client(project=project if project else gcp_get_current_project())
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

        return JSONObject(config_data)

    def write(self, name, config_dict, project=None, **kwargs):
        datastore_client = datastore.Client(project=project if project else gcp_get_current_project())
        date = datetime.datetime.utcnow()
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
    """
    Note: To override the default provider, set the GCP_DATASTORE_PROVIDER value to
          "GoogleCloudDatastoreConfigProvider".
    """
    # Set a good default and let the environment var be the override.
    if os.getenv('GAE_ENV', '').startswith('standard'):
        default_provider = GoogleCloudDatastoreConfigProvider
    else:
        default_provider = LocalFilesystemDataStoreProvider
    provider_class = ConfigProvider.get_provider(default=default_provider)
    return provider_class()
