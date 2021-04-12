import json

from rdr_service.config import GoogleCloudDatastoreConfigProvider


class ConfigClient:
    def __init__(self, gcp_env):
        self.gcp_env = gcp_env

    def _get_config(self, config_root, config_key='current_config'):
        config_data_str, _ = self.gcp_env.get_latest_config_from_bucket(config_root, config_key)
        if not config_data_str:
            raise FileNotFoundError(f'Error: {config_root} configuration not found in bucket.')

        return json.loads(config_data_str)

    def get_server_config(self) -> dict:
        """
        Provides the config values from the server (including any values in the base config).
        :return: A dictionary representing the config for the environment.
        """
        base_config = self._get_config('base-config')
        proj_config = self._get_config(self.gcp_env.project)

        config = {**base_config, **proj_config}

        if self.gcp_env.project != 'localhost':
            # insert the geocode key from 'pmi-drc-api-test' into this config.
            cloud_datastore_provider = GoogleCloudDatastoreConfigProvider()
            geocode_config = cloud_datastore_provider.load('geocode_key', project='pmi-drc-api-test')
            if geocode_config:
                config['geocode_api_key'] = [geocode_config['api_key']]

        return config
