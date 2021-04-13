import json
import mock

from rdr_service.services.config_client import ConfigClient
from tests.helpers.unittest_base import BaseTestCase


class ConfigClientTest(BaseTestCase):
    def setUp(self, **kwargs) -> None:
        super(ConfigClientTest, self).setUp(**kwargs)

        self.test_environment_name = 'localhost'

        self.mock_gcp_env = mock.MagicMock()
        self.mock_gcp_env.project = self.test_environment_name
        self.config_client = ConfigClient(self.mock_gcp_env)

    def _mock_server_config_data(self, base_config_data_str, test_config_data_str):
        def get_config_data(config_root, _):
            if config_root == self.test_environment_name:
                return test_config_data_str, config_root
            elif config_root == 'base-config':
                return base_config_data_str, config_root

            raise Exception(f'Unexpected config root "{config_root}"')

        self.mock_gcp_env.get_latest_config_from_bucket.side_effect = get_config_data

    def test_config_data_retrieved_for_environment(self):
        """
        Test that the client builds config data from the base config file
        as well as the file for the specific environment.
        """
        self._mock_server_config_data(
            base_config_data_str=json.dumps({
                'base_setting': 'default'
            }),
            test_config_data_str=json.dumps({
                'test_key': 'test_value'
            })
        )

        test_config = self.config_client.get_server_config()
        self.assertEqual({
            'base_setting': 'default',
            'test_key': 'test_value'
        }, test_config)

    def test_environment_settings_override_base(self):
        """Verify that the environment configs override values from the base file"""
        config_key = 'config_key_to_override'
        self._mock_server_config_data(
            base_config_data_str=json.dumps({
                config_key: 'default'
            }),
            test_config_data_str=json.dumps({
                config_key: 'new_value'
            })
        )

        test_config = self.config_client.get_server_config()
        self.assertEqual({
            config_key: 'new_value'
        }, test_config)
