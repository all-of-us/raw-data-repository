from datetime import datetime, timedelta
import mock

from rdr_service.offline.service_accounts import ServiceAccount, ServiceAccountKey, ServiceAccountKeyManager
from rdr_service.services.gcp_config import RdrEnvironment
from tests.helpers.unittest_base import BaseTestCase


class ServiceAccountManagerTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super(ServiceAccountManagerTest, self).setUp(*args, **kwargs)

        # Mock google discover
        patcher = mock.patch('rdr_service.offline.service_accounts.discovery')
        self.mock_discovery = patcher.start()
        self.addCleanup(patcher.stop)

        self.mock_project_sa_call = (self.mock_discovery.build.return_value.projects.
                                     return_value.serviceAccounts.return_value)
        self.mock_account_list = self.mock_project_sa_call.list
        self.mock_key_list = self.mock_project_sa_call.keys.return_value.list

        self.service_account_manager = ServiceAccountKeyManager()

    def test_listing_accounts(self):
        project_name = 'test_project'

        self._mock_service_accounts([
                {'email': 'one@test.com'},
                {'email': 'two@test.com'}
        ])

        self.assertEqual([
            ServiceAccount(email='one@test.com'),
            ServiceAccount(email='two@test.com')
        ], self.service_account_manager._get_service_accounts_for_project(project_name))
        self.mock_account_list.assert_called_with(name=f'projects/{project_name}')

    def test_listing_keys(self):
        project_name = 'test_project'
        service_account_name = 'test_account'

        self._mock_keys([
                {'name': 'one', 'validAfterTime': '2021-05-01T15:09:32Z'},
                {'name': 'two', 'validAfterTime': '2020-09-17T09:10:11Z'}
        ])

        self.assertEqual([
            ServiceAccountKey(name='one', start_date=datetime(2021, 5, 1, 15, 9, 32)),
            ServiceAccountKey(name='two', start_date=datetime(2020, 9, 17, 9, 10, 11))
        ], self.service_account_manager._get_keys_for_account(project_name, service_account_name))
        self.mock_key_list.assert_called_with(
            name=f'projects/{project_name}/serviceAccounts/{service_account_name}',
            keyTypes='USER_MANAGED'
        )

    def test_delete_old_keys(self):
        with mock.patch.object(self.service_account_manager, '_get_service_accounts_for_project') as mock_get_accounts,\
                mock.patch.object(self.service_account_manager, '_get_keys_for_account') as mock_get_keys:
            mock_get_accounts.return_value = [ServiceAccount(email='test')]
            mock_get_keys.return_value = [
                ServiceAccountKey(name='delete_this', start_date=datetime.now() - timedelta(days=4))
            ]

            self.service_account_manager.expire_old_keys()
            self.assertKeyDeleted(key_name='delete_this')

    def test_keep_newer_keys(self):
        with mock.patch.object(self.service_account_manager, '_get_service_accounts_for_project') as mock_get_accounts,\
                mock.patch.object(self.service_account_manager, '_get_keys_for_account') as mock_get_keys:
            mock_get_accounts.return_value = [ServiceAccount(email='test')]
            mock_get_keys.return_value = [
                ServiceAccountKey(name='do_not_delete', start_date=datetime.now() - timedelta(days=1))
            ]

            self.service_account_manager.expire_old_keys()
            self.assertNoKeysDeleted()

    def test_ignore_long_lived_accounts(self):
        with mock.patch.object(self.service_account_manager, '_get_service_accounts_for_project') as mock_get_accounts,\
                mock.patch.object(self.service_account_manager, '_get_keys_for_account') as mock_get_keys:
            long_lived_account_email = 'long@lived'
            mock_get_accounts.return_value = [ServiceAccount(email=long_lived_account_email)]
            mock_get_keys.return_value = [
                ServiceAccountKey(name='do_not_delete', start_date=datetime.now() - timedelta(days=100))
            ]
            self.service_account_manager._service_accounts_with_long_lived_keys = [long_lived_account_email]

            self.service_account_manager.expire_old_keys()
            self.assertNoKeysDeleted()

    def test_expire_keys_for_ops_project(self):
        """Check that Prod data-ops accounts are managed when they appear in the list of managed accounts"""
        self.service_account_manager._app_id = RdrEnvironment.PROD.value

        with mock.patch.object(self.service_account_manager, '_get_service_accounts_for_project') as get_accounts_mock,\
                mock.patch.object(self.service_account_manager, '_get_keys_for_account') as mock_get_keys:
            managed_account = 'test@managed.com'
            self._mock_accounts_for_project(get_accounts_mock, 'all-of-us-ops-data-api-prod', [
                ServiceAccount(email=managed_account)
            ])
            mock_get_keys.return_value = [
                ServiceAccountKey(name='delete_this', start_date=datetime.now() - timedelta(days=100))
            ]
            self.service_account_manager._managed_data_ops_accounts = [managed_account]

            self.service_account_manager.expire_old_keys()
            self.assertKeyDeleted(key_name='delete_this')

    def test_expire_only_managed_ops_accounts(self):
        """Make sure that only keys for accounts in the managed account list get expired for the data ops project"""
        self.service_account_manager._app_id = RdrEnvironment.PROD.value

        with mock.patch.object(self.service_account_manager, '_get_service_accounts_for_project') as get_accounts_mock,\
                mock.patch.object(self.service_account_manager, '_get_keys_for_account') as mock_get_keys:
            self._mock_accounts_for_project(get_accounts_mock, 'all-of-us-ops-data-api-prod', [
                ServiceAccount(email='not_managed@test.com')
            ])
            mock_get_keys.return_value = [
                ServiceAccountKey(name='do_not_delete', start_date=datetime.now() - timedelta(days=100))
            ]

            self.service_account_manager.expire_old_keys()
            self.assertNoKeysDeleted()

    def _mock_accounts_for_project(self, get_accounts_mock, project_name, accounts):
        def get_accounts_for_project(project_name_requested):
            return accounts if project_name_requested == project_name else []

        get_accounts_mock.side_effect = get_accounts_for_project

    def _mock_service_accounts(self, service_accounts):
        self.mock_account_list.return_value.execute.return_value = {
            'accounts': service_accounts
        }

    def _mock_keys(self, keys):
        self.mock_key_list.return_value.execute.return_value = {
            'keys': keys
        }

    def assertKeyDeleted(self, key_name):
        mock_delete_request_builder = self.mock_project_sa_call.keys.return_value.delete
        mock_delete_request_builder.assert_any_call(name=key_name)
        mock_delete_request_builder.return_value.execute.assert_called()

    def assertNoKeysDeleted(self):
        mock_delete_request_builder = self.mock_project_sa_call.keys.return_value.delete
        mock_delete_request_builder.assert_not_called()
