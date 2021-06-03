from dataclasses import dataclass
from datetime import datetime
from googleapiclient import discovery
import logging
from typing import List

from rdr_service import config
from rdr_service.config import GAE_PROJECT


@dataclass
class ServiceAccount:
    email: str


@dataclass
class ServiceAccountKey:
    name: str
    start_date: datetime

    def get_key_age_in_days(self):
        return (datetime.utcnow() - self.start_date).days


class ServiceAccountKeyManager:
    def __init__(self):
        self._app_id = GAE_PROJECT
        self._google_service = discovery.build("iam", "v1", cache_discovery=False)
        self._max_age_in_days = config.getSetting(config.DAYS_TO_DELETE_KEYS)
        self._service_accounts_with_long_lived_keys = config.getSettingList(
            config.SERVICE_ACCOUNTS_WITH_LONG_LIVED_KEYS, default=[]
        )

    def expire_old_keys(self):
        """Deletes service account keys older than 3 days as required by NIH"""

        if self._app_id is None:
            raise Exception('Unable to determine current project')

        self._expire_keys_for_project(
            project_name=self._app_id,
            ignore_service_account_func=lambda account: account.email in self._service_accounts_with_long_lived_keys
        )

    def _expire_keys_for_project(self, project_name, ignore_service_account_func):
        for service_account in self._get_service_accounts_for_project(project_name):
            if ignore_service_account_func(service_account.email):
                logging.info("Skip key expiration check for Service Account {}".format(service_account.email))
            else:
                for key in self._get_keys_for_account(
                    project_name=project_name,
                    service_account_name=service_account.email
                ):
                    key_age_days = key.get_key_age_in_days()
                    if key_age_days >= self._max_age_in_days:
                        logging.warning(
                            f"Deleting service account key older than {self._max_age_in_days} "
                            f"[{key_age_days}]: {key.name}"
                        )
                        self._delete_key(key)
                    else:
                        logging.info(f'Service Account key is {key_age_days} days old: {key.name}')

    def _get_service_accounts_for_project(self, project_name) -> List[ServiceAccount]:
        account_list_request = self._google_service.projects().serviceAccounts().list(name=f'projects/{project_name}')
        account_list_response = account_list_request.execute()
        service_accounts = [
            ServiceAccount(
                email=account['email']
            )
            for account in account_list_response.get('accounts', [])
        ]

        if not service_accounts:
            logging.info(f'No Service Accounts found in project "{project_name}"')

        return service_accounts

    def _get_keys_for_account(self, project_name, service_account_name) -> List[ServiceAccountKey]:
        key_list_request = self._google_service.projects().serviceAccounts().keys().list(
            name=f'projects/{project_name}/serviceAccounts/{service_account_name}',
            keyTypes='USER_MANAGED'
        )
        key_list_response = key_list_request.execute()
        return [
            ServiceAccountKey(
                name=key['name'],
                start_date=datetime.strptime(key["validAfterTime"], "%Y-%m-%dT%H:%M:%SZ")
            )
            for key in key_list_response.get('keys', [])
        ]

    def _delete_key(self, key: ServiceAccountKey):
        delete_request = self._google_service.projects().serviceAccount().keys().delete(name=key.name)
        delete_request.execute()
