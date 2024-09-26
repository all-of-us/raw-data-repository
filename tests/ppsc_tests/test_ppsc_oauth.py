from dataclasses import dataclass
from unittest import mock

from rdr_service.dao.ppsc_dao import PPSCDataTransferAuthDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.ppsc.ppsc_enums import AuthType
from rdr_service.ppsc.ppsc_oauth import PPSCTransferOauth
from tests.helpers.unittest_base import BaseTestCase


@dataclass
class MockedTransferResponse:
    status_code: int = 200

    @staticmethod
    def json():
        return {
            'access_token': 'eweweweweww',
            'expires_in': 3600
        }


class PPSCDataTransferTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        self.oauth_dao = PPSCDataTransferAuthDao()

    def build_oauth_data(self):

        oauth = {
            'auth_type': AuthType.DATA_TRANSFER,
            'auth_url': 'test_url',
            'client_id': 'wqwqwqwqqw1',
            'client_secret': 'wqwqwqqwqqwqwqwqqwqwqwq'
        }

        self.oauth_dao.insert(self.oauth_dao.model_type(**oauth))

    def test_oauth_key_record_stores(self):
        self.build_oauth_data()
        requests_api_patcher = mock.patch(
            "rdr_service.ppsc.ppsc_oauth.requests",
            **{"post.return_value": MockedTransferResponse()}
        )
        requests_api_patcher.start()

        ppsc_transfer_oauth = PPSCTransferOauth(auth_type=AuthType.DATA_TRANSFER)

        self.assertIsNotNone(ppsc_transfer_oauth.token)
        self.assertEqual(ppsc_transfer_oauth.token, MockedTransferResponse.json().get('access_token'))

        current_oauth_record = self.oauth_dao.get_all()
        self.assertEqual(len(current_oauth_record), 1)
        self.assertEqual(current_oauth_record[0].access_token, MockedTransferResponse.json().get('access_token'))
        self.assertEqual(current_oauth_record[0].expires, str(MockedTransferResponse.json().get('expires_in')))

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("ppsc.ppsc_data_transfer_auth")
