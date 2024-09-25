from dataclasses import dataclass
from unittest import mock

from faker import Faker

from rdr_service import clock
from rdr_service.dao.ppsc_dao import PPSCDataTransferAuthDao, PPSCDataTransferEndpointDao, PPSCDataTransferRecordDao, \
    ParticipantDao
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.ppsc.ppsc_data_transfer import PPSCDataTransferCore, PPSCDataTransferEHR, PPSCDataTransferHealthData, \
    PPSCDataTransferBiobank
from rdr_service.ppsc.ppsc_enums import DataSyncTransferType, AuthType
from tests.helpers.unittest_base import BaseTestCase


@dataclass
class MockedTransferResponse:
    status_code: int = 200


class PPSCDataTransferTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        self.participant_dao = ParticipantDao()
        self.oauth_dao = PPSCDataTransferAuthDao()
        self.endpoint_dao = PPSCDataTransferEndpointDao()
        self.transfer_record_dao = PPSCDataTransferRecordDao()
        self.faker = Faker()
        self.base_url = 'https://ppsc_base_url.com/'

        self.build_oauth_data()
        self.build_endpoint_data()

        self.current_endpoint_records = self.endpoint_dao.get_all()

    def build_oauth_data(self):

        oauth = {
            'auth_type': AuthType.DATA_TRANSFER,
            'auth_url': 'test_url',
            'client_id': 'wqwqwqwqqw1',
            'client_secret': 'wqwqwqqwqqwqwqwqqwqwqwq'
        }

        self.oauth_dao.insert(self.oauth_dao.model_type(**oauth))

    def build_endpoint_data(self) -> None:

        endpoint_types = [
            DataSyncTransferType.CORE,
            DataSyncTransferType.EHR,
            DataSyncTransferType.BIOBANK_SAMPLE,
            DataSyncTransferType.HEALTH_DATA
        ]

        for endpoint_type in endpoint_types:
            self.ppsc_data_gen.create_database_ppsc_data_sync_endpoint(
                data_sync_transfer_type=endpoint_type,
                endpoint=''.join(self.faker.random_letters(length=128)),
                base_url=self.base_url
            )

    @mock.patch('rdr_service.ppsc.ppsc_oauth.PPSCTransferOauth.generate_token')
    @mock.patch('rdr_service.ppsc.ppsc_data_transfer.PPSCDataTransferCore.send_item')
    def test_send_core_items_for_transfer(self, send_request, oauth_service) -> None:

        oauth_service.return_value = 'wqwqwqwqqwqqwqwqwqwqw'
        send_request.return_value = MockedTransferResponse()

        for _ in range(0, 3):
            participant = self.ppsc_data_gen.create_database_participant()
            self.ppsc_data_gen.create_database_ppsc_data_core(
                participant_id=participant.id,
                has_core_data=1,
                has_core_data_date_time=clock.CLOCK.now()
            )

        with PPSCDataTransferCore() as core_transfer:
            core_transfer.run_data_transfer()

        # contructor/__enter__ builds correctly
        self.assertEqual(core_transfer.ppsc_oauth_data.token, oauth_service.return_value)
        self.assertEqual(core_transfer.transfer_type, DataSyncTransferType.CORE)

        current_endpoint = [obj for obj in self.current_endpoint_records
                            if obj.data_sync_transfer_type == DataSyncTransferType.CORE]
        self.assertEqual(len(current_endpoint), 1)

        current_endpoint = current_endpoint[0]
        self.assertEqual(core_transfer.transfer_url, f'{self.base_url}{current_endpoint.endpoint}')

        self.assertEqual(len(core_transfer.transfer_items), 3)

        current_transfer_records = self.transfer_record_dao.get_all()

        self.assertEqual(len(current_transfer_records), len(core_transfer.transfer_items))

        current_participant_ids = [obj.id for obj in self.participant_dao.get_all()]
        self.assertTrue(all(obj.participant_id in current_participant_ids for obj in current_transfer_records))
        self.assertTrue(all(obj.data_sync_transfer_type == DataSyncTransferType.CORE
                            for obj in current_transfer_records))
        self.assertTrue(all(obj.request_payload is not None for obj in current_transfer_records))
        self.assertTrue(all(obj.response_code == '200' for obj in current_transfer_records))

        # test second run same data should not find items for transfer
        with PPSCDataTransferCore() as core_transfer:
            core_transfer.run_data_transfer()

        self.assertEqual(len(core_transfer.transfer_items), 0)

    @mock.patch('rdr_service.ppsc.ppsc_oauth.PPSCTransferOauth.generate_token')
    @mock.patch('rdr_service.ppsc.ppsc_data_transfer.PPSCDataTransferEHR.send_item')
    def test_send_ehr_items_for_transfer(self, send_request, oauth_service) -> None:

        oauth_service.return_value = 'wqwqwqwqqwqqwqwqwqwqw'
        send_request.return_value = MockedTransferResponse()

        for _ in range(0, 3):
            participant = self.ppsc_data_gen.create_database_participant()
            self.ppsc_data_gen.create_database_ppsc_data_ehr(
                participant_id=participant.id,
                first_time_date_time=clock.CLOCK.now(),
                last_time_date_time=clock.CLOCK.now(),
            )

        with PPSCDataTransferEHR() as ehr_transfer:
            ehr_transfer.run_data_transfer()

        # contructor/__enter__ builds correctly
        self.assertEqual(ehr_transfer.ppsc_oauth_data.token, oauth_service.return_value)
        self.assertEqual(ehr_transfer.transfer_type, DataSyncTransferType.EHR)

        current_endpoint = [obj for obj in self.current_endpoint_records
                            if obj.data_sync_transfer_type == DataSyncTransferType.EHR]
        self.assertEqual(len(current_endpoint), 1)

        current_endpoint = current_endpoint[0]
        self.assertEqual(ehr_transfer.transfer_url, f'{self.base_url}{current_endpoint.endpoint}')

        self.assertEqual(len(ehr_transfer.transfer_items), 3)

        current_transfer_records = self.transfer_record_dao.get_all()

        self.assertEqual(len(current_transfer_records), len(ehr_transfer.transfer_items))

        current_participant_ids = [obj.id for obj in self.participant_dao.get_all()]
        self.assertTrue(all(obj.participant_id in current_participant_ids for obj in current_transfer_records))
        self.assertTrue(all(obj.data_sync_transfer_type == DataSyncTransferType.EHR
                            for obj in current_transfer_records))
        self.assertTrue(all(obj.request_payload is not None for obj in current_transfer_records))
        self.assertTrue(all(obj.response_code == '200' for obj in current_transfer_records))

        # test second run same data should not find items for transfer
        with PPSCDataTransferEHR() as ehr_transfer:
            ehr_transfer.run_data_transfer()

        self.assertEqual(len(ehr_transfer.transfer_items), 0)

    @mock.patch('rdr_service.ppsc.ppsc_oauth.PPSCTransferOauth.generate_token')
    @mock.patch('rdr_service.ppsc.ppsc_data_transfer.PPSCDataTransferHealthData.send_item')
    def test_send_health_data_items_for_transfer(self, send_request, oauth_service) -> None:

        oauth_service.return_value = 'wqwqwqwqqwqqwqwqwqwqw'
        send_request.return_value = MockedTransferResponse()

        for _ in range(0, 3):
            participant = self.ppsc_data_gen.create_database_participant()
            self.ppsc_data_gen.create_database_ppsc_data_health_data(
                participant_id=participant.id,
                health_data_stream_sharing_status=2,
                health_data_stream_sharing_status_date_time=clock.CLOCK.now()
            )

        with PPSCDataTransferHealthData() as health_transfer:
            health_transfer.run_data_transfer()

        # contructor/__enter__ builds correctly
        self.assertEqual(health_transfer.ppsc_oauth_data.token, oauth_service.return_value)
        self.assertEqual(health_transfer.transfer_type, DataSyncTransferType.HEALTH_DATA)

        current_endpoint = [obj for obj in self.current_endpoint_records
                            if obj.data_sync_transfer_type == DataSyncTransferType.HEALTH_DATA]
        self.assertEqual(len(current_endpoint), 1)

        current_endpoint = current_endpoint[0]
        self.assertEqual(health_transfer.transfer_url, f'{self.base_url}{current_endpoint.endpoint}')

        self.assertEqual(len(health_transfer.transfer_items), 3)

        current_transfer_records = self.transfer_record_dao.get_all()

        self.assertEqual(len(current_transfer_records), len(health_transfer.transfer_items))

        current_participant_ids = [obj.id for obj in self.participant_dao.get_all()]
        self.assertTrue(all(obj.participant_id in current_participant_ids for obj in current_transfer_records))
        self.assertTrue(all(obj.data_sync_transfer_type == DataSyncTransferType.HEALTH_DATA
                            for obj in current_transfer_records))
        self.assertTrue(all(obj.request_payload is not None for obj in current_transfer_records))
        self.assertTrue(all(obj.response_code == '200' for obj in current_transfer_records))

        # test second run same data should not find items for transfer
        with PPSCDataTransferHealthData() as health_transfer:
            health_transfer.run_data_transfer()

        self.assertEqual(len(health_transfer.transfer_items), 0)

    @mock.patch('rdr_service.ppsc.ppsc_oauth.PPSCTransferOauth.generate_token')
    @mock.patch('rdr_service.ppsc.ppsc_data_transfer.PPSCDataTransferBiobank.send_item')
    def test_send_biobank_sample_items_for_transfer(self, send_request, oauth_service) -> None:

        oauth_service.return_value = 'wqwqwqwqqwqqwqwqwqwqw'
        send_request.return_value = MockedTransferResponse()

        for _ in range(0, 3):
            participant = self.ppsc_data_gen.create_database_participant()
            self.ppsc_data_gen.create_database_ppsc_data_biobank(
                participant_id=participant.id,
                first_time_date_time=clock.CLOCK.now(),
                last_time_date_time=clock.CLOCK.now(),
        )

        with PPSCDataTransferBiobank() as biobank_transfer:
            biobank_transfer.run_data_transfer()

        # contructor/__enter__ builds correctly
        self.assertEqual(biobank_transfer.ppsc_oauth_data.token, oauth_service.return_value)
        self.assertEqual(biobank_transfer.transfer_type, DataSyncTransferType.BIOBANK_SAMPLE)

        current_endpoint = [obj for obj in self.current_endpoint_records
                            if obj.data_sync_transfer_type == DataSyncTransferType.BIOBANK_SAMPLE]
        self.assertEqual(len(current_endpoint), 1)

        current_endpoint = current_endpoint[0]
        self.assertEqual(biobank_transfer.transfer_url, f'{self.base_url}{current_endpoint.endpoint}')

        self.assertEqual(len(biobank_transfer.transfer_items), 3)

        current_transfer_records = self.transfer_record_dao.get_all()

        self.assertEqual(len(current_transfer_records), len(biobank_transfer.transfer_items))

        current_participant_ids = [obj.id for obj in self.participant_dao.get_all()]
        self.assertTrue(all(obj.participant_id in current_participant_ids for obj in current_transfer_records))
        self.assertTrue(all(obj.data_sync_transfer_type == DataSyncTransferType.BIOBANK_SAMPLE
                            for obj in current_transfer_records))
        self.assertTrue(all(obj.request_payload is not None for obj in current_transfer_records))
        self.assertTrue(all(obj.response_code == '200' for obj in current_transfer_records))

        # test second run same data should not find items for transfer
        with PPSCDataTransferBiobank() as biobank_transfer:
            biobank_transfer.run_data_transfer()

        self.assertEqual(len(biobank_transfer.transfer_items), 0)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("ppsc.participant")
        self.clear_table_after_test("ppsc.ppsc_data_transfer_auth")
        self.clear_table_after_test("ppsc.ppsc_data_transfer_endpoint")
        self.clear_table_after_test("ppsc.ppsc_data_transfer_record")
        self.clear_table_after_test("ppsc.ppsc_core")
        self.clear_table_after_test("ppsc.ppsc_ehr")
        self.clear_table_after_test("ppsc.ppsc_biobank_sample")
        self.clear_table_after_test("ppsc.ppsc_health_data")
