from dataclasses import dataclass
from unittest import mock

from faker import Faker

from rdr_service import clock
from rdr_service.dao.ppsc_dao import ParticipantDao
from rdr_service.dao.ppsc_partner_transfer_dao import PPSCDataTransferAuthDao, PPSCDataTransferEndpointDao, \
    PPSCDataTransferRecordDao, RTIDataTransferAuthDao, RTIDataTransferEndpointDao, RTIDataTransferRecordDao
from rdr_service.data_gen.generators.nph import NphDataGenerator
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.ppsc.ppsc_partner_data_transfer import PPSCDataTransferCore, PPSCDataTransferEHR, \
    PPSCDataTransferHealthData, \
    PPSCDataTransferBiobank, RTIDataTransferNPHOptIn
from rdr_service.ppsc.ppsc_enums import DataSyncTransferType, AuthType, SpecimenType, SpecimenStatus
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
            'auth_type': AuthType.PPSC_DATA_TRANSFER,
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

    @mock.patch('rdr_service.ppsc.ppsc_partner_oauth.PPSCTransferOauth.generate_token')
    @mock.patch('rdr_service.ppsc.ppsc_partner_data_transfer.PPSCDataTransferCore.send_item')
    def test_send_core_items_for_transfer(self, send_request, oauth_service) -> None:

        oauth_service.return_value = {
            'last_generated': clock.CLOCK.now(),
            'expires': '3600',
            'access_token': 'wqwqwqqwqwqwqw'
        }
        send_request.return_value = MockedTransferResponse()

        for _ in range(0, 3):
            participant = self.ppsc_data_gen.create_database_participant()
            self.ppsc_data_gen.create_database_ppsc_data_core(
                participant_id=participant.id,
                has_core_data=1,
                event_date_time=clock.CLOCK.now()
            )

        with PPSCDataTransferCore() as core_transfer:
            core_transfer.run_data_transfer()

        # contructor/__enter__ builds correctly
        self.assertEqual(core_transfer.ppsc_oauth_data.token_data , oauth_service.return_value)
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

    @mock.patch('rdr_service.ppsc.ppsc_partner_oauth.PPSCTransferOauth.generate_token')
    @mock.patch('rdr_service.ppsc.ppsc_partner_data_transfer.PPSCDataTransferEHR.send_item')
    def test_send_ehr_items_for_transfer(self, send_request, oauth_service) -> None:

        oauth_service.return_value = {
            'last_generated': clock.CLOCK.now(),
            'expires': '3600',
            'access_token': 'wqwqwqqwqwqwqw'
        }
        send_request.return_value = MockedTransferResponse()

        for _ in range(0, 3):
            participant = self.ppsc_data_gen.create_database_participant()
            self.ppsc_data_gen.create_database_ppsc_data_ehr(
                participant_id=participant.id,
                event_date_time=clock.CLOCK.now()
            )

        with PPSCDataTransferEHR() as ehr_transfer:
            ehr_transfer.run_data_transfer()

        # contructor/__enter__ builds correctly
        self.assertEqual(ehr_transfer.ppsc_oauth_data.token_data, oauth_service.return_value)
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

    @mock.patch('rdr_service.ppsc.ppsc_partner_oauth.PPSCTransferOauth.generate_token')
    @mock.patch('rdr_service.ppsc.ppsc_partner_data_transfer.PPSCDataTransferHealthData.send_item')
    def test_send_health_data_items_for_transfer(self, send_request, oauth_service) -> None:

        oauth_service.return_value = {
            'last_generated': clock.CLOCK.now(),
            'expires': '3600',
            'access_token': 'wqwqwqqwqwqwqw'
        }
        send_request.return_value = MockedTransferResponse()

        for _ in range(0, 3):
            participant = self.ppsc_data_gen.create_database_participant()
            self.ppsc_data_gen.create_database_ppsc_data_health_data(
                participant_id=participant.id,
                health_data_stream_sharing_status=2,
                event_date_time=clock.CLOCK.now()
            )

        with PPSCDataTransferHealthData() as health_transfer:
            health_transfer.run_data_transfer()

        # contructor/__enter__ builds correctly
        self.assertEqual(health_transfer.ppsc_oauth_data.token_data, oauth_service.return_value)
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

    @mock.patch('rdr_service.ppsc.ppsc_partner_oauth.PPSCTransferOauth.generate_token')
    @mock.patch('rdr_service.ppsc.ppsc_partner_data_transfer.PPSCDataTransferBiobank.send_item')
    def test_send_biobank_sample_items_for_transfer(self, send_request, oauth_service) -> None:

        oauth_service.return_value = {
            'last_generated': clock.CLOCK.now(),
            'expires': '3600',
            'access_token': 'wqwqwqqwqwqwqw'
        }
        send_request.return_value = MockedTransferResponse()

        for _ in range(0, 3):
            participant = self.ppsc_data_gen.create_database_participant()
            self.ppsc_data_gen.create_database_ppsc_data_biobank(
                participant_id=participant.id,
                event_date_time=clock.CLOCK.now(),
                specimen_type=SpecimenType.BLOOD,
                specimen_status=SpecimenStatus.RECEIVED
        )

        with PPSCDataTransferBiobank() as biobank_transfer:
            biobank_transfer.run_data_transfer()

        # contructor/__enter__ builds correctly
        self.assertEqual(biobank_transfer.ppsc_oauth_data.token_data, oauth_service.return_value)
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

        # test second run new sample should send
        for _ in range(0, 1):
            self.ppsc_data_gen.create_database_ppsc_data_biobank(
                participant_id=current_participant_ids[0],
                event_date_time=clock.CLOCK.now(),
                specimen_type=SpecimenType.URINE,
                specimen_status=SpecimenStatus.RECEIVED
        )

        # test second run same data should only find new urine sample
        with PPSCDataTransferBiobank() as biobank_transfer:
            biobank_transfer.run_data_transfer()

        self.assertEqual(len(biobank_transfer.transfer_items), 1)
        self.assertEqual(biobank_transfer.transfer_items[0].participant_id, current_participant_ids[0])
        self.assertEqual(biobank_transfer.transfer_items[0].specimen_type, SpecimenType.URINE)

        # test third run same data should not find items for transfer
        with PPSCDataTransferBiobank() as biobank_transfer:
            biobank_transfer.run_data_transfer()

        self.assertEqual(len(biobank_transfer.transfer_items), 0)

    @mock.patch('rdr_service.ppsc.ppsc_partner_oauth.PPSCTransferOauth.generate_token')
    @mock.patch('rdr_service.ppsc.ppsc_partner_data_transfer.PPSCDataTransferBiobank.send_item')
    def test_send_sample_items_and_token_updates(self, send_request, oauth_service) -> None:

        oauth_service.return_value = {
            'last_generated': clock.CLOCK.now(),
            'expires': '0',  # set expire value so token will refresh accordingly
            'access_token': 'wqwqwqqwqwqwqw'
        }
        send_request.return_value = MockedTransferResponse()

        for _ in range(0, 3):
            participant = self.ppsc_data_gen.create_database_participant()
            self.ppsc_data_gen.create_database_ppsc_data_biobank(
                participant_id=participant.id,
                event_date_time=clock.CLOCK.now(),
                specimen_type=SpecimenType.BLOOD,
                specimen_status=SpecimenStatus.RECEIVED
        )

        with PPSCDataTransferBiobank() as biobank_transfer:
            # check current mocked token and headers
            self.assertEqual(biobank_transfer.ppsc_oauth_data.token_data, oauth_service.return_value)
            current_headers = {
                "Content-Type": "application/json",
                "Authorization": f'Bearer {oauth_service.return_value.get("access_token")}'
            }
            self.assertEqual(biobank_transfer.headers, current_headers)

            # set new token via updated return value
            oauth_service.return_value = {
                'last_generated': clock.CLOCK.now(),
                'expires': '3600',
                'access_token': 'ddddddddddddd'
            }
            biobank_transfer.run_data_transfer()

        # check updated headers updated with new token
        self.assertNotEqual(current_headers, biobank_transfer.headers)
        # check token data updated with updated mock value
        self.assertEqual(biobank_transfer.ppsc_oauth_data.token_data, oauth_service.return_value)

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


class RTIDataTransferTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        self.nph_datagen = NphDataGenerator()

        self.participant_dao = ParticipantDao()
        self.oauth_dao = RTIDataTransferAuthDao()
        self.endpoint_dao = RTIDataTransferEndpointDao()
        self.transfer_record_dao = RTIDataTransferRecordDao()
        self.faker = Faker()
        self.base_url = 'https://rti_base_url.com/'

        self.build_oauth_data()
        self.build_endpoint_data()

        self.current_endpoint_records = self.endpoint_dao.get_all()

    def build_oauth_data(self):

        oauth = {
            'auth_type': AuthType.RTI_DATA_TRANSFER,
            'auth_url': 'test_url',
            'x_public_key': 'wqwqwwqwqqwqwqwqqw',
            # RTI token for now is generated manually, no AUTH API available
            'access_token': 'wqwqwqwqwqwqwqqwqwqwqwqwqwqwwqqwqwqwqwqwqw'
        }

        self.oauth_dao.insert(self.oauth_dao.model_type(**oauth))

    def build_endpoint_data(self) -> None:

        endpoint_types = [
            DataSyncTransferType.NPH_OPT_IN,
        ]

        for endpoint_type in endpoint_types:
            self.ppsc_data_gen.create_database_rti_data_sync_endpoint(
                data_sync_transfer_type=endpoint_type,
                endpoint=''.join(self.faker.random_letters(length=128)),
                base_url=self.base_url
            )

    @mock.patch('rdr_service.ppsc.ppsc_partner_data_transfer.RTIDataTransferNPHOptIn.send_item')
    def test_send_nph_opt_in_items_for_transfer(self, send_request) -> None:

        send_request.return_value = MockedTransferResponse()
        nph_participant_ids = []

        for num in range(1, 4):
            nph_participant_id = f'{num}0000000001'
            nph_participant_ids.append(nph_participant_id)
            self.ppsc_data_gen.create_database_rti_data_nph_opt_in(
                nph_participant_id=nph_participant_id,
                first_name=f'{self.faker.first_name()}',
                last_name=f'{self.faker.last_name()}',
                email=f'{self.faker.email()}',
                phone=1111111111,
                zip_code=11111,
                language_preference=1,
            )

        with RTIDataTransferNPHOptIn() as rti_nph_opt_in:
            rti_nph_opt_in.run_data_transfer()

        # contructor/__enter__ builds correctly
        self.assertEqual(rti_nph_opt_in.transfer_type, DataSyncTransferType.NPH_OPT_IN)

        self.assertEqual(send_request.call_count, len(rti_nph_opt_in.transfer_items))

        current_endpoint = [obj for obj in self.current_endpoint_records
                            if obj.data_sync_transfer_type == DataSyncTransferType.NPH_OPT_IN]
        self.assertEqual(len(current_endpoint), 1)

        current_endpoint = current_endpoint[0]
        self.assertEqual(rti_nph_opt_in.transfer_url, f'{self.base_url}{current_endpoint.endpoint}')

        self.assertEqual(len(rti_nph_opt_in.transfer_items), 3)

        current_transfer_records = self.transfer_record_dao.get_all()

        self.assertEqual(len(current_transfer_records), len(rti_nph_opt_in.transfer_items))

        self.assertTrue(all(str(obj.nph_participant_id) in nph_participant_ids for obj in current_transfer_records))
        self.assertTrue(all(obj.data_sync_transfer_type == DataSyncTransferType.NPH_OPT_IN
                            for obj in current_transfer_records))
        self.assertTrue(all(obj.request_payload is not None for obj in current_transfer_records))
        self.assertTrue(all(obj.response_code == '200' for obj in current_transfer_records))

        # test second run same data should not find items for transfer
        with RTIDataTransferNPHOptIn() as rti_nph_opt_in:
            rti_nph_opt_in.run_data_transfer()

        self.assertEqual(len(rti_nph_opt_in.transfer_items), 0)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("ppsc.rti_data_transfer_auth")
        self.clear_table_after_test("ppsc.rti_data_transfer_endpoint")
        self.clear_table_after_test("ppsc.rti_data_transfer_record")
        self.clear_table_after_test("ppsc.rti_nph_opt_in")
