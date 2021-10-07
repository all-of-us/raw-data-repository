
from unittest import mock

from rdr_service import config
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpro_consent_dao import HealthProConsentDao
from rdr_service.model.consent_file import ConsentSyncStatus
from rdr_service.services.hpro_consent import HealthProConsentFile

from tests.helpers.unittest_base import BaseTestCase


class HealthProConsentFileTest(BaseTestCase):
    def setUp(self):
        super(HealthProConsentFileTest, self).setUp()
        self.consent_dao = ConsentDao()
        self.hpro_consent_dao = HealthProConsentDao()
        self.hpro_consent_bucket = ['healthpro-consents-env']
        config.override_setting(config.HEALTHPRO_CONSENT_BUCKET, self.hpro_consent_bucket)
        self.hpro_consents = HealthProConsentFile()
        self.num_consents = 4
        self.sync_statuses = [ConsentSyncStatus.SYNC_COMPLETE, ConsentSyncStatus.READY_FOR_SYNC]

    @mock.patch('rdr_service.services.hpro_consent.HealthProConsentFile.cp_consent_files')
    def test_initialize_transfers(self, cp_mock):
        self.hpro_consents.initialize_consent_transfer()

        self.assertEqual(len(self.hpro_consents.consents_for_transfer), 0)

        self.assertEqual(cp_mock.call_count, 0)
        self.assertFalse(cp_mock.called)

        consent_ids = []
        for num in range(self.num_consents):
            consent = self.data_generator.create_database_consent_file(
                file_path=f'test_file_path/{num}',
                file_exists=1,
                sync_status=self.sync_statuses[0]
            )
            consent_ids.append(consent.id)

        self.hpro_consents.initialize_consent_transfer()

        self.assertEqual(cp_mock.call_count, 1)
        self.assertTrue(cp_mock.called)

        self.assertEqual(len(self.hpro_consents.consents_for_transfer), self.num_consents)
        self.assertTrue(all(obj for obj in self.hpro_consents.consents_for_transfer if obj.id in consent_ids))

    def test_getting_consent_records_for_transfer(self):
        self.hpro_consents.get_consents_for_transfer()

        self.assertEmpty(self.hpro_consents.consents_for_transfer)

        consent_ids = []
        for num in range(self.num_consents):
            consent = self.data_generator.create_database_consent_file(
                file_path=f'test_file_path/{num}',
                file_exists=1,
                sync_status=self.sync_statuses[0]
            )
            consent_ids.append(consent.id)

            if num > 1:
                consent_ids.pop()
                self.data_generator.create_database_hpro_consent(
                    consent_file_id=consent.id
                )

        self.hpro_consents.get_consents_for_transfer()

        self.assertNotEmpty(self.hpro_consents.consents_for_transfer)

        self.assertTrue(all(obj for obj in self.hpro_consents.consents_for_transfer if obj.id in consent_ids))

        self.assertEqual(len(self.consent_dao.get_all()), self.num_consents)
        self.assertEqual(len(self.hpro_consent_dao.get_all()), self.num_consents // 2)

        self.hpro_consents.transfer_limit = 1
        self.hpro_consents.get_consents_for_transfer()

        self.assertNotEmpty(self.hpro_consents.consents_for_transfer)
        self.assertTrue(any(obj for obj in self.hpro_consents.consents_for_transfer if obj.id in consent_ids))
        self.assertEqual(len(self.hpro_consents.consents_for_transfer), self.hpro_consents.transfer_limit)

    def test_destination_creation(self):
        destinations = []
        for num in range(self.num_consents):
            self.data_generator.create_database_consent_file(
                file_path=f'test_file_path/{num}',
                file_exists=1,
            )

        self.hpro_consents.get_consents_for_transfer()

        for consent in self.hpro_consents.consents_for_transfer:

            self.assertEqual(consent.file_path.split('/')[0], 'test_file_path')

            destinations.append(
                self.hpro_consents.create_path_destination(consent.file_path)
            )

        for dest in destinations:
            self.assertIn('gs://', dest)
            self.assertIn(self.hpro_consent_bucket[0], dest)

    @mock.patch('rdr_service.services.hpro_consent.gcp_cp')
    def test_copying_consent_files_calls_transfer(self, gcp_cp_mock):
        self.hpro_consents.get_consents_for_transfer()

        self.assertEmpty(self.hpro_consents.consents_for_transfer)

        self.hpro_consents.cp_consent_files()

        self.assertFalse(gcp_cp_mock.called)
        self.assertEqual(gcp_cp_mock.call_count, 0)

        paths = []

        for num in range(self.num_consents):
            consent = self.data_generator.create_database_consent_file(
                file_path=f'test_file_path/{num}',
                file_exists=1,
                sync_status=self.sync_statuses[0]
            )
            paths.append({
                'src': f'gs://test_file_path/{num}',
                'dest': self.hpro_consents.create_path_destination(consent.file_path)
            })

        self.hpro_consents.get_consents_for_transfer()

        self.assertNotEmpty(self.hpro_consents.consents_for_transfer)

        self.hpro_consents.cp_consent_files()

        self.assertTrue(gcp_cp_mock.called)
        self.assertEqual(gcp_cp_mock.call_count, self.num_consents)

        call_args = gcp_cp_mock.call_args_list

        for i, val in enumerate(call_args):
            self.assertIsNotNone(paths[i]['src'])
            self.assertIsNotNone(paths[i]['dest'])
            self.assertEqual(paths[i]['src'], val[0][0])
            self.assertEqual(paths[i]['dest'], val[0][1])

    def test_hpro_transfer_limit_from_config(self):
        limit = [2]

        for num in range(self.num_consents):
            self.data_generator.create_database_consent_file(
                file_path=f'test_file_path/{num}',
                file_exists=1,
                sync_status=self.sync_statuses[0]
            )

        self.hpro_consents.get_consents_for_transfer()
        self.assertEqual(len(self.hpro_consents.consents_for_transfer), self.num_consents)

        self.hpro_consents.transfer_limit = config.getSetting(config.HEALTHPRO_CONSENTS_TRANSFER_LIMIT, default=1)
        self.hpro_consents.get_consents_for_transfer()
        self.assertEqual(len(self.hpro_consents.consents_for_transfer), 1)

        config.override_setting(config.HEALTHPRO_CONSENTS_TRANSFER_LIMIT, limit)
        self.hpro_consents.transfer_limit = config.getSetting(config.HEALTHPRO_CONSENTS_TRANSFER_LIMIT)
        self.hpro_consents.get_consents_for_transfer()
        self.assertEqual(len(self.hpro_consents.consents_for_transfer), limit[0])


