from itertools import cycle

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpro_consent_dao import HealthProConsentDao
from rdr_service.model.consent_file import ConsentSyncStatus
from tests.helpers.unittest_base import BaseTestCase


class HealthProConsentDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao = HealthProConsentDao()
        self.consent_dao = ConsentDao()
        self.num_consents = 10
        self.sync_statuses = [ConsentSyncStatus.SYNC_COMPLETE, ConsentSyncStatus.READY_FOR_SYNC]

    def test_get_consents_for_transfer(self):
        needed_count, needed_ids, limit = 0, [], 2

        for num in range(self.num_consents):
            consent_file = self.data_generator.create_database_consent_file(
                file_path=f'test_file_path/{num}',
                sync_status=self.sync_statuses[0] if num % 2 == 0 else self.sync_statuses[1]
            )
            needed_ids.append(consent_file.id)

            if num % 2 == 0:
                needed_ids.pop()
                needed_count += 1
                self.data_generator.create_database_hpro_consent(
                    consent_file_id=consent_file.id
                )

        self.assertEqual(len(needed_ids), needed_count)

        current_hpro_consents = self.dao.get_all()
        self.assertTrue(
            all(obj for obj in current_hpro_consents if obj.consent_file_id not in needed_ids)
        )

        needed_transfer_consents = self.dao.get_needed_consents_for_transfer()
        self.assertTrue(
            all(obj for obj in needed_transfer_consents if obj.id in needed_ids)
        )
        self.assertEqual(len(needed_transfer_consents), len(needed_ids))

        needed_transfer_consents = self.dao.get_needed_consents_for_transfer(limit=limit)
        self.assertTrue(
            all(obj for obj in needed_transfer_consents if obj.id in needed_ids)
        )
        self.assertEqual(len(needed_transfer_consents), limit)

        self.data_generator.create_database_consent_file()

        needed_transfer_consents = self.dao.get_needed_consents_for_transfer()
        self.assertTrue(
            all(obj for obj in needed_transfer_consents if obj.file_path)
        )

    def test_get_consents_for_transfer_bad_sync_status(self):
        bad_sync_statuses = [status for status in ConsentSyncStatus if status not in self.sync_statuses]
        bad_iterate = cycle(bad_sync_statuses)

        for num in range(self.num_consents):
            consent_file = self.data_generator.create_database_consent_file(
                file_path=f'test_file_path/{num}',
                sync_status=next(bad_iterate)
            )
            self.data_generator.create_database_hpro_consent(
                consent_file_id=consent_file.id
            )

        needed_transfer_consents = self.dao.get_needed_consents_for_transfer()
        self.assertEmpty(needed_transfer_consents)

    def test_get_records_by_participant(self):
        num_pid_records, pids_inserted, no_path_num = 4, [], 2

        for num in range(num_pid_records):
            consent_file = self.data_generator.create_database_consent_file(
                file_path=f'test_file_path/{num}'
            )
            pids_inserted.append(consent_file.participant_id)

            for pid_num in range(num_pid_records):
                self.data_generator.create_database_hpro_consent(
                    file_path=f'test_two_file_path/{num}' if pid_num > 1 else None,
                    participant_id=consent_file.participant_id,
                    consent_file_id=consent_file.id
                )

        for pid in pids_inserted:
            records = self.dao.get_by_participant(pid)
            self.assertEqual(len(records), no_path_num)



