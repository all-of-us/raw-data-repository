
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpro_consent_dao import HProConsentDao
from tests.helpers.unittest_base import BaseTestCase


class HproConsentDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao = HProConsentDao()
        self.consent_dao = ConsentDao()
        self.num_consents = 10

    def test_get_consents_for_transfer(self):
        needed_count, needed_ids, limit = 0, [], 2

        for num in range(self.num_consents):
            consent_file = self.data_generator.create_database_consent_file(
                file_path=f'test_file_path/{num}'
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

    def test_get_records_by_participant(self):
        num_pid_records, pids_inserted, no_path_num = 4, [], 2

        for num in range(5):
            consent_file = self.data_generator.create_database_consent_file(
                file_path=f'test_file_path/{num}'
            )
            pids_inserted.append(consent_file.participant_id)

            for pid_num in range(num_pid_records):
                self.data_generator.create_database_hpro_consent(
                    file_path=consent_file.file_path if pid_num > 1 else None,
                    participant_id=consent_file.participant_id,
                )

        for pid in pids_inserted:
            records = self.dao.get_by_participant(pid)
            self.assertEqual(len(records), no_path_num)

