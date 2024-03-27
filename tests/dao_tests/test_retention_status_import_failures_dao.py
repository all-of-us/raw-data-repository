from datetime import datetime

from rdr_service.dao.retention_status_import_failures_dao import RetentionStatusImportFailuresDao
from rdr_service.model.retention_status_import_failures import RetentionStatusImportFailures
from tests.helpers.unittest_base import BaseTestCase


class RetentionStatusImportFailuresDaoTest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.failures_dao = RetentionStatusImportFailuresDao()

        self.failure_1 = RetentionStatusImportFailures(
            id=1,
            created=datetime(2024, 1, 1),
            modified=datetime(2024, 1, 1),
            file_path="gs://bucket/file_name_1",
            failure_count=5
        )
        self.failure_2 = RetentionStatusImportFailures(
            id=2,
            created=datetime(2024, 1, 2),
            modified=datetime(2024, 1, 2),
            file_path="gs://bucket/file_name_2",
            failure_count=10
        )

    def test_get_before_insert(self):
        self.assertIsNone(self.failures_dao.get(1))

    def test_insert_retention_import_failures(self):
        # Insert failures in table
        self.failures_dao.insert(self.failure_1)
        self.failures_dao.insert(self.failure_2)

        self.assertEqual("gs://bucket/file_name_1", self.failures_dao.get(1).file_path)
        self.assertEqual("gs://bucket/file_name_2", self.failures_dao.get(2).file_path)
        self.assertEqual(5, self.failures_dao.get(1).failure_count)
        self.assertEqual(10, self.failures_dao.get(2).failure_count)
