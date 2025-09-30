from datetime import date
from unittest.mock import patch

from rdr_service.dao.study_nph_dao import NphBiospecimenDao

from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.fake_bq import FakeBQClient


class NphBiospecimenAPITest(BaseTestCase):
    def setUp(self):
        super().setUp()

    def make_fake_rows(self, ids):
        # Minimal to test API functionality. BigQuery results should be verified independently
        return [{"nph_participant_id": i, "biospecimens": [{"orderID": str(i)}]} for i in ids]

    @patch("rdr_service.dao.study_nph_dao.bigquery.Client")
    def test_get_by_participant_404(self, bq_mock):
        bq_mock.return_value = FakeBQClient({date(2025, 9, 3): self.make_fake_rows([1, 2])})
        dao = NphBiospecimenDao()
        response = dao.get_by_participant(999)
        self.assertEqual(response, [])

    @patch("rdr_service.dao.study_nph_dao.bigquery.Client")
    def test_get_by_participant(self, bq_mock):
        bq_mock.return_value = FakeBQClient({date(2025, 9, 3): self.make_fake_rows([1, 2])})
        dao = NphBiospecimenDao()
        result = dao.get_by_participant(2)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0]["biospecimens"], list)

    @patch("rdr_service.dao.study_nph_dao.bigquery.Client")
    def test_get_page_with_token(self, bq_mock):
        bq_mock.return_value = FakeBQClient({date(2025, 9, 3): self.make_fake_rows([1, 2, 3, 4, 5, 6])})
        dao = NphBiospecimenDao()
        result = dao.get_all(count=5, token=None)
        self.assertEqual(len(result.items), 5)
        self.assertTrue(result.more_available)
        self.assertIsNotNone(result.pagination_token)
        self.assertEqual(result.items[-1]["nph_participant_id"], 5)

    @patch("rdr_service.dao.study_nph_dao.bigquery.Client")
    def test_get_all_second_page(self, bq_mock):
        bq_mock.return_value = FakeBQClient({date(2025, 9, 3): self.make_fake_rows([1, 2, 3, 4, 5, 6])})
        dao = NphBiospecimenDao()
        result_1 = dao.get_all(count=5)
        result_2 = dao.get_all(count=5, token=result_1.pagination_token)
        self.assertEqual(len(result_2.items), 1)
        self.assertFalse(result_2.more_available)
        self.assertEqual(result_2.items[0]["nph_participant_id"], 6)

    @patch("rdr_service.dao.study_nph_dao.bigquery.Client")
    def test_pagination_locks_run_date(self, bq_mock):
        # First partition
        partitions = {date(2025, 9, 3): self.make_fake_rows([1, 2, 3, 4, 5, 6])}
        fake = FakeBQClient(partitions)
        bq_mock.return_value = fake

        dao = NphBiospecimenDao()
        result_1 = dao.get_all(count=3)  # token should contain run_date=2025-09-03

        # New partition arrives
        partitions[date(2025, 9, 4)] = self.make_fake_rows([10, 11, 12])

        # Page 2 should still read from 2025-09-03 partition
        result_2 = dao.get_all(count=3, token=result_1.pagination_token)
        self.assertEqual([i["nph_participant_id"] for i in result_2.items], [4, 5, 6])

    @patch("rdr_service.dao.study_nph_dao.bigquery.Client")
    def test_api_get_single_participant(self, bq_mock):
        bq_mock.return_value = FakeBQClient({date(2025, 9, 3): self.make_fake_rows([1001])})
        response = self.send_get("nph/Biospecimen/1001")
        self.assertIsInstance(response, list)
        self.assertEqual(response[0]["nph_participant_id"], 1001)

    @patch("rdr_service.dao.study_nph_dao.bigquery.Client")
    def test_api_get_all_with_pages(self, bq_mock):
        bq_mock.return_value = FakeBQClient({date(2025, 9, 3): self.make_fake_rows([1, 2, 3])})

        # Page 1
        response_1 = self.send_get("nph/Biospecimen?count=2")
        self.assertEqual(len(response_1["items"]), 2)
        self.assertTrue(response_1["more_available"])
        self.assertIsNotNone(response_1["pagination_token"])

        # Page 2
        response_2 = self.send_get("nph/Biospecimen", query_string={"count": 2, "token": response_1["pagination_token"]})
        self.assertEqual([item["nph_participant_id"] for item in response_2["items"]], [3])
        self.assertFalse(response_2["more_available"])
        self.assertIsNone(response_2["pagination_token"])

    def tearDown(self):
        self.tearDown()
        self.clear_table_after_test("nph.participant")
