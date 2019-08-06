import datetime
import unittest

import pytz

from rdr_service.cloud_utils import bigquery


class BigQueryJobTest(unittest.TestCase):
    """
  This test suite tests querying against real BigQuery datasets.
  """

    @unittest.skip("Only used for manual testing, should not be included in automated test suite")
    def test_query(self):
        """
    Test an actual query with pagination
    """
        query = "SELECT gameId, gameNumber, created FROM `bigquery-public-data.baseball.schedules` LIMIT 3"
        job = bigquery.BigQueryJob(
            query, project_id="all-of-us-rdr-sandbox", default_dataset_id="baseball", page_size=2
        )
        pages = list(job)
        self.assertEqual(len(pages), 2)
        row = pages[0][0]
        self.assertTrue(isinstance(row.gameId, str))
        self.assertTrue(isinstance(row.gameNumber, int))
        self.assertTrue(isinstance(row.created, datetime.datetime))


class BigQueryJobTransformationTest(unittest.TestCase):
    def test_response_transformation(self):
        """
    Test the transformation function to make bigquery responses easier to use practically.
    """
        response = {
            "cacheHit": True,
            "jobComplete": True,
            "jobReference": {
                "jobId": "job_TT9ZzgKsi9wg_pwXYS_Krm1D_2IV",
                "location": "US",
                "projectId": "all-of-us-rdr-sandbox",
            },
            "kind": "bigquery#queryResponse",
            "rows": [
                {"f": [{"v": "LVII"}, {"v": "1"}, {"v": "sonnets"}, {"v": "0"}]},
                {"f": [{"v": "augurs"}, {"v": "1"}, {"v": "sonnets"}, {"v": "0"}]},
                {"f": [{"v": "dimm'd"}, {"v": "1"}, {"v": "sonnets"}, {"v": "0"}]},
            ],
            "schema": {
                "fields": [
                    {"mode": "NULLABLE", "name": "word", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "word_count", "type": "INTEGER"},
                    {"mode": "NULLABLE", "name": "corpus", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "corpus_date", "type": "INTEGER"},
                ]
            },
            "totalBytesProcessed": "0",
            "totalRows": "3",
        }
        rows = bigquery.BigQueryJob.get_rows_from_response(response)
        self.assertEqual(len(rows), 3)
        first_row = rows[0]
        self.assertEqual(list(first_row._asdict().keys()), ["word", "word_count", "corpus", "corpus_date"])
        self.assertEqual(type(first_row.word), type("some unicode string"))
        self.assertEqual(type(first_row.word_count), type(12345))

    def test_type_timestamp(self):
        response = {
            "rows": [
                {"f": [{"v": "2019-05-25 04:22:16.052 UTC"}]},
                {"f": [{"v": "1.475735115E9"}]},
                {"f": [{"v": 1.475735115e9}]},
                {"f": [{"v": None}]},
                {"f": [{"v": ""}]},
            ],
            "schema": {"fields": [{"mode": "NULLABLE", "name": "timestamp", "type": "TIMESTAMP"}]},
        }
        rows = bigquery.BigQueryJob.get_rows_from_response(response)
        self.assertEqual(rows[0].timestamp, datetime.datetime(2019, 5, 25, 4, 22, 16, 52000, pytz.UTC))
        self.assertEqual(rows[1].timestamp, datetime.datetime(2016, 10, 6, 6, 25, 15, 0, pytz.UTC))
        self.assertEqual(rows[2].timestamp, datetime.datetime(2016, 10, 6, 6, 25, 15, 0, pytz.UTC))
        self.assertEqual(rows[3].timestamp, None)
        self.assertEqual(rows[4].timestamp, None)

        response = {
            "rows": [{"f": [{"v": dict(foo="bar")}]}, {"f": [{"v": "An invalid datestring"}]}],
            "schema": {"fields": [{"mode": "NULLABLE", "name": "timestamp", "type": "TIMESTAMP"}]},
        }
        with self.assertRaises(ValueError):
            bigquery.BigQueryJob.get_rows_from_response(response)
