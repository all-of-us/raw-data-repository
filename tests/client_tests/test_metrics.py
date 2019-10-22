"""Simple end to end test to exercise the metrics APIs.

The correctness of the metrics is really tested by the unit tests.
As we don't know the state of the database, this is really just testing
that metrics come back and that it doen't crash.
"""

import http.client
import unittest

from rdr_service.rdr_client.client import HttpException
from tests.client_tests.base import BaseClientTest


class MetricsTest(BaseClientTest):
    def test_metrics(self):
        request = {"start_date": "2017-01-21", "end_date": "2017-01-22"}
        try:
            response = self.client.request_json("Metrics", "POST", request)  # pylint: disable=unused-variable
        except HttpException as ex:
            if ex.code == http.client.NOT_FOUND:
                pass
            else:
                raise

    @unittest.skip("metrics 1 endpoint, remove with endpoint in main.")
    def test_metrics_limit(self):
        request = {"start_date": "2017-01-21", "end_date": "2017-01-29"}
        with self.assertRaises(HttpException) as cm:
            self.client.request_json("Metrics", "POST", request)
        self.assertEqual(cm.exception.code, http.client.BAD_REQUEST)

    @unittest.skip("metrics 1 endpoint, remove with endpoint in main.")
    def test_metrics_empty_dates(self):
        request = {"start_date": "", "end_date": ""}
        with self.assertRaises(HttpException) as cm:
            self.client.request_json("Metrics", "POST", request)
        self.assertEqual(cm.exception.code, http.client.BAD_REQUEST)

    def test_metrics_fields(self):
        response = self.client.request_json("MetricsFields")
        self.assertEqual("Participant.ageRange", response[0]["name"])


if __name__ == "__main__":
    unittest.main()
