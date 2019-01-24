
from google.cloud import bigquery
from test.unit_test.unit_test_util import FlaskTestBase

# To fully unit test Big Query, the authentication and communications need to be
# mocked up.

# Examples: https://github.com/googleapis/google-cloud-python/tree/master/bigquery/tests/unit

class TestConfig(FlaskTestBase):

  def test_init_bigquery_client(self):

    client = bigquery.Client()
    self.assertIsNotNone(client)




