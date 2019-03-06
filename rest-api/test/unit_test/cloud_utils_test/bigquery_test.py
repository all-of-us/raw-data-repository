import cloud_utils.bigquery
from unit_test_util import CloudStorageSqlTestBase


class BigQueryTest(CloudStorageSqlTestBase):
  """
  This test suite tests querying against real BigQuery datasets.
  """

  def test_query_shakespeare(self):
    """
    Test that the `bigquery` helper method returns results in the expected format.
    """
    query = 'select * from `bigquery-public-data.samples.shakespeare` limit 3'
    results = cloud_utils.bigquery.bigquery(query)
    self.assertEqual(len(results), 3)
    self.assertEqual(results[0].keys(), [
      'word',
      'word_count',
      'corpus',
      'corpus_date',
    ])
