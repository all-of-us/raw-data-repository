import unittest

import cloud_utils.bigquery
from unit_test_util import CloudStorageSqlTestBase


class BigQueryTest(CloudStorageSqlTestBase):
  """
  This test suite tests querying against real BigQuery datasets.
  """

  @unittest.skip("Authentication to actual BigQuery not properly set up for tests")
  def test_query(self):
    """
    Test that the `bigquery` helper method returns results in the expected format.
    """

    query = 'select * from ehr_upload_pids limit 3'
    response = cloud_utils.bigquery.bigquery(
      query,
      app_id='all-of-us-rdr-sandbox',
      dataset_id='curation_test'
    )
    self.assertEqual(response['totalRows'], u'3')

  @unittest.skip("Authentication to actual BigQuery not properly set up for tests")
  def test_response_transformation(self):
    """
    Test the transformation function to make bigquery responses easier to use practically.
    """
    response = {u'cacheHit': True,
                u'jobComplete': True,
                u'jobReference': {u'jobId': u'job_TT9ZzgKsi9wg_pwXYS_Krm1D_2IV',
                                  u'location': u'US',
                                  u'projectId': u'all-of-us-rdr-sandbox'},
                u'kind': u'bigquery#queryResponse',
                u'rows': [{u'f': [{u'v': u'LVII'},
                                  {u'v': u'1'},
                                  {u'v': u'sonnets'},
                                  {u'v': u'0'}]},
                          {u'f': [{u'v': u'augurs'},
                                  {u'v': u'1'},
                                  {u'v': u'sonnets'},
                                  {u'v': u'0'}]},
                          {u'f': [{u'v': u"dimm'd"},
                                  {u'v': u'1'},
                                  {u'v': u'sonnets'},
                                  {u'v': u'0'}]}],
                u'schema': {u'fields': [{u'mode': u'NULLABLE',
                                         u'name': u'word',
                                         u'type': u'STRING'},
                                        {u'mode': u'NULLABLE',
                                         u'name': u'word_count',
                                         u'type': u'INTEGER'},
                                        {u'mode': u'NULLABLE',
                                         u'name': u'corpus',
                                         u'type': u'STRING'},
                                        {u'mode': u'NULLABLE',
                                         u'name': u'corpus_date',
                                         u'type': u'INTEGER'}]},
                u'totalBytesProcessed': u'0',
                u'totalRows': u'3'}
    rows = cloud_utils.bigquery.get_row_dicts_from_bigquery_response(response)
    self.assertEqual(len(rows), 3)
    first_row = rows[0]
    self.assertEqual(first_row.keys(), [
      'word',
      'word_count',
      'corpus',
      'corpus_date',
    ])
    self.assertEqual(type(first_row['word']), type(u'some unicode string'))
    self.assertEqual(type(first_row['word_count']), type(u'12345'))
