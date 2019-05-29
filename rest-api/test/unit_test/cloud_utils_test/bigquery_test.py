import unittest

import cloud_utils.bigquery
from test.unit_test.unit_test_util import CloudStorageSqlTestBase


class BigQueryJobTest(CloudStorageSqlTestBase):
  """
  This test suite tests querying against real BigQuery datasets.
  """

  @unittest.skip("Only used for manual testing, should not be included in automated test suite")
  def test_query(self):
    """
    Test an actual query with pagination
    """
    query = 'SELECT * FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 10'
    job = cloud_utils.bigquery.BigQueryJob(
      query,
      project_id='all-of-us-rdr-sandbox',
      default_dataset_id='usa_names',
      page_size=3
    )
    pages = list(job)
    self.assertEqual(len(pages), 4)
    row = pages[0][0]
    self.assertTrue(isinstance(row.name, basestring))
    self.assertTrue(isinstance(row.year, int))

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
    rows = cloud_utils.bigquery.BigQueryJob.get_rows_from_response(response)
    self.assertEqual(len(rows), 3)
    first_row = rows[0]
    self.assertEqual(first_row._asdict().keys(), [
      'word',
      'word_count',
      'corpus',
      'corpus_date',
    ])
    self.assertEqual(type(first_row.word), type(u'some unicode string'))
    self.assertEqual(type(first_row.word_count), type(12345))
