import datetime
import unittest

import pytz

import cloud_utils.bigquery


class BigQueryJobTest(unittest.TestCase):
  """
  This test suite tests querying against real BigQuery datasets.
  """

  @unittest.skip("Only used for manual testing, should not be included in automated test suite")
  def test_query(self):
    """
    Test an actual query with pagination
    """
    query = (
      'SELECT gameId, gameNumber, created FROM `bigquery-public-data.baseball.schedules` LIMIT 3'
    )
    job = cloud_utils.bigquery.BigQueryJob(
      query,
      project_id='all-of-us-rdr-sandbox',
      default_dataset_id='baseball',
      page_size=2
    )
    pages = list(job)
    self.assertEqual(len(pages), 2)
    row = pages[0][0]
    self.assertTrue(isinstance(row.gameId, basestring))
    self.assertTrue(isinstance(row.gameNumber, int))
    self.assertTrue(isinstance(row.created, datetime.datetime))


class BigQueryJobTransformationTest(unittest.TestCase):

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

  def test_type_timestamp(self):
    response = {
      u'rows': [
        {u'f': [{u'v': u'2019-05-25 04:22:16.052 UTC'}]},
        {u'f': [{u'v': u'1.475735115E9'}]},
        {u'f': [{u'v': 1.475735115E9}]},
        {u'f': [{u'v': None}]},
        {u'f': [{u'v': u''}]},
      ],
      u'schema': {
        u'fields': [
          {
            u'mode': u'NULLABLE',
            u'name': u'timestamp',
            u'type': u'TIMESTAMP',
          }
        ]
      }
    }
    rows = cloud_utils.bigquery.BigQueryJob.get_rows_from_response(response)
    self.assertEqual(rows[0].timestamp, datetime.datetime(2019, 5, 25, 4, 22, 16, 52000, pytz.UTC))
    self.assertEqual(rows[1].timestamp, datetime.datetime(2016, 10, 6, 6, 25, 15, 0, pytz.UTC))
    self.assertEqual(rows[2].timestamp, datetime.datetime(2016, 10, 6, 6, 25, 15, 0, pytz.UTC))
    self.assertEqual(rows[3].timestamp, None)
    self.assertEqual(rows[4].timestamp, None)

    response = {
      u'rows': [
        {u'f': [{u'v': dict(foo='bar')}]},
        {u'f': [{u'v': u'An invalid datestring'}]},
      ],
      u'schema': {
        u'fields': [
          {
            u'mode': u'NULLABLE',
            u'name': u'timestamp',
            u'type': u'TIMESTAMP',
          }
        ]
      }
    }
    with self.assertRaises(ValueError):
      cloud_utils.bigquery.BigQueryJob.get_rows_from_response(response)
