"""Simple end to end test to exercise the metrics APIs.

The correctness of the metrics is really tested by the unit tests.
As we don't know the state of the database, this is really just testing
that metrics come back and that it doen't crash.
"""

import unittest
import pprint

import test_util

class MetricsTest(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('metrics/v1')

  # def testMetricsByMonth(self):
  #   request = {
  #       'metric': 'PARTICIPANT_ZIP_CODE',
  #       'bucket_by': 'MONTH',
  #       'start_date': '2016-10-01',
  #       'end_date': '2017-10-01',
  #   }

  #   response = self.client.request_json('metrics', 'POST', request)
  #   pprint.pprint(response)

  # def testTotalMetric(self):
  #   request = {
  #       'metric': 'PARTICIPANT_TOTAL',
  #   }

  #   response = self.client.request_json('metrics', 'POST', request)
  #   pprint.pprint(response)

  def testMembershipTier(self):
    request = {
        'metric': 'PARTICIPANT_MEMBERSHIP_TIER',
        'facets': ['HPO_ID'],
    }
    response = self.client.request_json('metrics', 'POST', request)
    pprint.pprint(response)

if __name__ == '__main__':
  unittest.main()
