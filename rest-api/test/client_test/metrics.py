"""Simple end to end test to exercise the metrics APIs.

The correctness of the metrics is really tested by the unit tests.
As we don't know the state of the database, this is really just testing
that metrics come back and that it doen't crash.
"""

import unittest
import pprint
import client

import test_util

class MetricsTest(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')

  def testMembershipTier(self):
    request = {
        'metric': 'PARTICIPANT_MEMBERSHIP_TIER',
        'facets': ['HPO_ID'],
    }
    try:
      response = self.client.request_json('Metrics', 'POST', request)
      pprint.pprint(response)
    except client.client.HttpException as ex:
      if ex.code == 404:
        print "No metrics loaded"
      else:
        raise ex

  def testParticipantTotal(self):
    request = {
        'metric': 'PARTICIPANT_TOTAL',
        'bucket_by': 'NONE'
    }
    try:
      response = self.client.request_json('Metrics', 'POST', request)
      pprint.pprint(response)
    except client.client.HttpException as ex:
      if ex.code == 404:
        print "No Metrics loaded"
      else:
        raise ex


if __name__ == '__main__':
  unittest.main()
