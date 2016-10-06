"""Tests for metrics_pipeline."""


import datetime
import json
import metrics
import metrics_pipeline
import participant
import unittest

from collections import Counter
from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.ext import testbed


class MetricsPipelineTest(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    ndb.get_context().clear_cache()

    # One participant signs up on 9/1
    self.p1r1 = participant.DAO.history_model(
        date=datetime.datetime(2016, 9, 1, 11, 0, 1),
        obj=participant.Participant(
            drc_internal_id='1',
            zip_code='12345',
            membership_tier=participant.MembershipTier.INTERESTED))
    # Accidentally changes status to ENGAGED
    self.p1r2 = participant.DAO.history_model(
        date=datetime.datetime(2016, 9, 1, 11, 0, 2),
        obj=participant.Participant(
            drc_internal_id='1',
            zip_code='12345',
            membership_tier=participant.MembershipTier.ENGAGED))
    # Fixes it back to INTERESTED
    self.p1r3 = participant.DAO.history_model(
        date=datetime.datetime(2016, 9, 1, 11, 0, 3),
        obj=participant.Participant(
            drc_internal_id='1',
            zip_code='12345',
            membership_tier=participant.MembershipTier.INTERESTED))

    # On 9/10, participant 1 changes their tier, and their zip code.
    self.p1r4 = participant.DAO.history_model(
        date=datetime.datetime(2016, 9, 10),
        obj=participant.Participant(
            drc_internal_id='1',
            zip_code='11111',
            membership_tier=participant.MembershipTier.CONSENTED))

    self.history1 = [self.p1r1, self.p1r2, self.p1r3, self.p1r4]

  def test_key_by_date(self):
    hist_json = json.dumps(participant.DAO.history_to_json(self.p1r1))
    date, obj = metrics_pipeline.key_by_date(hist_json).next()
    self.assertEqual(self.p1r1.date.isoformat(), date)
    self._compare_json(participant.DAO.history_to_json(self.p1r1), obj)

  def test_map_to_id(self):
    group_key, hist_obj = metrics_pipeline.map_to_id(self.p1r1).next()
    self.assertEqual('ParticipantHistory:1', group_key)
    self._compare_json(participant.DAO.history_to_json(self.p1r1), hist_obj)

  def test_reduce_by_id(self):
    history_json = [json.dumps(
        participant.DAO.history_to_json(h)) for h in self.history1]
    results = list(metrics_pipeline.reduce_by_id('ParticipantHistory:1',
                                                 history_json))
    expected1 = {
        "date": "2016-09-01",
        "summary": {
            "Participant.membership_tier.INTERESTED": 1,
            "Participant.zip_code.12345": 1,
            "Participant": 1,
        }
    }
    expected2 = {
        "date": "2016-09-01",
        "summary": {
            "Participant.membership_tier.INTERESTED": -1,
            "Participant.membership_tier.ENGAGED": 1,
        }
    }
    expected3 = {
        "date": "2016-09-01",
        "summary": {
            "Participant.membership_tier.ENGAGED": -1,
            "Participant.membership_tier.INTERESTED": 1,
        }
    }
    expected4 = {
        "date": "2016-09-10",
        "summary": {
            "Participant.membership_tier.INTERESTED": -1,
            "Participant.membership_tier.CONSENTED": 1,
            "Participant.zip_code.12345": -1,
            "Participant.zip_code.11111": 1,
        }
    }
    self.assertEqual(4, len(results))
    self._compare_json(expected1, results[0])
    self._compare_json(expected2, results[1])
    self._compare_json(expected3, results[2])
    self._compare_json(expected4, results[3])

  def test_reduce_date(self):
    reduce_input = [
        '{"date": "2016-09-10T11:00:01", "summary": '
        '{"Participant.membership_tier.INTERESTED": 1,'
        ' "Participant.zip_code.12345": 1}}',

        # Flips to ENGAGED and back.
        '{"date": "2016-09-10T11:00:02", "summary": '
        '  { "Participant.membership_tier.ENGAGED": 1,'
        '    "Participant.membership_tier.INTERESTED": -1}}',

        '{"date": "2016-09-10T11:00:03", "summary": '
        '  { "Participant.membership_tier.ENGAGED": -1,'
        '    "Participant.membership_tier.INTERESTED": 1}}',
    ]
    metrics.set_pipeline_in_progress()
    results = list(metrics_pipeline.reduce_date("2016-09-10", reduce_input))
    self.assertEquals(len(results), 1)
    expected_cnt = Counter(('Participant.zip_code.12345',
                            'Participant.membership_tier.INTERESTED'))
    expected = metrics_pipeline.MetricsBucket(
        date=datetime.datetime(2016, 9, 10), metrics=json.dumps(expected_cnt))
    self.assertEquals(expected.date, results[0].entity.date)
    self.assertEquals(json.loads(expected.metrics),
                      json.loads(results[0].entity.metrics))

  def _compare_json(self, a, b):
    if isinstance(a, str):
      a = json.loads(a)

    if isinstance(b, str):
      b = json.loads(b)

    a = json.dumps(a, sort_keys=True, indent=2)
    b = json.dumps(b, sort_keys=True, indent=2)

    self.assertMultiLineEqual(a, b)

if __name__ == '__main__':
  unittest.main()
