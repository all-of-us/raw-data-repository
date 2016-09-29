"""Tests for metrics_pipeline."""

import copy
import datetime
import json
import metrics
import participant
import unittest

from offline import metrics_pipeline
from collections import Counter
from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.ext import testbed


class MetricsPipelineTest(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.longMessage = True
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    ndb.get_context().clear_cache()

    self.saved_configs = copy.deepcopy(metrics_pipeline.METRICS_CONFIGS)

  def tearDown(self):
    metrics_pipeline.METRICS_CONFIGS = self.saved_configs

  def test_map_key_to_summary(self):
    key = ndb.Key(participant.Participant, '1')
    history_list = [
        # One participant signs up on 9/1
        (datetime.datetime(2016, 9, 1, 11, 0, 1),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1975, 8, 21),
             gender_identity=participant.GenderIdentity.MALE,
             participant_id='1',
             membership_tier=participant.MembershipTier.REGISTERED,
             hpo_id='HPO1')),
        # Accidentally changes status to FULL_PARTICIPANT
        (datetime.datetime(2016, 9, 1, 11, 0, 2),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1975, 8, 21),
             gender_identity=participant.GenderIdentity.MALE,
             participant_id='1',
             membership_tier=participant.MembershipTier.FULL_PARTICIPANT,
             hpo_id='HPO1')),
        # Fixes it back to REGISTERED
        (datetime.datetime(2016, 9, 1, 11, 0, 3),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1975, 8, 21),
             gender_identity=participant.GenderIdentity.MALE,
             participant_id='1',
             membership_tier=participant.MembershipTier.REGISTERED,
             hpo_id='HPO1')),
        # On 9/10, participant 1 changes their tier.
        (datetime.datetime(2016, 9, 10),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1975, 8, 21),
             sign_up_time=datetime.datetime(2016, 9, 1, 11, 0, 2),
             gender_identity=participant.GenderIdentity.MALE,
             participant_id='1',
             membership_tier=participant.MembershipTier.VOLUNTEER,
             hpo_id='HPO1')),
    ]
    for fake_date, p in history_list:
      participant.DAO.store(p, fake_date)
    results = list(metrics_pipeline.map_key_to_summary(key.to_old_key()))
    expected = [
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.age_range.36-45': 1,
             'Participant.gender_identity.MALE': 1,
             'Participant.hpo_id.HPO1': 1,
             'Participant.membership_tier.REGISTERED': 1,
             'Participant': 1,
         }),
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.REGISTERED': -1,
         }),
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.FULL_PARTICIPANT': 1,
         }),
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.FULL_PARTICIPANT': -1,
         }),
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.REGISTERED': 1,
         }),
        ({'date': '2016-09-10', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.REGISTERED': -1,
         }),
        ({'date': '2016-09-10', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.VOLUNTEER': 1,
         }),
    ]
    expected = [(json.dumps(d), json.dumps(s, sort_keys=True)) for d, s in expected]
    self._compare_json_list(expected, results)

  def test_map_key_to_summary_participant_ages(self):
    key = ndb.Key(participant.Participant, '1')
    history_list = [
        # One participant signs up in 2013.
        (datetime.datetime(2013, 9, 1, 11, 0, 1),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1970, 8, 21),
             gender_identity=participant.GenderIdentity.MALE,
             membership_tier=participant.MembershipTier.REGISTERED,
             hpo_id='HPO1')),
        # One state change in 2015.
        (datetime.datetime(2015, 9, 1, 11, 0, 2),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1970, 8, 21),
             gender_identity=participant.GenderIdentity.MALE,
             membership_tier=participant.MembershipTier.FULL_PARTICIPANT,
             hpo_id='HPO1')),
    ]
    for fake_date, p in history_list:
      participant.DAO.store(p, fake_date)

    results = list(metrics_pipeline.map_key_to_summary(key.to_old_key(),
                                                       datetime.datetime(2016, 10, 17)))
    expected = [
        ({'date': '2013-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.age_range.36-45': 1,
             'Participant.gender_identity.MALE': 1,
             'Participant.hpo_id.HPO1': 1,
             'Participant.membership_tier.REGISTERED': 1,
             'Participant': 1,
        }),
        ({'date': '2014-08-21', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]}, {}),
        ({'date': '2015-08-21', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]}, {}),
        ({'date': '2015-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.REGISTERED': -1,
         }),
        ({'date': '2015-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.FULL_PARTICIPANT': 1,
         }),
        ({'date': '2016-08-21', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.age_range.36-45': -1,
         }),
        ({'date': '2016-08-21', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.age_range.46-55': 1,
         }),
    ]
    expected = [(json.dumps(d), json.dumps(s, sort_keys=True)) for d, s in expected]
    self._compare_json_list(expected, results)

  def test_map_key_to_summary_hpo_changes(self):
    key = ndb.Key(participant.Participant, '1')
    history_list = [
        # One participant signs up in 2013.
        (datetime.datetime(2016, 9, 1),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1970, 8, 21),
             gender_identity=participant.GenderIdentity.MALE,
             hpo_id='HPO1')),
        # One state change in 2012.
        (datetime.datetime(2016, 9, 2),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1970, 8, 21),
             gender_identity=participant.GenderIdentity.MALE,
             hpo_id='HPO2')),
    ]
    for fake_date, p in history_list:
      participant.DAO.store(p, fake_date)

    results = list(metrics_pipeline.map_key_to_summary(key.to_old_key()))
    expected = [
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.age_range.46-55': 1,
             'Participant.gender_identity.MALE': 1,
             'Participant.hpo_id.HPO1': 1,
             'Participant.membership_tier.None': 1,
             'Participant': 1,
         }),
        ({'date': '2016-09-02', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.age_range.46-55': -1,
             'Participant.gender_identity.MALE': -1,
             'Participant.hpo_id.HPO1': -1,
             'Participant.membership_tier.None': -1,
             'Participant': -1,
         }),
        ({'date': '2016-09-02', 'facets': [{'type': 'HPO_ID', 'value': 'HPO2'}]},
         {
             'Participant.age_range.46-55': 1,
             'Participant.gender_identity.MALE': 1,
             'Participant.hpo_id.HPO2': 1,
             'Participant.membership_tier.None': 1,
             'Participant': 1,
         }),
    ]
    expected = [(json.dumps(d), json.dumps(s, sort_keys=True)) for d, s in expected]
    self._compare_json_list(expected, results)

  def test_reduce_facets(self):
    reduce_input = [
        json.dumps({
            'Participant.membership_tier.REGISTERED': 1,
        }),

        # Flips to ENGAGED and back.
        json.dumps({
            'Participant.membership_tier.FULL_PARTICIPANT': 1,
            'Participant.membership_tier.REGISTERED': -1,
        }),
        json.dumps({
            'Participant.membership_tier.FULL_PARTICIPANT': -1,
            'Participant.membership_tier.REGISTERED': 1,
        }),
    ]
    facets_key = json.dumps({'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]})

    metrics.set_pipeline_in_progress()
    results = list(metrics_pipeline.reduce_facets(facets_key, reduce_input))
    self.assertEquals(len(results), 1)
    expected_cnt = Counter(('Participant.membership_tier.REGISTERED',))
    expected_json = json.dumps(expected_cnt)
    self.assertEquals(
        json.dumps([{'type': 'HPO_ID', 'value': 'HPO1'}]),
        results[0].entity.facets)
    self.assertEquals(expected_json, results[0].entity.metrics)

  def test_bucket_age(self):
    self.assertEqual('18-25', metrics_pipeline._bucket_age(18))
    self.assertEqual('18-25', metrics_pipeline._bucket_age(19))
    self.assertEqual('18-25', metrics_pipeline._bucket_age(25))
    self.assertEqual('26-35', metrics_pipeline._bucket_age(26))
    self.assertEqual('76-85', metrics_pipeline._bucket_age(85))
    self.assertEqual('86-', metrics_pipeline._bucket_age(86))
    self.assertEqual('86-', metrics_pipeline._bucket_age(100))


  def _compare_json(self, a, b, msg=None):
    if isinstance(a, str):
      a = json.loads(a)

    if isinstance(b, str):
      b = json.loads(b)

    a = json.dumps(a, sort_keys=True, indent=2)
    b = json.dumps(b, sort_keys=True, indent=2)

    self.assertMultiLineEqual(a, b, msg=msg)

  def _compare_json_list(self, a_list, b_list):
    self.assertEqual(len(a_list), len(b_list))
    for i, (a, b) in enumerate(zip(a_list, b_list)):
      msg = 'Comparing element {}'.format(i)
      self._compare_json(a, b, msg)


if __name__ == '__main__':
  unittest.main()
