"""Tests for metrics_pipeline."""

import biobank_sample
import datetime
import extraction
import json
import metrics
import offline.metrics_config
import participant
import evaluation
import unittest

from extraction import ExtractionResult
from offline import metrics_pipeline
from offline.metrics_config import FieldDef, FacetDef
from collections import Counter
from google.appengine.ext import ndb
from mapreduce import test_support
from testlib import testutil


def compute_meta(summary):
  if summary['membership_tier'] == 'REGISTERED' and summary['hpo_id'] == 'HPO1':
    val = 'R1'
  else:
    val = 'NOPE'
  return ExtractionResult(val)

CONFIGS_FOR_TEST = {
    'Participant': {
        'load_history_func': participant.load_history_entities,
        'facets': [
            FacetDef(offline.metrics_config.FacetType.HPO_ID, lambda s: s['hpo_id']),
        ],
        'initial_state': {
            'physical_evaluation': 'UNSET',
            'biospecimen_samples': 'UNSET',
        },
        'fields': {
            'ParticipantHistory': [
                FieldDef('membership_tier',
                         extraction.simple_field_extractor('membership_tier'),
                         iter(participant.MembershipTier)),
                FieldDef('age_range',
                         participant.extract_bucketed_age,
                         participant.AGE_BUCKETS),
                FieldDef('hpo_id',
                         participant.extract_HPO_id,
                         participant.HPO_VALUES),
            ],
            'EvaluationHistory': [
                # The presence of a physical evaluation implies that it is complete.
                FieldDef('physical_evaluation',
                         lambda h: ExtractionResult('COMPLETE'),
                         ('None', 'COMPLETE')),
            ],
            'BiobankSamples': [
               # The presence of a biobank sample implies that samples have arrived
               FieldDef('biospecimen_samples', lambda h: ExtractionResult('SAMPLES_ARRIVED'),
                         ('None', 'SAMPLES_ARRIVED'))
            ]
        },
        'summary_fields': [
            FieldDef('meta', compute_meta, ('R1', 'NOPE')),
        ],
    },
}

class MetricsPipelineTest(testutil.HandlerTestBase):
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    self.maxDiff = None
    self.longMessage = True
    self.saved_config_fn = offline.metrics_config.get_config
    offline.metrics_config.get_config = (lambda _=None: CONFIGS_FOR_TEST)

  def tearDown(self):
    offline.metrics_config.get_config = self.saved_config_fn

  def test_map_key_to_summary(self):
    key = ndb.Key(participant.Participant, '1')
    self._populate_sample_history(key)
    results = list(metrics_pipeline.map_key_to_summary(key.to_old_key()))
    expected = [
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.age_range.36-45': 1,
             'Participant.hpo_id.HPO1': 1,
             'Participant.membership_tier.REGISTERED': 1,
             'Participant': 1,
             'Participant.physical_evaluation.UNSET': 1,
             'Participant.biospecimen_samples.UNSET': 1,
             'Participant.meta.R1': 1,
         }),
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.REGISTERED': -1,
             'Participant.meta.R1': -1,
         }),
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.FULL_PARTICIPANT': 1,
             'Participant.meta.NOPE': 1,
         }),
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.FULL_PARTICIPANT': -1,
             'Participant.meta.NOPE': -1,
         }),
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.REGISTERED': 1,
             'Participant.meta.R1': 1,
         }),
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.biospecimen_samples.UNSET': -1,
         }),
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.biospecimen_samples.SAMPLES_ARRIVED': 1,
         }),
        ({'date': '2016-09-05', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.physical_evaluation.UNSET': -1,
         }),
        ({'date': '2016-09-05', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.physical_evaluation.COMPLETE': 1,
         }),
        ({'date': '2016-09-10', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.REGISTERED': -1,
             'Participant.meta.R1': -1,
         }),
        ({'date': '2016-09-10', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.VOLUNTEER': 1,
             'Participant.meta.NOPE': 1,
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
             membership_tier=participant.MembershipTier.REGISTERED,
             hpo_id='HPO1')),
        # One state change in 2015.
        (datetime.datetime(2015, 9, 1, 11, 0, 2),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1970, 8, 21),
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
             'Participant.hpo_id.HPO1': 1,
             'Participant.membership_tier.REGISTERED': 1,
             'Participant.meta.R1': 1,
             'Participant': 1,
             'Participant.physical_evaluation.UNSET': 1,
             'Participant.biospecimen_samples.UNSET': 1,
        }),
        ({'date': '2015-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.REGISTERED': -1,
             'Participant.meta.R1': -1,
         }),
        ({'date': '2015-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.membership_tier.FULL_PARTICIPANT': 1,
             'Participant.meta.NOPE': 1,
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
             hpo_id='HPO1')),
        # One state change in 2012.
        (datetime.datetime(2016, 9, 2),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1970, 8, 21),
             hpo_id='HPO2')),
    ]
    for fake_date, p in history_list:
      participant.DAO.store(p, fake_date)

    results = list(metrics_pipeline.map_key_to_summary(key.to_old_key()))
    expected = [
        ({'date': '2016-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.age_range.46-55': 1,
             'Participant.hpo_id.HPO1': 1,
             'Participant.membership_tier.None': 1,
             'Participant.meta.NOPE': 1,
             'Participant': 1,
             'Participant.physical_evaluation.UNSET': 1,
             'Participant.biospecimen_samples.UNSET': 1,
         }),
        ({'date': '2016-09-02', 'facets': [{'type': 'HPO_ID', 'value': 'HPO1'}]},
         {
             'Participant.age_range.46-55': -1,
             'Participant.hpo_id.HPO1': -1,
             'Participant.membership_tier.None': -1,
             'Participant.meta.NOPE': -1,
             'Participant': -1,
             'Participant.physical_evaluation.UNSET': -1,
             'Participant.biospecimen_samples.UNSET': -1,
         }),
        ({'date': '2016-09-02', 'facets': [{'type': 'HPO_ID', 'value': 'HPO2'}]},
         {
             'Participant.age_range.46-55': 1,
             'Participant.hpo_id.HPO2': 1,
             'Participant.membership_tier.None': 1,
             'Participant.meta.NOPE': 1,
             'Participant': 1,
             'Participant.physical_evaluation.UNSET': 1,
             'Participant.biospecimen_samples.UNSET': 1,
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

  def test_end_to_end(self):
    key = ndb.Key(participant.Participant, '1')
    self._populate_sample_history(key)
    metrics_pipeline.MetricsPipeline().start()
    test_support.execute_until_empty(self.taskqueue)

    serving_version = metrics.get_serving_version()
    metrics_list = list(metrics.MetricsBucket.query(ancestor=serving_version).fetch())
    metrics_list = sorted(metrics_list, key=lambda m: m.date)
    self.assertEquals(datetime.date(2016, 9, 1), metrics_list[0].date)
    self.assertEquals(datetime.date(2016, 9, 5), metrics_list[1].date)
    self.assertEquals(datetime.date(2016, 9, 10), metrics_list[2].date)
    self.assertEquals('[{"type": "HPO_ID", "value": "HPO1"}]', metrics_list[0].facets)
    self.assertEquals('[{"type": "HPO_ID", "value": "HPO1"}]', metrics_list[1].facets)
    self.assertEquals('[{"type": "HPO_ID", "value": "HPO1"}]', metrics_list[2].facets)
    metrics0 = json.loads(metrics_list[0].metrics)
    self.assertEquals(1, metrics0['Participant'])
    self.assertEquals(1, metrics0['Participant.membership_tier.REGISTERED'])
    self.assertEquals(1, metrics0['Participant.hpo_id.HPO1'])

    metrics1 = json.loads(metrics_list[1].metrics)
    self.assertEquals(-1, metrics1['Participant.physical_evaluation.UNSET'])
    self.assertEquals(1, metrics1['Participant.physical_evaluation.COMPLETE'])

    metrics2 = json.loads(metrics_list[2].metrics)
    self.assertEquals(1, metrics2['Participant.membership_tier.VOLUNTEER'])
    self.assertEquals(-1, metrics2['Participant.membership_tier.REGISTERED'])

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

  @ndb.transactional(xg=True)
  def _populate_sample_history(self, key):
    history_list = [
        # One participant signs up on 9/1
        (datetime.datetime(2016, 9, 1, 11, 0, 1),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1975, 8, 21),
             participant_id='1',
             membership_tier=participant.MembershipTier.REGISTERED,
             hpo_id='HPO1')),
        # Accidentally changes status to FULL_PARTICIPANT
        (datetime.datetime(2016, 9, 1, 11, 0, 2),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1975, 8, 21),
             participant_id='1',
             membership_tier=participant.MembershipTier.FULL_PARTICIPANT,
             hpo_id='HPO1')),
        # Fixes it back to REGISTERED
        (datetime.datetime(2016, 9, 1, 11, 0, 3),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1975, 8, 21),
             participant_id='1',
             membership_tier=participant.MembershipTier.REGISTERED,
             hpo_id='HPO1')),

        # Note that on 9/5, an evaluation is entered.

        # On 9/10, participant 1 changes their tier.
        (datetime.datetime(2016, 9, 10),
         participant.Participant(
             key=key,
             date_of_birth=datetime.datetime(1975, 8, 21),
             sign_up_time=datetime.datetime(2016, 9, 1, 11, 0, 2),
             participant_id='1',
             membership_tier=participant.MembershipTier.VOLUNTEER,
             hpo_id='HPO1')),
    ]
    for fake_date, ptc in history_list:
      participant.DAO.store(ptc, fake_date)
    key = ndb.Key(key.flat()[0], key.flat()[1], evaluation.Evaluation, evaluation.DAO.allocate_id())
    evaluation.DAO.store(evaluation.Evaluation(key=key, resource="ignored"),
                         datetime.datetime(2016, 9, 5))
    sample_dict_1 = {
        'familyId': 'SF160914-000001',
        'sampleId': '16258000008',
        'eventName': 'DRC-00123',
        'storageStatus': 'In Prep',
        'type': 'Urine',
        'treatments': 'No Additive',
        'expectedVolume': '10 mL',
        'quantity': '1 mL',
        'containerType': 'TS - Matrix 1.4mL',
        'collectionDate': '2016/09/1 23:59:00',
        'parentSampleId': '16258000001',
        'confirmedDate': '2016/09/2 09:49:00' }
    samples_1 = biobank_sample.DAO.from_json(
        { 'samples': [ sample_dict_1 ]}, '1', biobank_sample.SINGLETON_SAMPLES_ID)
    biobank_sample.DAO.store(samples_1)

if __name__ == '__main__':
  unittest.main()
