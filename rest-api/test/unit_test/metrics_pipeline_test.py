"""Tests for metrics_pipeline."""

import biobank_sample
import concepts
import datetime
import extraction
import fhir_datatypes
import json
import metrics
import offline.metrics_config
import participant
import participant_summary
import questionnaire
import questionnaire_response
import evaluation
import unittest
import os

from extraction import ExtractionResult, BASE_VALUES, UNSET
from offline import metrics_pipeline
from offline.metrics_config import FieldDef
from collections import Counter
from google.appengine.ext import ndb
from mapreduce import test_support
from testlib import testutil


def compute_meta(summary):
  if summary['membershipTier'] == 'REGISTERED' and summary.get('hpoId') == 'PITT':
    val = 'R1'
  else:
    val = 'NOPE'
  return ExtractionResult(val)

NOW = datetime.datetime(2016, 9, 12, 11, 0, 1)
CONFIGS_FOR_TEST = {
    'Participant': {
        'initial_state': {
            'physicalEvaluation': UNSET,
            'biospecimenSamples': UNSET,
            'membershipTier': UNSET,
            'ageRange': UNSET,
            'race': UNSET,
            'ethnicity': UNSET,
            'state': UNSET,
        },
        'fields': {
            'ParticipantHistory': [
              FieldDef('hpoId', participant.extract_HPO_id,
                       BASE_VALUES | set(participant_summary.HPOId)),
            ],
            'AgeHistory': [
              FieldDef('ageRange', participant_summary.extract_bucketed_age,
                       BASE_VALUES | set(participant_summary.AGE_BUCKETS)),
            ],
            'QuestionnaireResponseHistory': [
                FieldDef('race',
                            questionnaire_response.extractor_for(concepts.RACE, extraction.VALUE_CODING),
                            set(participant_summary.Race)),
                FieldDef('ethnicity',
                            questionnaire_response.extractor_for(concepts.ETHNICITY, extraction.VALUE_CODING),
                            set(participant_summary.Ethnicity)),
                FieldDef('state',
                         questionnaire_response.extract_state_of_residence,
                         BASE_VALUES | questionnaire_response.states()),
                FieldDef('membershipTier',
                         questionnaire_response.extractor_for(concepts.MEMBERSHIP_TIER, extraction.VALUE_CODING),
                            set(participant_summary.MembershipTier)),
            ],
            'EvaluationHistory': [
                # The presence of a physical evaluation implies that it is complete.
                FieldDef('physicalEvaluation',
                         lambda h: ExtractionResult('COMPLETE'),
                         ('None', 'COMPLETE')),
            ],
            'BiobankSamples': [
               # The presence of a biobank sample implies that samples have arrived
               FieldDef('biospecimenSamples', lambda h: ExtractionResult('SAMPLES_ARRIVED'),
                         ('None', 'SAMPLES_ARRIVED'))
            ]
        },
        'summary_fields': [
            FieldDef('meta', compute_meta, ('R1', 'NOPE')),
        ],
    },
}

class MetricsPipelineTest(testutil.CloudStorageTestBase):
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    self.maxDiff = None
    self.longMessage = True
    self.saved_config_fn = offline.metrics_config.get_config
    offline.metrics_config.get_config = (lambda: CONFIGS_FOR_TEST)

  def tearDown(self):
    offline.metrics_config.get_config = self.saved_config_fn

  def test_map(self):
    key = ndb.Key(participant.Participant, '1')
    self._populate_sample_history(key)
    results = list(metrics_pipeline.map(key.to_old_key(), NOW))

    expected = [
        ('PITT|Participant', '2016-09-01|1'),
        ('PITT|Participant.ageRange.UNSET', '2016-09-01|1'),
        ('PITT|Participant.biospecimenSamples.UNSET', '2016-09-01|1'),
        ('PITT|Participant.ethnicity.UNSET', '2016-09-01|1'),
        ('PITT|Participant.hpoId.PITT', '2016-09-01|1'),
        ('PITT|Participant.membershipTier.UNSET', '2016-09-01|1'),
        ('PITT|Participant.meta.NOPE', '2016-09-01|1'),
        ('PITT|Participant.physicalEvaluation.UNSET', '2016-09-01|1'),
        ('PITT|Participant.race.UNSET', '2016-09-01|1'),
        ('PITT|Participant.state.UNSET', '2016-09-01|1'),
        ('PITT|Participant.ageRange.UNSET', '2016-09-01|-1'),
        ('PITT|Participant.ageRange.36-45', '2016-09-01|1'),
        ('PITT|Participant.ethnicity.UNSET', '2016-09-01|-1'),
        ('PITT|Participant.membershipTier.UNSET', '2016-09-01|-1'),
        ('PITT|Participant.meta.NOPE', '2016-09-01|-1'),
        ('PITT|Participant.race.UNSET', '2016-09-01|-1'),
        ('PITT|Participant.state.UNSET', '2016-09-01|-1'),
        ('PITT|Participant.ethnicity.NON_HISPANIC', '2016-09-01|1'),
        ('PITT|Participant.membershipTier.REGISTERED', '2016-09-01|1'),
        ('PITT|Participant.meta.R1', '2016-09-01|1'),
        ('PITT|Participant.race.WHITE', '2016-09-01|1'),
        ('PITT|Participant.state.TX', '2016-09-01|1'),
        ('PITT|Participant.ethnicity.NON_HISPANIC', '2016-09-01|-1'),
        ('PITT|Participant.race.WHITE', '2016-09-01|-1'),
        ('PITT|Participant.membershipTier.REGISTERED', '2016-09-01|-1'),
        ('PITT|Participant.meta.R1', '2016-09-01|-1'),
        ('PITT|Participant.ethnicity.SKIPPED', '2016-09-01|1'),
        ('PITT|Participant.race.UNMAPPED', '2016-09-01|1'),
        ('PITT|Participant.membershipTier.FULL_PARTICIPANT', '2016-09-01|1'),
        ('PITT|Participant.meta.NOPE', '2016-09-01|1'),
        ('PITT|Participant.membershipTier.FULL_PARTICIPANT', '2016-09-01|-1'),
        ('PITT|Participant.meta.NOPE', '2016-09-01|-1'),
        ('PITT|Participant.membershipTier.REGISTERED', '2016-09-01|1'),
        ('PITT|Participant.meta.R1', '2016-09-01|1'),
        ('PITT|Participant.biospecimenSamples.UNSET', '2016-09-01|-1'),
        ('PITT|Participant.biospecimenSamples.SAMPLES_ARRIVED', '2016-09-01|1'),                
        ('PITT|Participant.physicalEvaluation.UNSET', '2016-09-05|-1'),
        ('PITT|Participant.physicalEvaluation.COMPLETE', '2016-09-05|1'),
        ('PITT|Participant.membershipTier.REGISTERED', '2016-09-10|-1'),
        ('PITT|Participant.meta.R1', '2016-09-10|-1'),
        ('PITT|Participant.membershipTier.VOLUNTEER', '2016-09-10|1'),
        ('PITT|Participant.meta.NOPE', '2016-09-10|1'),
        ('PITT|Participant.state.TX', '2016-09-11|-1'),
        ('PITT|Participant.state.CA', '2016-09-11|1')
        ]
    self._compare_json_list(sorted(expected), sorted(results))

  def test_map_key_to_summary_participant_ages(self):
    key = ndb.Key(participant.Participant, '1')
    link = participant.ProviderLink(primary=True,
                                    organization=fhir_datatypes.FHIRReference(reference='Organization/PITT'))
    # One participant signs up in 2013
    participant.DAO.insert(participant.Participant(key=key,
                                                   providerLink = [ link ]),
                          datetime.datetime(2013, 9, 1, 11, 0, 1))
    summary_key = ndb.Key(participant_summary.ParticipantSummary,
                          participant_summary.SINGLETON_SUMMARY_ID,
                          parent=key)
    summary = participant_summary.ParticipantSummary(key=summary_key,
                                                     dateOfBirth=datetime.datetime(1970, 8, 21))
    participant_summary.DAO.store(summary)
    questionnaire_json = json.loads(open(_data_path('questionnaire_example.json')).read())
    questionnaire_key = questionnaire.DAO.store(questionnaire.DAO.from_json(questionnaire_json, None, questionnaire.DAO.allocate_id()))
    # REGISTERED when signed up.
    questionnaire_response.DAO.store(self.make_questionnaire_response(key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.REGISTERED)]),
                                     datetime.datetime(2013, 9, 1, 11, 0, 1))
    # FULL_PARTICIPANT two years later
    questionnaire_response.DAO.store(self.make_questionnaire_response(key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.FULL_PARTICIPANT)]),
                                     datetime.datetime(2015, 9, 1, 11, 0, 2))
    results = list(metrics_pipeline.map(key.to_old_key(),
                                        datetime.datetime(2016, 10, 17)))
    expected = [
        ('PITT|Participant', '2013-09-01|1'),
        ('PITT|Participant.ageRange.UNSET', '2013-09-01|1'),
        ('PITT|Participant.biospecimenSamples.UNSET', '2013-09-01|1'),
        ('PITT|Participant.ethnicity.UNSET', '2013-09-01|1'),
        ('PITT|Participant.hpoId.PITT', '2013-09-01|1'),
        ('PITT|Participant.membershipTier.UNSET', '2013-09-01|1'),
        ('PITT|Participant.meta.NOPE', '2013-09-01|1'),
        ('PITT|Participant.physicalEvaluation.UNSET', '2013-09-01|1'),
        ('PITT|Participant.race.UNSET', '2013-09-01|1'),
        ('PITT|Participant.state.UNSET', '2013-09-01|1'),
        ('PITT|Participant.ageRange.UNSET', '2013-09-01|-1'),
        ('PITT|Participant.ageRange.36-45', '2013-09-01|1'),
        ('PITT|Participant.membershipTier.UNSET', '2013-09-01|-1'),
        ('PITT|Participant.meta.NOPE', '2013-09-01|-1'),
        ('PITT|Participant.ethnicity.UNSET', '2013-09-01|-1'),
        ('PITT|Participant.race.UNSET', '2013-09-01|-1'),
        ('PITT|Participant.state.UNSET', '2013-09-01|-1'),
        ('PITT|Participant.membershipTier.REGISTERED', '2013-09-01|1'),
        ('PITT|Participant.meta.R1', '2013-09-01|1'),
        ('PITT|Participant.ethnicity.SKIPPED', '2013-09-01|1'),
        ('PITT|Participant.race.SKIPPED', '2013-09-01|1'),
        ('PITT|Participant.state.SKIPPED', '2013-09-01|1'),
        ('PITT|Participant.membershipTier.REGISTERED', '2015-09-01|-1'),
        ('PITT|Participant.meta.R1', '2015-09-01|-1'),
        ('PITT|Participant.membershipTier.FULL_PARTICIPANT', '2015-09-01|1'),
        ('PITT|Participant.meta.NOPE', '2015-09-01|1'),
        ('PITT|Participant.ageRange.36-45', '2016-08-21|-1'),
        ('PITT|Participant.ageRange.46-55', '2016-08-21|1')
    ]
    self._compare_json_list(sorted(expected), sorted(results))

  def test_reduce(self):
    reducer_values = [
      '2016-09-01|1',
      '2016-09-01|-1',
      '2016-09-01|1',
      '2016-09-01|1',
      '2016-09-02|1',
      '2016-09-03|-1',
      '2016-09-04|1',
      '2016-09-07|1',
      '2016-09-07|3',
      '2016-09-10|1',
    ]
    metrics.set_pipeline_in_progress()
    results = list(metrics_pipeline.reduce('PITT|Participant', reducer_values, NOW))
    expected = [
      'PITT|Participant|2016-09-01|2\n',
      'PITT|Participant|2016-09-02|3\n',
      'PITT|Participant|2016-09-03|2\n',
      'PITT|Participant|2016-09-04|3\n',
      'PITT|Participant|2016-09-05|3\n',
      'PITT|Participant|2016-09-06|3\n',
      'PITT|Participant|2016-09-07|7\n',
      'PITT|Participant|2016-09-08|7\n',
      'PITT|Participant|2016-09-09|7\n',
      'PITT|Participant|2016-09-10|8\n',
      'PITT|Participant|2016-09-11|8\n',
      'PITT|Participant|2016-09-12|8\n',
    ]
    self.assertEquals(sorted(expected), sorted(results))

  def test_end_to_end(self):
    key = ndb.Key(participant.Participant, '1')
    self._populate_sample_history(key)    
    metrics_pipeline.MetricsPipeline('pmi-drc-biobank-test.appspot.com', NOW).start()
    test_support.execute_until_empty(self.taskqueue)

    serving_version = metrics.get_serving_version()
    metrics_list = list(metrics.MetricsBucket.query(ancestor=serving_version)
                        .order(metrics.MetricsBucket.date).fetch())
    # Twelve dates, * and PITT for each
    self.assertEquals(24, len(metrics_list))
    for i in range(0, 12):
      all_metrics = metrics_list[i * 2]
      pitt_metrics = metrics_list[(i * 2) + 1]
      self.assertEquals(datetime.date(2016, 9, 1 + i), all_metrics.date)
      self.assertEquals('*', all_metrics.hpoId)
      self.assertEquals(datetime.date(2016, 9, 1 + i), pitt_metrics.date)
      self.assertEquals('PITT', pitt_metrics.hpoId)    
      self.assertEquals(all_metrics.metrics, pitt_metrics.metrics)

    for i in range(0, 4):
      all_metrics = json.loads(metrics_list[i * 2].metrics)
      self.assertEquals(1, all_metrics['Participant'])
      self.assertEquals(1, all_metrics['Participant.membershipTier.REGISTERED'])
      self.assertEquals(1, all_metrics['Participant.hpoId.PITT'])
      self.assertEquals(1, all_metrics['Participant.physicalEvaluation.UNSET'])   
      self.assertFalse(all_metrics.get('Participant.physicalEvaluation.COMPLETE'))
      self.assertFalse(all_metrics.get('Participant.membershipTier.VOLUNTEER'))

    for i in range(4, 9):
      all_metrics = json.loads(metrics_list[i * 2].metrics)
      self.assertEquals(1, all_metrics['Participant'])
      self.assertEquals(1, all_metrics['Participant.membershipTier.REGISTERED'])
      self.assertEquals(1, all_metrics['Participant.hpoId.PITT'])
      self.assertEquals(1, all_metrics['Participant.physicalEvaluation.COMPLETE']) 
      self.assertFalse(all_metrics.get('Participant.physicalEvaluation.UNSET')) 
      self.assertFalse(all_metrics.get('Participant.membershipTier.VOLUNTEER')) 

    for i in range(9, 12):
      all_metrics = json.loads(metrics_list[i * 2].metrics)
      self.assertEquals(1, all_metrics['Participant'])
      self.assertEquals(1, all_metrics['Participant.membershipTier.VOLUNTEER'])
      self.assertEquals(1, all_metrics['Participant.hpoId.PITT'])
      self.assertEquals(1, all_metrics['Participant.physicalEvaluation.COMPLETE']) 
      self.assertFalse(all_metrics.get('Participant.physicalEvaluation.UNSET'))
      self.assertFalse(all_metrics.get('Participant.membershipTier.REGISTERED'))
   
    serving_version = metrics.get_serving_version()    
    all_buckets = list(metrics.SERVICE.get_metrics(metrics.MetricsRequest(), serving_version))
    self.assertEquals(24, len(all_buckets))
    for i in range(0, 24):
      bucket = json.loads(all_buckets[i])
      metrics_entry = metrics_list[i]
      facets = bucket['facets']
      self.assertEquals(metrics_entry.date.isoformat(), facets['date'])
      if metrics_entry.hpoId == '*':
        self.assertFalse(facets.get('hpoId'))
      else:          
        self.assertEquals(metrics_entry.hpoId, facets['hpoId'])
      self.assertEquals(json.loads(metrics_entry.metrics), bucket['entries'])
    
    request = metrics.MetricsRequest(start_date='2016-09-02', end_date='2016-09-04')
    sub_buckets = list(metrics.SERVICE.get_metrics(request, serving_version))
    self.assertEquals(6, len(sub_buckets))
    for i in range(0, 6):
      bucket = json.loads(sub_buckets[i])
      metrics_entry = metrics_list[i + 2]
      facets = bucket['facets']
      self.assertEquals(metrics_entry.date.isoformat(), facets['date'])
      if metrics_entry.hpoId == '*':
        self.assertFalse(facets.get('hpoId'))
      else:          
        self.assertEquals(metrics_entry.hpoId, facets['hpoId'])
      self.assertEquals(json.loads(metrics_entry.metrics), bucket['entries'])
    

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

  def _populate_sample_history(self, key):
    link = participant.ProviderLink(primary=True,
                                    organization=fhir_datatypes.FHIRReference(reference='Organization/PITT'))
    participant.DAO.insert(participant.Participant(key=key,
                                                   providerLink = [ link ]),
                          datetime.datetime(2016, 9, 1, 11, 0, 1))
    summary_key = ndb.Key(participant_summary.ParticipantSummary,
                          participant_summary.SINGLETON_SUMMARY_ID,
                          parent=key)
    summary = participant_summary.ParticipantSummary(key=summary_key,
                                                     dateOfBirth=datetime.datetime(1975, 8, 21))
    participant_summary.DAO.store(summary)
    self.populate_questionnaire_responses(key)
    key = ndb.Key(key.flat()[0], key.flat()[1], evaluation.Evaluation, evaluation.DAO.allocate_id())
    evaluation.DAO.store(evaluation.Evaluation(key=key, resource="ignored"),
                         datetime.datetime(2016, 9, 5))
    sample_dict_1 = {
        'familyId': 'SF160914-000001',
        'sampleId': '16258000008',
        'storageStatus': 'In Prep',
        'type': 'Urine',
        'testCode': '1ED10',
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

  def populate_questionnaire_responses(self, participant_key):
    questionnaire_json = json.loads(open(_data_path('questionnaire_example.json')).read())
    questionnaire_key = questionnaire.DAO.store(questionnaire.DAO.from_json(questionnaire_json, None, questionnaire.DAO.allocate_id()))
    unmapped_race = concepts.Concept(concepts.SYSTEM_RACE, 'unmapped-race')
    # Set race, ethnicity, state, and membership tier on 9/1/2016
    questionnaire_response.DAO.store(self.make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("race", concepts.WHITE),
                                                                  ("ethnicity", concepts.NON_HISPANIC),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['TX']),
                                                                  ("membershipTier", concepts.REGISTERED)]),
                                     datetime.datetime(2016, 9, 1, 11, 0, 2))
    # Accidentally change status to FULL_PARTICIPANT; don't fill out ethnicity and put in an unmapped race
    questionnaire_response.DAO.store(self.make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.FULL_PARTICIPANT),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['TX']),
                                                                  ("race", unmapped_race)]),
                                     datetime.datetime(2016, 9, 1, 11, 0, 3))
    # Change it back to REGISTERED
    questionnaire_response.DAO.store(self.make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.REGISTERED),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['TX']),
                                                                  ("race", unmapped_race)]),
                                     datetime.datetime(2016, 9, 1, 11, 0, 4))
    # Note that on 9/5, an evaluation is entered.

    # Change to VOLUNTEER on 9/10
    questionnaire_response.DAO.store(self.make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.VOLUNTEER),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['TX']),
                                                                  ("race", unmapped_race)]),
                                     datetime.datetime(2016, 9, 10, 11, 0, 1))

    # Change state on 9/11
    questionnaire_response.DAO.store(self.make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.VOLUNTEER),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['CA']),
                                                                  ("race", unmapped_race)]),
                                     datetime.datetime(2016, 9, 11, 11, 0, 2))


  def make_questionnaire_response(self, participant_id, questionnaire_id, answers):
    results = []
    for answer in answers:
      results.append({ "linkId": answer[0],
                       "answer": [
                          { "valueCoding": {
                            "code": answer[1].code,
                            "system": answer[1].system
                          }
                        }]
                    })
    return questionnaire_response.DAO.from_json({"resourceType": "QuestionnaireResponse",
            "status": "completed",
            "subject": { "reference": "Patient/{}".format(participant_id) },
            "questionnaire": { "reference": "Questionnaire/{}".format(questionnaire_id) },
            "group": {
              "question": results
            }
            }, participant_id, questionnaire_response.DAO.allocate_id())

def _data_path(filename):
  return os.path.join(os.path.dirname(__file__), '..', 'test-data', filename)

if __name__ == '__main__':
  unittest.main()
