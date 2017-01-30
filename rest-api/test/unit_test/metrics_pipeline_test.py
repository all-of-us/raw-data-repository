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
import measurements
import unittest
import os

from extraction import ExtractionResult, BASE_VALUES, UNSET
from offline import metrics_pipeline
from offline.metrics_config import FieldDef
from collections import Counter
from google.appengine.ext import ndb
from mapreduce import test_support
from testlib import testutil
from unit_test_util import make_questionnaire_response, _data_path

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
            'physicalMeasurements': UNSET,
            'biospecimenSamples': UNSET,
            'biospecimen': UNSET,
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
            'PhysicalMeasurementsHistory': [
                # The presence of physical measurements implies that it is complete.
                FieldDef('physicalMeasurements',
                         lambda h: ExtractionResult('COMPLETE'),
                         ('None', 'COMPLETE')),
            ],
            'BiobankOrderHistory': [
                # The presence of a biobank order implies that an order has been placed.
                FieldDef('biospecimen',
                         lambda h: ExtractionResult('ORDER_PLACED'),
                         (UNSET, 'ORDER_PLACED'))
            ],
            'BiobankSamples': [
               # The presence of a biobank sample implies that samples have arrived
               FieldDef('biospecimenSamples', lambda h: ExtractionResult('SAMPLES_ARRIVED'),
                         ('None', 'SAMPLES_ARRIVED'))
            ]
        },
        'summary_fields': [
            FieldDef('meta', compute_meta, ('R1', 'NOPE')),
        ] + offline.metrics_config.ALL_CONFIG['Participant']['summary_fields'],
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

  def test_map1(self):
    key = ndb.Key(participant.Participant, '1')
    self._populate_sample_history(key, 'PITT')
    results = list(metrics_pipeline.map1(key.to_old_key(), NOW))

    expected = [
        ('PITT|Participant', '2016-09-01|1'),
        ('PITT|Participant.ageRange.UNSET', '2016-09-01|1'),
        ('PITT|Participant.biospecimenSamples.UNSET', '2016-09-01|1'),
        ('PITT|Participant.ethnicity.UNSET', '2016-09-01|1'),
        ('PITT|Participant.hpoId.PITT', '2016-09-01|1'),
        ('PITT|Participant.membershipTier.UNSET', '2016-09-01|1'),
        ('PITT|Participant.meta.NOPE', '2016-09-01|1'),
        ('PITT|Participant.physicalMeasurements.UNSET', '2016-09-01|1'),
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
        ('PITT|Participant.biospecimen.UNSET', '2016-09-01|1'),
        ('PITT|Participant.biospecimenSummary.UNSET', '2016-09-01|1'),
        ('PITT|Participant.consentForStudyEnrollmentAndEHR.UNSET', '2016-09-01|1'),                
        ('PITT|Participant.physicalMeasurements.UNSET', '2016-09-05|-1'),
        ('PITT|Participant.physicalMeasurements.COMPLETE', '2016-09-05|1'),
        ('PITT|Participant.membershipTier.REGISTERED', '2016-09-10|-1'),
        ('PITT|Participant.meta.R1', '2016-09-10|-1'),
        ('PITT|Participant.membershipTier.VOLUNTEER', '2016-09-10|1'),
        ('PITT|Participant.meta.NOPE', '2016-09-10|1'),
        ('PITT|Participant.state.TX', '2016-09-11|-1'),
        ('PITT|Participant.state.CA', '2016-09-11|1')        
        ]
    self._compare_json_list(sorted(expected), sorted(results))

  def test_map1_participant_ages(self):
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
    questionnaire_response.DAO.store(make_questionnaire_response(key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.REGISTERED)]),
                                     datetime.datetime(2013, 9, 1, 11, 0, 1))
    # FULL_PARTICIPANT two years later
    questionnaire_response.DAO.store(make_questionnaire_response(key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.FULL_PARTICIPANT)]),
                                     datetime.datetime(2015, 9, 1, 11, 0, 2))
    results = list(metrics_pipeline.map1(key.to_old_key(),
                                        datetime.datetime(2016, 10, 17)))
    expected = [
        ('PITT|Participant', '2013-09-01|1'),
        ('PITT|Participant.ageRange.UNSET', '2013-09-01|1'),
        ('PITT|Participant.biospecimenSamples.UNSET', '2013-09-01|1'),
        ('PITT|Participant.ethnicity.UNSET', '2013-09-01|1'),
        ('PITT|Participant.hpoId.PITT', '2013-09-01|1'),
        ('PITT|Participant.membershipTier.UNSET', '2013-09-01|1'),
        ('PITT|Participant.meta.NOPE', '2013-09-01|1'),
        ('PITT|Participant.physicalMeasurements.UNSET', '2013-09-01|1'),
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
        ('PITT|Participant.biospecimen.UNSET', '2013-09-01|1'),
        ('PITT|Participant.biospecimenSummary.UNSET', '2013-09-01|1'),
        ('PITT|Participant.consentForStudyEnrollmentAndEHR.UNSET', '2013-09-01|1'),
        ('PITT|Participant.membershipTier.REGISTERED', '2015-09-01|-1'),
        ('PITT|Participant.meta.R1', '2015-09-01|-1'),
        ('PITT|Participant.membershipTier.FULL_PARTICIPANT', '2015-09-01|1'),
        ('PITT|Participant.meta.NOPE', '2015-09-01|1'),
        ('PITT|Participant.ageRange.36-45', '2016-08-21|-1'),
        ('PITT|Participant.ageRange.46-55', '2016-08-21|1')
    ]
    self._compare_json_list(sorted(expected), sorted(results))

  def test_reduce1(self):
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
    results = list(metrics_pipeline.reduce1('PITT|Participant', reducer_values, NOW))
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
    self._populate_sample_history(key, 'PITT')
    key2 = ndb.Key(participant.Participant, '2')
    self._populate_sample_history(key2, 'COLUMBIA')
    metrics_pipeline.MetricsPipeline('pmi-drc-biobank-test.appspot.com', NOW).start()
    test_support.execute_until_empty(self.taskqueue)

    serving_version = metrics.get_serving_version()
    metrics_list = list(metrics.MetricsBucket.query(ancestor=serving_version)
                        .order(metrics.MetricsBucket.date).fetch())
    # Twelve dates, *, COLUMBIA, and PITT for each
    self.assertEquals(36, len(metrics_list))
    for i in range(0, 12):
      self.run_checks_for_all_dates(metrics_list, i)
      
    for i in range(0, 4):
      self.check_first_dates(metrics_list, i)
      
    for i in range(4, 9):
      self.check_second_dates(metrics_list, i)
      
    for i in range(9, 12):
      self.check_third_dates(metrics_list, i)
   
    serving_version = metrics.get_serving_version()    
    all_buckets = list(metrics.SERVICE.get_metrics(metrics.MetricsRequest(), serving_version))
    self.assertEquals(36, len(all_buckets))
    for i in range(0, 36):
      bucket = json.loads(all_buckets[i])
      metrics_entry = metrics_list[i]
      facets = bucket['facets']
      self.assertEquals(metrics_entry.date.isoformat(), facets['date'])
      if metrics_entry.hpoId == '':
        self.assertFalse(facets.get('hpoId'))
      else:          
        self.assertEquals(metrics_entry.hpoId, facets['hpoId'])
      self.assertEquals(json.loads(metrics_entry.metrics), bucket['entries'])
    
    request = metrics.MetricsRequest(start_date='2016-09-02', end_date='2016-09-04')
    sub_buckets = list(metrics.SERVICE.get_metrics(request, serving_version))
    self.assertEquals(9, len(sub_buckets))
    for i in range(0, 9):
      bucket = json.loads(sub_buckets[i])
      metrics_entry = metrics_list[i + 3]
      facets = bucket['facets']
      self.assertEquals(metrics_entry.date.isoformat(), facets['date'])
      if metrics_entry.hpoId == '':
        self.assertFalse(facets.get('hpoId'))
      else:          
        self.assertEquals(metrics_entry.hpoId, facets['hpoId'])
      self.assertEquals(json.loads(metrics_entry.metrics), bucket['entries'])
    
  def run_checks_for_all_dates(self, metrics_list, i):
    all_metrics_bucket = metrics_list[i * 3]
    columbia_metrics_bucket = metrics_list[(i * 3) + 1]
    pitt_metrics_bucket = metrics_list[(i * 3) + 2]
    all_metrics = json.loads(metrics_list[i * 3].metrics)
    columbia_metrics = json.loads(metrics_list[(i * 3) + 1].metrics)
    pitt_metrics = json.loads(metrics_list[(i * 3) + 2].metrics)    
    self.assertEquals(datetime.date(2016, 9, 1 + i), all_metrics_bucket.date)
    self.assertEquals('', all_metrics_bucket.hpoId)
    self.assertEquals(datetime.date(2016, 9, 1 + i), columbia_metrics_bucket.date)
    self.assertEquals('COLUMBIA', columbia_metrics_bucket.hpoId)
    self.assertEquals(datetime.date(2016, 9, 1 + i), pitt_metrics_bucket.date)
    self.assertEquals('PITT', pitt_metrics_bucket.hpoId)
    self.assertEquals(2, all_metrics['Participant'])
    self.assertEquals(1, all_metrics['Participant.hpoId.PITT'])
    self.assertEquals(1, all_metrics['Participant.hpoId.COLUMBIA'])
    self.assertEquals(1, columbia_metrics['Participant'])
    self.assertEquals(1, columbia_metrics['Participant.hpoId.COLUMBIA'])
    self.assertEquals(1, pitt_metrics['Participant'])
    self.assertEquals(1, pitt_metrics['Participant.hpoId.PITT'])
    self.assertFalse(columbia_metrics.get('Participant.hpoId.PITT'))
    self.assertFalse(pitt_metrics.get('Participant.hpoId.COLUMBIA'))  
    
  def check_first_dates(self, metrics_list, i):
    all_metrics = json.loads(metrics_list[i * 3].metrics)
    columbia_metrics = json.loads(metrics_list[(i * 3) + 1].metrics)
    pitt_metrics = json.loads(metrics_list[(i * 3) + 2].metrics)
    self.assertEquals(2, all_metrics['Participant.membershipTier.REGISTERED'])
    self.assertEquals(2, all_metrics['Participant.physicalMeasurements.UNSET'])
    self.assertEquals(1, columbia_metrics['Participant.membershipTier.REGISTERED'])    
    self.assertEquals(1, columbia_metrics['Participant.physicalMeasurements.UNSET'])
    self.assertFalse(columbia_metrics.get('Participant.physicalMeasurements.COMPLETE'))
    self.assertFalse(columbia_metrics.get('Participant.membershipTier.VOLUNTEER'))    
    self.assertEquals(1, pitt_metrics['Participant.membershipTier.REGISTERED'])    
    self.assertEquals(1, pitt_metrics['Participant.physicalMeasurements.UNSET'])
    self.assertFalse(pitt_metrics.get('Participant.physicalMeasurements.COMPLETE'))
    self.assertFalse(pitt_metrics.get('Participant.membershipTier.VOLUNTEER'))
    

  def check_second_dates(self, metrics_list, i):
    all_metrics = json.loads(metrics_list[i * 3].metrics)
    columbia_metrics = json.loads(metrics_list[(i * 3) + 1].metrics)
    pitt_metrics = json.loads(metrics_list[(i * 3) + 2].metrics)
    self.assertEquals(2, all_metrics['Participant.membershipTier.REGISTERED'])
    self.assertEquals(2, all_metrics['Participant.physicalMeasurements.COMPLETE'])
    self.assertFalse(all_metrics.get('Participant.physicalMeasurements.UNSET'))
    self.assertFalse(all_metrics.get('Participant.membershipTier.VOLUNTEER')) 
    self.assertEquals(1, columbia_metrics['Participant.membershipTier.REGISTERED'])
    self.assertEquals(1, columbia_metrics['Participant.physicalMeasurements.COMPLETE'])
    self.assertFalse(columbia_metrics.get('Participant.physicalMeasurements.UNSET'))
    self.assertFalse(columbia_metrics.get('Participant.membershipTier.VOLUNTEER'))
    self.assertEquals(1, pitt_metrics['Participant.membershipTier.REGISTERED'])
    self.assertEquals(1, pitt_metrics['Participant.physicalMeasurements.COMPLETE'])
    self.assertFalse(pitt_metrics.get('Participant.physicalMeasurements.UNSET'))

  def check_third_dates(self, metrics_list, i):
    all_metrics = json.loads(metrics_list[i * 3].metrics)
    columbia_metrics = json.loads(metrics_list[(i * 3) + 1].metrics)
    pitt_metrics = json.loads(metrics_list[(i * 3) + 2].metrics)
    self.assertEquals(2, all_metrics['Participant.membershipTier.VOLUNTEER'])
    self.assertEquals(2, all_metrics['Participant.physicalMeasurements.COMPLETE'])
    self.assertFalse(all_metrics.get('Participant.physicalMeasurements.UNSET'))
    self.assertFalse(all_metrics.get('Participant.membershipTier.REGISTERED'))
    self.assertEquals(1, columbia_metrics['Participant.membershipTier.VOLUNTEER'])
    self.assertEquals(1, columbia_metrics['Participant.physicalMeasurements.COMPLETE'])
    self.assertFalse(columbia_metrics.get('Participant.physicalMeasurements.UNSET'))
    self.assertFalse(columbia_metrics.get('Participant.membershipTier.REGISTERED'))    
    self.assertEquals(1, pitt_metrics['Participant.membershipTier.VOLUNTEER'])
    self.assertEquals(1, pitt_metrics['Participant.physicalMeasurements.COMPLETE'])
    self.assertFalse(pitt_metrics.get('Participant.physicalMeasurements.UNSET'))
    self.assertFalse(pitt_metrics.get('Participant.membershipTier.REGISTERED'))    

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

  def _populate_sample_history(self, key, hpoId):
    org = fhir_datatypes.FHIRReference(reference='Organization/' + hpoId)
    link = participant.ProviderLink(primary=True, organization=org)
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
    key = ndb.Key(key.flat()[0], key.flat()[1], measurements.PhysicalMeasurements,
                  measurements.DAO.allocate_id())
    measurements.DAO.store(measurements.PhysicalMeasurements(key=key, resource="ignored"),
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
    questionnaire_response.DAO.store(make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("race", concepts.WHITE),
                                                                  ("ethnicity", concepts.NON_HISPANIC),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['TX']),
                                                                  ("membershipTier", concepts.REGISTERED)]),
                                     datetime.datetime(2016, 9, 1, 11, 0, 2))
    # Accidentally change status to FULL_PARTICIPANT; don't fill out ethnicity and put in an unmapped race
    questionnaire_response.DAO.store(make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.FULL_PARTICIPANT),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['TX']),
                                                                  ("race", unmapped_race)]),
                                     datetime.datetime(2016, 9, 1, 11, 0, 3))
    # Change it back to REGISTERED
    questionnaire_response.DAO.store(make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.REGISTERED),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['TX']),
                                                                  ("race", unmapped_race)]),
                                     datetime.datetime(2016, 9, 1, 11, 0, 4))
    # Note that on 9/5, physical measurements are entered.

    # Change to VOLUNTEER on 9/10
    questionnaire_response.DAO.store(make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.VOLUNTEER),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['TX']),
                                                                  ("race", unmapped_race)]),
                                     datetime.datetime(2016, 9, 10, 11, 0, 1))

    # Change state on 9/11
    questionnaire_response.DAO.store(make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.VOLUNTEER),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['CA']),
                                                                  ("race", unmapped_race)]),
                                     datetime.datetime(2016, 9, 11, 11, 0, 2))


if __name__ == '__main__':
  unittest.main()
