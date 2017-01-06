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
from offline.metrics_config import FieldDef, FacetDef
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

CONFIGS_FOR_TEST = {
    'Participant': {
        'facets': [
            FacetDef(offline.metrics_config.FacetType.HPO_ID, lambda s: s.get('hpoId', UNSET)),
        ],
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

class MetricsPipelineTest(testutil.HandlerTestBase):
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)
    self.maxDiff = None
    self.longMessage = True
    self.saved_config_fn = offline.metrics_config.get_config
    offline.metrics_config.get_config = (lambda: CONFIGS_FOR_TEST)

  def tearDown(self):
    offline.metrics_config.get_config = self.saved_config_fn

  def test_map_key_to_summary(self):
    key = ndb.Key(participant.Participant, '1')
    self._populate_sample_history(key)
    results = list(metrics_pipeline.map_key_to_summary(key.to_old_key()))

    expected = [
        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant": 1,
              "Participant.ageRange.UNSET": 1,
              "Participant.biospecimenSamples.UNSET": 1,
              "Participant.ethnicity.UNSET": 1,
              "Participant.hpoId.PITT": 1,
              "Participant.membershipTier.UNSET": 1,
              "Participant.meta.NOPE": 1,
              "Participant.physicalEvaluation.UNSET": 1,
              "Participant.race.UNSET": 1,
              "Participant.state.UNSET": 1}),
        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.ageRange.UNSET": -1}),

        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.ageRange.36-45": 1}),
        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.ethnicity.UNSET": -1,
              "Participant.membershipTier.UNSET": -1,
              "Participant.meta.NOPE": -1,
              "Participant.race.UNSET": -1,
              "Participant.state.UNSET": -1}),
        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.ethnicity.NON_HISPANIC": 1,
              "Participant.membershipTier.REGISTERED": 1,
              "Participant.meta.R1": 1,
              "Participant.race.WHITE": 1,
              "Participant.state.TX": 1}),
        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.ethnicity.NON_HISPANIC": -1,
              "Participant.race.WHITE": -1,
              "Participant.membershipTier.REGISTERED": -1,
              "Participant.meta.R1": -1}),
        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.ethnicity.SKIPPED": 1,
              "Participant.race.UNMAPPED": 1,
              "Participant.membershipTier.FULL_PARTICIPANT": 1,
              "Participant.meta.NOPE": 1}),
        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.membershipTier.FULL_PARTICIPANT": -1,
              "Participant.meta.NOPE": -1}),
        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.membershipTier.REGISTERED": 1,
              "Participant.meta.R1": 1}),
        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.biospecimenSamples.UNSET": -1}),
        ({'date': "2016-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.biospecimenSamples.SAMPLES_ARRIVED": 1}),
        ({'date': "2016-09-05", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.physicalEvaluation.UNSET": -1}),
        ({'date': "2016-09-05", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.physicalEvaluation.COMPLETE": 1}),
        ({'date': "2016-09-10", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.membershipTier.REGISTERED": -1,
              "Participant.meta.R1": -1}),
        ({'date': "2016-09-10", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.membershipTier.VOLUNTEER": 1,
              "Participant.meta.NOPE": 1}),
        ({'date': "2016-10-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.state.TX": -1}),
        ({'date': "2016-10-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.state.CA": 1}),
        ]
    expected = [(json.dumps(d), json.dumps(s, sort_keys=True)) for d, s in expected]
    self._compare_json_list(expected, results)

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
    results = list(metrics_pipeline.map_key_to_summary(key.to_old_key(),
                                                       datetime.datetime(2016, 10, 17)))
    expected = [
        ({'date': '2013-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'PITT'}]},
         {
              "Participant": 1,
              "Participant.ageRange.UNSET": 1,
              "Participant.biospecimenSamples.UNSET": 1,
              "Participant.ethnicity.UNSET": 1,
              "Participant.hpoId.PITT": 1,
              "Participant.membershipTier.UNSET": 1,
              "Participant.meta.NOPE": 1,
              "Participant.physicalEvaluation.UNSET": 1,
              "Participant.race.UNSET": 1,
              "Participant.state.UNSET": 1
        }),
        ({'date': "2013-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.ageRange.UNSET": -1}),

        ({'date': "2013-09-01", "facets": [{"type": "HPO_ID", "value": "PITT"}]},
         {
              "Participant.ageRange.36-45": 1}),
        ({'date': '2013-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'PITT'}]},
         {
             'Participant.membershipTier.UNSET': -1,
             'Participant.meta.NOPE': -1,
             "Participant.ethnicity.UNSET": -1,
             "Participant.race.UNSET": -1,
             "Participant.state.UNSET": -1
         }),
        ({'date': '2013-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'PITT'}]},
         {
             'Participant.membershipTier.REGISTERED': 1,
             'Participant.meta.R1': 1,
             "Participant.ethnicity.SKIPPED": 1,
             "Participant.race.SKIPPED": 1,
             "Participant.state.SKIPPED": 1
         }),
        ({'date': '2015-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'PITT'}]},
         {
             'Participant.membershipTier.REGISTERED': -1,
             'Participant.meta.R1': -1,
         }),
        ({'date': '2015-09-01', 'facets': [{'type': 'HPO_ID', 'value': 'PITT'}]},
         {
             'Participant.membershipTier.FULL_PARTICIPANT': 1,
             'Participant.meta.NOPE': 1,
         }),
        ({'date': '2016-08-21', 'facets': [{'type': 'HPO_ID', 'value': 'PITT'}]},
         {
             'Participant.ageRange.36-45': -1,
         }),
        ({'date': '2016-08-21', 'facets': [{'type': 'HPO_ID', 'value': 'PITT'}]},
         {
             'Participant.ageRange.46-55': 1,
         }),
    ]
    expected = [(json.dumps(d), json.dumps(s, sort_keys=True)) for d, s in expected]
    self._compare_json_list(expected, results)

  def test_reduce_facets(self):
    reduce_input = [
        json.dumps({
            'Participant.membershipTier.REGISTERED': 1,
        }),

        # Flips to ENGAGED and back.
        json.dumps({
            'Participant.membershipTier.FULL_PARTICIPANT': 1,
            'Participant.membershipTier.REGISTERED': -1,
        }),
        json.dumps({
            'Participant.membershipTier.FULL_PARTICIPANT': -1,
            'Participant.membershipTier.REGISTERED': 1,
        }),
    ]
    facets_key = json.dumps({'facets': [{'type': 'HPO_ID', 'value': 'PITT'}]})

    metrics.set_pipeline_in_progress()
    results = list(metrics_pipeline.reduce_facets(facets_key, reduce_input))
    self.assertEquals(len(results), 1)
    expected_cnt = Counter(('Participant.membershipTier.REGISTERED',))
    expected_json = json.dumps(expected_cnt)
    self.assertEquals(
        json.dumps([{'type': 'HPO_ID', 'value': 'PITT'}]),
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
    print "ML", metrics_list, len(metrics_list)
    self.assertEquals(datetime.date(2016, 9, 1), metrics_list[0].date)
    self.assertEquals(datetime.date(2016, 9, 5), metrics_list[1].date)
    self.assertEquals(datetime.date(2016, 9, 10), metrics_list[2].date)
    self.assertEquals('[{"type": "HPO_ID", "value": "PITT"}]', metrics_list[0].facets)
    self.assertEquals('[{"type": "HPO_ID", "value": "PITT"}]', metrics_list[1].facets)
    self.assertEquals('[{"type": "HPO_ID", "value": "PITT"}]', metrics_list[2].facets)
    metrics0 = json.loads(metrics_list[0].metrics)
    self.assertEquals(1, metrics0['Participant'])
    self.assertEquals(1, metrics0['Participant.membershipTier.REGISTERED'])
    self.assertEquals(1, metrics0['Participant.hpoId.PITT'])

    metrics1 = json.loads(metrics_list[1].metrics)
    self.assertEquals(-1, metrics1['Participant.physicalEvaluation.UNSET'])
    self.assertEquals(1, metrics1['Participant.physicalEvaluation.COMPLETE'])

    metrics2 = json.loads(metrics_list[2].metrics)
    self.assertEquals(1, metrics2['Participant.membershipTier.VOLUNTEER'])
    self.assertEquals(-1, metrics2['Participant.membershipTier.REGISTERED'])

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

    # Change state on 10/1/2016
    questionnaire_response.DAO.store(self.make_questionnaire_response(participant_key.id(),
                                                                 questionnaire_key.id(),
                                                                 [("membershipTier", concepts.VOLUNTEER),
                                                                  ("stateOfResidence", concepts.STATES_BY_ABBREV['CA']),
                                                                  ("race", unmapped_race)]),
                                     datetime.datetime(2016, 10, 1, 11, 0, 2))


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
