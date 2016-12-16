"""Tests for participant."""

import datetime
import json
import unittest
import os

import biobank_order
import evaluation
import participant
import questionnaire
import questionnaire_response

from google.appengine.ext import ndb
from test.unit_test.unit_test_util import NdbTestBase, TestBase, to_dict_strip_last_modified

class ParticipantNdbTest(NdbTestBase):
  """Participant test cases requiring the NDB testbed."""
  def test_load_history_entities(self):
    dates = [datetime.datetime(2015, 9, d) for d in range(1, 7)]
    test_data = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'test-data')
    participant_id = '1'
    participant_key = ndb.Key(participant.Participant, participant_id)
    participant_entry = participant.Participant(
        key=participant_key,
        biobankId=None)
    participant.DAO.insert(participant_entry, dates[0])

    questionnaire_id = questionnaire.DAO.allocate_id()
    questionnaire_key = ndb.Key(questionnaire.Questionnaire, questionnaire_id)
    with open(os.path.join(test_data, 'questionnaire1.json')) as rfile:
      questionnaire_entry = questionnaire.Questionnaire(
          key=questionnaire_key, resource=json.load(rfile))
    questionnaire.DAO.store(questionnaire_entry, dates[1])

    response_key = ndb.Key(
        participant_key.flat()[0], participant_key.flat()[1],
        questionnaire_response.QuestionnaireResponse, questionnaire_response.DAO.allocate_id())
    with open(os.path.join(test_data, 'questionnaire_response3.json')) as rfile:
      resource = json.load(rfile)
    resource['subject']['reference'] = resource['subject']['reference'].format(
        participant_id=participant_id)
    resource['questionnaire']['reference'] = resource['questionnaire']['reference'].format(
        questionnaire_id=questionnaire_id)
    response_entry = questionnaire_response.QuestionnaireResponse(key=response_key,
                                                                  resource=resource)
    questionnaire_response.DAO.store(response_entry, dates[2])

    evaluation_key = ndb.Key(participant_key.flat()[0], participant_key.flat()[1],
                             evaluation.Evaluation, evaluation.DAO.allocate_id())
    evaluation_entry = evaluation.Evaluation(key=evaluation_key, resource='notused_eval')
    evaluation.DAO.store(evaluation_entry, dates[3])

    biobank_key = ndb.Key(participant_key.flat()[0], participant_key.flat()[1],
                          biobank_order.BiobankOrder,
                          biobank_order.DAO.allocate_id())
    biobank_entry = biobank_order.BiobankOrder(key=biobank_key, subject='foo')
    biobank_order.DAO.store(biobank_entry, dates[4])

    entries = sorted(
        participant.load_history_entities(participant_key, dates[5]),
        key=lambda m: m.date)
    entries_json = []

    expected = [participant_entry, response_entry, evaluation_entry, biobank_entry]
    expected_json = []
    for entry in expected:
      expected_json.append(to_dict_strip_last_modified(entry))
      entry.key = None

    entries = [e.obj for e in entries]
    for entry in entries:
      entries_json.append(to_dict_strip_last_modified(entry))
      entry.key = None

    self.assertEquals(expected_json, entries_json)

if __name__ == '__main__':
  unittest.main()
