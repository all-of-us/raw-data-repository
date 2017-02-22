import datetime
import json
import unittest
import os

import biobank_order
import biobank_sample
import concepts
import config
import measurements
import participant
import participant_dao
import participant_enums
import participant_summary
import questionnaire
import questionnaire_response

from google.appengine.ext import ndb
from unit_test_util import NdbTestBase, to_dict_strip_last_modified
from unit_test_util import make_questionnaire_response
from test.test_data import data_path


class ParticipantNdbTest(NdbTestBase):
  def setUp(self):
    super(ParticipantNdbTest, self).setUp()
    config.override_setting(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS,
                            ["foo", "questionnaireOnSociodemographics"])
    config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, ["bar", "1ED04"])

  """Participant test cases requiring the NDB testbed."""
  def test_load_history_entities(self):
    dates = [datetime.datetime(2015, 9, d) for d in range(1, 7)]
    test_data = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'test-data')
    participant_id = '1'
    participant_key = ndb.Key(participant.Participant, participant_id)
    participant_entry = participant.Participant(
        key=participant_key,
        biobankId=None)
    participant_dao.DAO().insert(participant_entry, dates[0])
    
    participant_result = participant_dao.DAO().load(participant_id)
    self.assertTrue(participant_result.biobankId)
    participant_summary_result = participant_summary.DAO().get_summary_for_participant(participant_id)
    self.assertTrue(participant_summary_result)
    self.assertEquals(participant_enums.HPOId.UNSET, participant_summary_result.hpoId)
    self.assertEquals(0, participant_summary_result.numCompletedBaselinePPIModules)
    self.assertEquals(0, participant_summary_result.numBaselineSamplesArrived)
    self.assertEquals(participant_id, participant_summary_result.participantId)
    self.assertEquals(participant_result.biobankId, participant_summary_result.biobankId)    

    questionnaire_id = questionnaire.DAO().allocate_id()
    questionnaire_key = ndb.Key(questionnaire.Questionnaire, questionnaire_id)
    with open(os.path.join(test_data, 'questionnaire1.json')) as rfile:
      questionnaire_entry = questionnaire.Questionnaire(
          key=questionnaire_key, resource=json.load(rfile))
    questionnaire.DAO().store(questionnaire_entry, dates[1])

    response_key = ndb.Key(
        participant_key.flat()[0], participant_key.flat()[1],
        questionnaire_response.QuestionnaireResponse, questionnaire_response.DAO().allocate_id())
    with open(os.path.join(test_data, 'questionnaire_response3.json')) as rfile:
      resource = json.load(rfile)
    resource['subject']['reference'] = resource['subject']['reference'].format(
        participant_id=participant_id)
    resource['questionnaire']['reference'] = resource['questionnaire']['reference'].format(
        questionnaire_id=questionnaire_id)
    response_entry = questionnaire_response.QuestionnaireResponse(key=response_key,
                                                                  resource=resource)
    questionnaire_response.DAO().store(response_entry, dates[2])

    measurements_key = ndb.Key(participant_key.flat()[0], participant_key.flat()[1],
                               measurements.PhysicalMeasurements, measurements.DAO().allocate_id())
    measurements_entry = measurements.PhysicalMeasurements(
        key=measurements_key, resource={'entry': [{'resource': {}}]})
    measurements.DAO().store(measurements_entry, dates[3])

    biobank_key = ndb.Key(participant_key.flat()[0], participant_key.flat()[1],
                          biobank_order.BiobankOrder,
                          biobank_order.DAO().allocate_id())
    biobank_entry = biobank_order.BiobankOrder(key=biobank_key, subject='foo')
    biobank_order.DAO().store(biobank_entry, dates[4])

    entries = sorted(
        participant_dao.load_history_entities(participant_key, dates[5]),
        key=lambda m: m.date)
    entries_json = []

    expected = [participant_entry, response_entry, measurements_entry, biobank_entry]
    expected_json = []
    for entry in expected:
      expected_json.append(to_dict_strip_last_modified(entry))
      entry.key = None

    entries = [e.obj for e in entries]
    for entry in entries:
      entries_json.append(to_dict_strip_last_modified(entry))
      entry.key = None

    self.assertEquals(expected_json, entries_json)

  def test_regenerate_summary_no_participant(self):
    participant_key = ndb.Key(participant.Participant, '1')
    self.assertFalse(participant_dao.DAO().regenerate_summary(participant_key))

  def test_regenerate_summary_participant(self):
    participant_id = '1'
    participant_key = ndb.Key(participant.Participant, participant_id)
    participant_entry = participant.Participant(
        key=participant_key,
        biobankId=None)
    participant_dao.DAO().insert(participant_entry, datetime.datetime(2015, 9, 1))

    questionnaire_json = json.loads(open(data_path('questionnaire_example.json')).read())
    questionnaire_key = questionnaire.DAO().store(
        questionnaire.DAO().from_json(questionnaire_json, None, questionnaire.DAO().allocate_id()))
    response = make_questionnaire_response(participant_key.id(),
                                           questionnaire_key.id(),
                                           [("race", concepts.WHITE),
                                            ("ethnicity", concepts.NON_HISPANIC),
                                            ("stateOfResidence", concepts.STATES_BY_ABBREV['TX']),
                                            ("membershipTier", concepts.REGISTERED)])
    questionnaire_response.DAO().store(response, datetime.datetime(2016, 9, 1, 11, 0, 2))

    samples_json = { "samples": [ {"testCode": "xxx"}, {"testCode": "1ED04"} ]}
    samples = biobank_sample.DAO().from_json(samples_json, participant_id,
                                           biobank_sample.SINGLETON_SAMPLES_ID)
    biobank_sample.DAO().store(samples)

    participant_result = participant_dao.DAO().load(participant_id)
    self.assertTrue(participant_result.biobankId)
    summary = participant_summary.DAO().get_summary_for_participant(participant_id)
    self.assertTrue(summary)
    self.assertEquals(participant_enums.Race.WHITE, summary.race)
    self.assertEquals(participant_enums.Ethnicity.NON_HISPANIC, summary.ethnicity)
    self.assertEquals(participant_enums.MembershipTier.REGISTERED, summary.membershipTier)
    self.assertEquals(1, summary.numCompletedBaselinePPIModules)
    self.assertEquals(1, summary.numBaselineSamplesArrived)

    # Nothing has changed; no summary is returned.
    self.assertFalse(participant_dao.DAO().regenerate_summary(participant_key))

    # Delete the participant summary.
    summary.key.delete()
    self.assertFalse(participant_summary.DAO().get_summary_for_participant(participant_id))

    new_summary = participant_dao.DAO().regenerate_summary(participant_key)
    # The new summary should be the same as the old one.
    self.assertEquals(summary, new_summary)
    new_summary = participant_summary.DAO().get_summary_for_participant(participant_id)
    self.assertEquals(summary, new_summary)

if __name__ == '__main__':
  unittest.main()
