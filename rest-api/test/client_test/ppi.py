"""Simple example for calling the ppi/fhir endpoints.

Also serves as end to end test to exercise each of these REST APIs.
"""

import json
import unittest

import test_util
from client.client import HttpException

def _questionnaire_response_url(participant_id):
  return 'Participant/{}/QuestionnaireResponse'.format(participant_id)

class TestPPI(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')

  def test_questionnaires(self):
    questionnaire_files = [
        'test-data/questionnaire1.json',
        # Example from vibrent.
        'test-data/questionnaire2.json',
        'test-data/questionnaire_demographics.json',
    ]

    for json_file in questionnaire_files:
      with open(json_file) as f:
        questionnaire = json.load(f)
        test_util.round_trip(self, self.client, 'Questionnaire', questionnaire)

  def test_valid_and_invalid_questionnaire_responses(self):
    questionnaire_response_files = [
        # Stripped down version of the official FHIR example
        #'test-data/questionnaire_response1.json',
        # Example response from vibrent.  Doesn't pass validation.
        #'test-data/questionnaire_response2.json',
        'test-data/questionnaire_response3.json',
    ]
    participant_id = test_util.create_participant(
        self.client, 'Bovine', 'Knickers', '1970-10-10')
    questionnaire_id = test_util.create_questionnaire(
        self.client, 'test-data/questionnaire1.json')
    for json_file in questionnaire_response_files:
      with open(json_file) as f:
        resource = json.load(f)
        # Sending response with the dummy participant id in the file is an error
        with self.assertRaises(HttpException) as context:
          test_util.round_trip(self, self.client,
                               _questionnaire_response_url('{participant_id}'),
                               resource)
        # Fixing participant id but not the questionnaire id is also an error
        good_url = _questionnaire_response_url(participant_id)
        resource['subject']['reference'] = \
            resource['subject']['reference'].format(
                participant_id=participant_id)
        with self.assertRaises(HttpException) as context:
          test_util.round_trip(self, self.client, good_url, resource)
        # Also fixing participant id succeeds
        resource['questionnaire']['reference'] = \
            resource['questionnaire']['reference'].format(
                questionnaire_id=questionnaire_id)
        test_util.round_trip(self, self.client, good_url, resource)
  
  def test_demographic_questionnaire_responses(self):
    questionnaire_response_files = [
        'test-data/questionnaire_response_demographics.json',
    ]
    participant_id = test_util.create_participant(
        self.client, 'Bovine', 'Knickers', '1970-10-10')
    questionnaire_id = test_util.create_questionnaire(
        self.client, 'test-data/questionnaire_demographics.json')
    for json_file in questionnaire_response_files:
      with open(json_file) as f:
        resource = json.load(f)
        # Sending response with the dummy participant id in the file is an error
        good_url = _questionnaire_response_url(participant_id)
        resource['subject']['reference'] = \
            resource['subject']['reference'].format(
                participant_id=participant_id)
        resource['questionnaire']['reference'] = \
            resource['questionnaire']['reference'].format(
                questionnaire_id=questionnaire_id)
        test_util.round_trip(self, self.client, good_url, resource)
    response = self.client.request_json('Participant/{}'.format(participant_id))
    self.assertEqual(response['gender_identity'], 'MALE_TO_FEMALE_TRANSGENDER')

if __name__ == '__main__':
  unittest.main()
