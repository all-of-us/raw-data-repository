"""Simple example for calling the ppi/fhir endpoints.

Also serves as end to end test to exercise each of these REST APIs.
"""

import json
import unittest

import test_util


class TestPPI(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('ppi/fhir')

  def test_questionnaires(self):
    questionnaire_files = [
        'test-data/questionnaire1.json',
        # Example from vibrent.
        'test-data/questionnaire2.json',
    ]

    for json_file in questionnaire_files:
      with open(json_file) as f:
        questionnaire = json.load(f)
        test_util.round_trip(self, self.client, 'Questionnaire', questionnaire)

  def test_questionnaire_responses(self):
    questionnaire_response_files = [
        # Stripped down version of the official FHIR example
        #'test-data/questionnaire_response1.json',
        # Example response from vibrent.  Doesn't pass validation.
        #'test-data/questionnaire_response2.json',
        'test-data/questionnaire_response3.json',
    ]
    participant_id = test_util.create_participant(
        'Bovine', 'Knickers', '1970-10-10')
    for json_file in questionnaire_response_files:
      with open(json_file) as f:
        resource = json.load(f)
        resource['subject']['reference'] = \
            resource['subject']['reference'].format(
                participant_id=participant_id)
        test_util.round_trip(
            self, self.client, 'QuestionnaireResponse', resource)


if __name__ == '__main__':
  unittest.main()
