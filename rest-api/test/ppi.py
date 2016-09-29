"""Simple example for calling the ppi/fhir endpoints.

Also serves as end to end test to exercise each of these REST APIs.
"""
import copy
import json
import unittest

from client.client import Client

class TestPPI(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    creds_file = './test-data/test-client-cert.json'
    default_instance = 'http://localhost:8080'
    self.client = Client('ppi/fhir', creds_file, default_instance)
    self.participant_client = Client('participant/v1', creds_file,
                                     default_instance)

  def test_questionnaires(self):
    questionnaire_files = [
        'test-data/questionnaire1.json',
        # Example from vibrent.
        'test-data/questionnaire2.json',
    ]

    for json_file in questionnaire_files:
      with open(json_file) as f:
        questionnaire = json.load(f)
        self.round_trip('Questionnaire', questionnaire)

  def test_questionnaire_responses(self):
    questionnaire_response_files = [
        # Stripped down version of the official FHIR example
        'test-data/questionnaire_response1.json',
        # Example response from vibrent.  Doesn't pass validation.
        #'test-data/questionnaire_response2.json',
    ]
    participant_id = self.create_participant()
    for json_file in questionnaire_response_files:
      with open(json_file) as f:
        resource = json.load(f)
        resource['subject']['reference'] = \
            resource['subject']['reference'].format(
                participant_id=participant_id)
        self.round_trip('QuestionnaireResponse', resource)

  def round_trip(self, path, resource):
    response = self.client.request_json(path, 'POST', resource)
    q_id = response['id']
    del response['id']
    self._compare_json(resource, response)

    response = self.client.request_json('{}/{}'.format(path, q_id), 'GET')
    del response['id']
    self._compare_json(resource, response)

  def create_participant(self):
    participant = {
        'first_name': 'Mother',
        'last_name': 'Shorts',
        'date_of_birth': '1975-08-21',
    }
    response = self.participant_client.request_json(
        'participants', 'POST', participant)
    return response['drc_internal_id']

  def _compare_json(self, obj_a, obj_b):
    obj_b = copy.deepcopy(obj_b)
    if 'etag' in obj_b:
      del obj_b['etag']
    if 'kind' in obj_b:
      del obj_b['kind']
    self.assertMultiLineEqual(pretty(obj_a), pretty(obj_b))


def pretty(obj):
  return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == '__main__':
  unittest.main()
