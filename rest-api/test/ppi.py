"""Simple example for calling the ppi/fhir endpoints.

Aslo serves as end to end test to exercise each of these REST APIs.
"""
import copy
import httplib2
import json
import StringIO
import unittest
from email.generator import Generator


from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
CREDS_FILE = './test-data/test-client-cert.json'
#API_ROOT = 'https://pmi-rdr-api-test.appspot.com/_ah/api'
API_ROOT = 'http://localhost:8080'

class TestPPI(unittest.TestCase):

  def setUp(self):
    self.maxDiff = None
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE,
                                                                 [SCOPE])
    self.http = credentials.authorize(httplib2.Http())
    self.base_url = '{}/ppi/fhir'.format(API_ROOT)
    self.headers = {
        'Content-Type': 'application/json; charset=UTF-8',
    }

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

    for json_file in questionnaire_response_files:
      with open(json_file) as f:
        questionnaire_response = json.load(f)
        self.round_trip('QuestionnaireResponse', questionnaire_response)

  def round_trip(self, path, resource):
    url = '{}/{}'.format(self.base_url, path)
    _, content = self.http.request(
        url, 'POST', headers=self.headers, body=json.dumps(resource))
    response = json.loads(content)
    q_id = response['id']
    if 'id' not in resource:
      del response['id']
    self._compare_json(resource, response)

    _, content = self.http.request('{}/{}'.format(url, q_id), 'GET')
    response = json.loads(content)
    if 'id' not in resource:
      del response['id']
    self._compare_json(resource, response)

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
