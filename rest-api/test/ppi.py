"""Simple example for calling the ppi/fhir endpoints.

Aslo serves as end to end test to exercise each of these REST APIs.
"""
import copy
import httplib2
import pprint
import json
import unittest


from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
CREDS_FILE = './test-data/test-client-cert.json'
#API_ROOT = 'https://pmi-rdr-api-test.appspot.com/_ah/api'
API_ROOT = 'http://localhost:8080/_ah/api'

class TestPPI(unittest.TestCase):

  def setUp(self):
    self.maxDiff = None
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE,
                                                                 [SCOPE])
    http = httplib2.Http()
    http = credentials.authorize(http)

    # Build a service object for interacting with the API.
    api = 'participant'
    version = 'v1'
    discovery_url = '%s/discovery/v1/apis/%s/%s/rest' % (API_ROOT, api, version)
    pprint.pprint(discovery_url)
    self.service = discovery.build(api, version,
                                   discoveryServiceUrl=discovery_url, http=http,
                                   cache_discovery=False)

  def test_questionnaires(self):
    questionnaire_files = [
        'test-data/questionnaire1.json',
        # Example from vibrent.
        'test-data/questionnaire2.json',
    ]

    for json_file in questionnaire_files:
      with open(json_file) as f:
        questionnaire = json.load(f)
        self.round_trip_questionnaire(questionnaire)

  def test_questionnaire_responses(self):
    questionnaire_response_files = [
        # Stripped down version of the official FHIR example
        'test-data/questionnaire_response1.json',
        # Example response from vibrent.
        'test-data/questionnaire_response2.json',
    ]

    for json_file in questionnaire_response_files:
      with open(json_file) as f:
        questionnaire_response = json.load(f)
        self.round_trip_questionnaire_responses(questionnaire_response)

  def round_trip_questionnaire(self, questionnaire):
    questionnaire['id'] = None
    questionnaire_service = self.service.ppi().fhir().questionnaire()

    response = questionnaire_service.insert(body=questionnaire).execute()
    questionnaire_id = response['id']
    print questionnaire_id

    response = questionnaire_service.get(id=questionnaire_id).execute()
    self._compare_json(questionnaire, response)

  def round_trip_questionnaire_responses(self, questionnaire_response):
    questionnaire_response['id'] = None
    response_service = self.service.ppi().fhir().questionnaire_response()

    response = response_service.insert(body=questionnaire_response).execute()
    questionnaire_response_id = response['id']
    print questionnaire_response_id

    response = response_service.get(id=questionnaire_response_id).execute()
    self._compare_json(questionnaire_response, response)

  def _compare_json(self, obj_a, obj_b):
    obj_b = copy.deepcopy(obj_b)
    # Clear out the id and fields added by the server before checking.
    obj_b['id'] = None
    if 'etag' in obj_b:
      del obj_b['etag']
    if 'kind' in obj_b:
      del obj_b['kind']
    self.assertMultiLineEqual(pretty(obj_a), pretty(obj_b))



def pretty(obj):
  return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == '__main__':
  unittest.main()
