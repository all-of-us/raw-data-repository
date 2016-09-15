"""Simple end to end test to exercise each of the REST APIs.
"""
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
    ]

    for json_file in questionnaire_files:
      with open(json_file) as f:
        questionnaire = json.load(f)
        self.round_trip_questionnaire(questionnaire)


  def round_trip_questionnaire(self, questionnaire):
    questionnaire['id'] = None
    # Create a participant.
    response = self.service.ppi().insert(body=questionnaire,
                                         ppi_type='questionnaire').execute()
    questionnaire_id = response['id']
    print questionnaire_id

    response = self.service.ppi().get(id=questionnaire_id).execute()
    # Clear out the ID before checking.
    response['id'] = None
    self.assertMultiLineEqual(pretty(questionnaire), pretty(response))


def pretty(obj):
  return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))


if __name__ == '__main__':
  unittest.main()
