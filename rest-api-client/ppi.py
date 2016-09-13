"""Simple end to end test to exercise each of the REST APIs.
"""
import datetime
import googleapiclient
import httplib2
import pprint
import json

from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
CREDS_FILE = './test-client-cert.json'
#API_ROOT = 'https://pmi-rdr-api-test.appspot.com/_ah/api'
API_ROOT = 'http://localhost:8080/_ah/api'


def main():
  credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE,
                                                                 [SCOPE])
  http = httplib2.Http()
  http = credentials.authorize(http)

  # Build a service object for interacting with the API.
  api = 'participant'
  version = 'v1'
  discovery_url = '%s/discovery/v1/apis/%s/%s/rest' % (API_ROOT, api, version)
  pprint.pprint(discovery_url)
  service = discovery.build(api, version, discoveryServiceUrl=discovery_url,
                             http=http, cache_discovery=False)

  with open('questionnaire.json') as f:
    questionnaire = json.load(f)

  questionnaire['id'] = None
  # Create a participant.
  response = service.ppi().insert(body=questionnaire,
                                  ppi_type='questionnaire').execute()
  pprint.pprint(response)
  if response['status'] != 'draft':
    raise StandardError()
  questionnaire_id = response['id']

  print questionnaire_id
  response = service.ppi().get(id=questionnaire_id).execute()
  pprint.pprint(response)


  print "It worked!!!"


if __name__ == '__main__':
  main()
