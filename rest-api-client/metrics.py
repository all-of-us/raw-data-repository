"""Simple end to end test to exercise the participant and evaluation APIs.
"""
import datetime
import googleapiclient
import httplib2
import pprint

from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
CREDS_FILE = '../rest-api/test/test-data/test-client-cert.json'
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



  total_request = {
      'metric': 'PARTICIPANT_TOTAL',
  }


  response = service.metrics().calculate(
      body=total_request).execute()
  pprint.pprint(response)


  enrollment_request = {
      'metric': 'PARTICIPANT_MEMBERSHIP_TIER',
  }

  response = service.metrics().calculate(
      body=enrollment_request).execute()
  pprint.pprint(response)



if __name__ == '__main__':
  main()
