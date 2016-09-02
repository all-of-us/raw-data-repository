import httplib2
import pprint
import sys

from apiclient import discovery
from oauth2client import tools
import oauth2client
from oauth2client.service_account import ServiceAccountCredentials

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
CREDS_FILE = '/usr/local/google/home/geoffreyb/PMI/auth/test-client-cert.json'
#API_ROOT = 'https://pmi-rdr-api-test.appspot.com/_ah/api'
API_ROOT = 'http://localhost:8080/_ah/api'


def main(argv):
  credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE,
                                                                 [SCOPE])

  http = httplib2.Http()
  http = credentials.authorize(http)

  # Build a service object for interacting with the API.
  api = 'participant'
  version = 'v1'
  discovery_url = '%s/discovery/v1/apis/%s/%s/rest' % (API_ROOT, api, version)
  pprint.pprint(discovery_url)
  service = discovery.build(
      api, version, discoveryServiceUrl=discovery_url, http=http, cache_discovery=False)

  name = 'Mr Foo'

  # Create a new participant.
  participant = {
      'name': name,
  }

  # Create a participant.
  response = service.participants().insert(body=participant).execute()
  pprint.pprint(response)
  if response['name'] != name:
    raise StandardError()
  id = response['id']

  # Fetch that participant and print it out.
  response = service.participants().get(id=id).execute()
  pprint.pprint(response)
  if response['name'] != name:
    raise StandardError()

  # Add a field to the participant update it.
  address = '123 Some Street, Cambridge, MA 02142'
  response['address'] = address
  response = service.participants().update(id=id, body=response).execute()
  if response['address'] != address:
    raise StandardError()
  pprint.pprint(response)

  response = service.participants().list().execute()
  # Make sure the newly created participant is in the list.
  for p in response['items']:
    if p['id'] == id:
      break
  else:
    raise StandardError()

  print "It worked!!!"

if __name__ == '__main__':
  main(sys.argv)
