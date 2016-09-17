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

  first_name = 'Mister'
  last_name = 'Pants'
  date_of_birth = datetime.datetime(1975, 8, 21).isoformat();

  # Create a new participant.
  participant = {
      'first_name': first_name,
      'last_name': last_name,
      'date_of_birth': date_of_birth,
  }

  # Create a participant.
  response = service.participants().insert(body=participant).execute()
  pprint.pprint(response)
  if response['first_name'] != first_name:
    raise StandardError()
  drc_internal_id = response['drc_internal_id']

  # Fetch that participant and print it out.
  response = service.participants().get(
      drc_internal_id=drc_internal_id).execute()
  pprint.pprint(response)
  if response['first_name'] != first_name:
    raise StandardError()

  # Add a field to the participant update it.
  zip_code = '02142'
  response['zip_code'] = zip_code
  response = service.participants().update(drc_internal_id=drc_internal_id,
                                           body=response).execute()
  if response['zip_code'] != zip_code:
    raise StandardError()
  pprint.pprint(response)

  try:
    # List request must contain at least last name and birth date.
    response = service.participants().list().execute()
  except googleapiclient.errors.HttpError, e:
    if e.resp.status != 403:
      raise StandardError()

  response = service.participants().list(last_name=last_name,
                                         date_of_birth=date_of_birth).execute()
  # Make sure the newly created participant is in the list.
  for participant in response['items']:
    if (participant['first_name'] != first_name
        or participant['last_name'] != last_name
        or participant['date_of_birth'] != date_of_birth):
      raise StandardError()

    if participant['drc_internal_id'] == drc_internal_id:
      break
  else:
    raise StandardError()

  evaluation_id = "5"
  # Now add an evaluation for that participant.
  evaluation = {
      'participant_drc_id': drc_internal_id,
      'evaluation_id': evaluation_id,
  }
  response = service.evaluations().insert(participant_drc_id=id,
                                          body=evaluation).execute()
  if response['evaluation_id'] != evaluation_id:
    raise StandardError()

  time = datetime.datetime(2016, 9, 2, 10, 30, 15)
  evaluation_data = "{'some_key': 'someval'}"
  response['completed'] = time.isoformat()
  response['evaluation_data'] = evaluation_data
  response = service.evaluations().update(participant_drc_id=drc_internal_id,
                                          evaluation_id=evaluation_id,
                                          body=response).execute()
  pprint.pprint(response)
  if response['completed'] != '2016-09-02T10:30:15':
    raise StandardError()

  # Try updating a bad id.
  response['evaluation_id'] = 'BAD_ID'
  try:
    response = service.evaluations().update(participant_drc_id=drc_internal_id,
                                            evaluation_id='BAD_ID',
                                            body=response).execute()
    raise StandardError() # Should throw.
  except googleapiclient.errors.HttpError, e:
    if e.resp.status != 404:
      raise StandardError()

  if response['evaluation_data'] != evaluation_data:
    raise StandardError()

  response = service.evaluations().list(
      participant_drc_id=drc_internal_id).execute()
  for evaluations in response['items']:
    if (evaluations['participant_drc_id'] == drc_internal_id
        and evaluations['evaluation_id'] == evaluation_id):
      break
  else:
    raise StandardError()

  response = service.evaluations().list(participant_drc_id='NOT_AN_ID').execute()
  if 'items' in response and len(response['items']):
    raise StandardError()

  print "It worked!!!"


if __name__ == '__main__':
  main()
