"""Simple end to end test to exercise the participant and evaluation APIs.
"""

import datetime
import pprint

from client.client import Client, HttpException


def main():
  client = Client('participant/v1')

  first_name = 'Mister'
  last_name = 'Pants'
  date_of_birth = '1975-08-21'

  # Create a new participant.
  participant = {
      'first_name': first_name,
      'last_name': last_name,
      'date_of_birth': date_of_birth,
  }

  # Create a participant.
  response = client.request_json('participants', 'POST', participant)
  pprint.pprint(response)
  if response['first_name'] != first_name:
    raise StandardError()
  drc_internal_id = response['drc_internal_id']

  # Fetch that participant and print it out.
  response = client.request_json('participants/{}'.format(drc_internal_id))
  pprint.pprint(response)
  if response['first_name'] != first_name:
    raise StandardError()

  # Add a field to the participant update it.
  zip_code = '02142'
  response['zip_code'] = zip_code
  response['membership_tier'] = 'CONSENTED'
  response['consent_time'] = datetime.datetime.now().isoformat()
  response['hpo_id'] = '1234'
  response = client.request_json(
      'participants/{}'.format(drc_internal_id), 'PATCH', response)
  pprint.pprint(response)
  if (response['zip_code'] != zip_code
      or response['membership_tier'] != 'CONSENTED'
      or not 'sign_up_time' in response
      or response['hpo_id'] != '1234'):
    raise StandardError()
  pprint.pprint(response)

  try:
    # List request must contain at least last name and birth date.
    response = client.request_json('participants',
                                   query_args={"last_name": last_name})
  except HttpException, e:
    if e.code != 400:
      raise StandardError('Code is {}'.format(e.code))

  args = {
      "first_name": first_name,
      "last_name": last_name,
      "date_of_birth": date_of_birth,
      }

  response = client.request_json('participants', query_args=args)
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
  print "It worked!!!"


if __name__ == '__main__':
  main()
