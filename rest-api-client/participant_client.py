"""Simple end to end test to exercise the participant and evaluation APIs.
"""

import datetime
import pprint

import common


def main():
  client = common.Client('participant/v1')

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
  except common.HttpException, e:
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

  evaluation_id = "5"
  # Now add an evaluation for that participant.
  evaluation = {
      'participant_drc_id': drc_internal_id,
      'evaluation_id': evaluation_id,
  }
  response = client.request_json(
      'participants/{}/evaluation'.format(drc_internal_id), 'POST', evaluation)
  if response['evaluation_id'] != evaluation_id:
    raise StandardError()

  time = datetime.datetime(2016, 9, 2, 10, 30, 15)
  evaluation_data = "{'some_key': 'someval'}"
  response['completed'] = time.isoformat()
  response['evaluation_data'] = evaluation_data
  response = client.request_json('participants/{}/evaluation/{}'.format(
      drc_internal_id, evaluation_id), 'PATCH', response)
  pprint.pprint(response)
  if response['completed'] != '2016-09-02T10:30:15':
    raise StandardError(response['completed'])

  # Try updating a bad id.
  response['evaluation_id'] = 'BAD_ID'
  try:
    response = client.request_json(
        'participants/{}/evaluation/BAD_ID'.format(drc_internal_id))
    raise StandardError() # Should throw.
  except common.HttpException, e:
    if e.code != 404:
      raise StandardError()

  if response['evaluation_data'] != evaluation_data:
    raise StandardError()

  response = client.request_json(
      'participants/{}/evaluation'.format(drc_internal_id))
  for evaluations in response['items']:
    if (evaluations['participant_drc_id'] == drc_internal_id
        and evaluations['evaluation_id'] == evaluation_id):
      break
  else:
    raise StandardError()

  response = client.request_json(
            'participants/NOT_AN_ID/evaluation'.format(drc_internal_id))
  if 'items' in response and len(response['items']):
    raise StandardError()

  print "It worked!!!"


if __name__ == '__main__':
  main()
