"""Simple end to end test to exercise the participant and evaluation APIs.
"""

import datetime
import unittest

import test_util

from client.client import Client, HttpException


class ParticipantTest(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('participant/v1')

  def testCreateAndModifyParticipant(self):
    first_name = 'Mister'
    last_name = 'O\'Pants'
    date_of_birth = '1975-08-21'

    # Create a new participant.
    participant = {
        'first_name': first_name,
        'last_name': last_name,
        'date_of_birth': date_of_birth,
    }

    response = self.client.request_json('participants', 'POST', participant)
    self.assertEqual(response['first_name'], first_name)

    participant_id = response['participant_id']

    # Fetch that participant.
    response = self.client.request_json('participants/{}'.format(participant_id))
    self.assertEqual(response['first_name'], first_name)

    # Add fields to the participant.
    zip_code = '02142'
    response['zip_code'] = zip_code
    response['membership_tier'] = 'CONSENTED'
    response['consent_time'] = datetime.datetime.now().isoformat()
    response['hpo_id'] = '1234'
    response = self.client.request_json(
        'participants/{}'.format(participant_id), 'PATCH', response)
    self.assertEqual(response['zip_code'], zip_code)
    self.assertEqual(response['membership_tier'], 'CONSENTED')
    self.assertTrue('sign_up_time' in response)
    self.assertEqual(response['hpo_id'], '1234')

    try:
      # List request must contain at least last name and birth date.
      response = self.client.request_json('participants',
                                          query_args={"last_name": last_name})
      self.fail('List request without last name and birth date should fail.')
    except HttpException, e:
      self.assertEqual(e.code, 400)

    args = {
        "first_name": first_name.upper(),
        "last_name": last_name.upper(),
        "date_of_birth": date_of_birth,
    }

    response = self.client.request_json('participants', query_args=args)
    # Make sure the newly created participant is in the list.
    for participant in response['items']:
      self.assertEqual(participant['first_name'], first_name)
      self.assertEqual(participant['last_name'], last_name)
      self.assertEqual(participant['date_of_birth'], date_of_birth)

      if participant['participant_id'] == participant_id:
        break
    else:
      raise self.fail('Did not encounter newly created participant')

if __name__ == '__main__':
  unittest.main()
