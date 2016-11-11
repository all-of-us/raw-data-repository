"""Simple end to end test to exercise the participant and evaluation APIs.
"""

import datetime
import unittest

import test_util

from client.client import HttpException
from dateutil.relativedelta import relativedelta


class ParticipantTest(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')

  def testCreateAndModifyParticipant(self):
    first_name = 'Mister'
    last_name = 'O\'Pants'
    date_of_birth = (datetime.datetime.now() - relativedelta(years=40)).date().isoformat()
    physical_evaluation_status = 'COMPLETED'

    # Create a new participant.
    participant = {
        'first_name': first_name,
        'last_name': last_name,
        'date_of_birth': date_of_birth,
        'physical_evaluation_status': physical_evaluation_status,
    }

    response = self.client.request_json('Participant', 'POST', participant)
    self.assertEqual(response['first_name'], first_name)
    self.assertEqual(response['physical_evaluation_status'],
        physical_evaluation_status)
    biobank_id = response['biobank_id']
    self.assertTrue(biobank_id.startswith('B'))

    participant_id = response['participant_id']

    # Fetch that participant.
    response = self.client.request_json('Participant/{}'.format(participant_id))
    self.assertEqual(response['first_name'], first_name)
    last_etag = self.client.last_etag

    # Add fields to the participant.
    zip_code = '02142'
    response['zip_code'] = zip_code
    response['membership_tier'] = 'VOLUNTEER'
    response['consent_time'] = datetime.datetime.now().isoformat()
    response['hpo_id'] = '1234'
    response['biobank_id'] = None
    try:
      response = self.client.request_json(
          'Participant/{}'.format(participant_id), 'PATCH', response)
      self.fail("Need If-Match header for update")
    except HttpException, ex:
      self.assertEqual(ex.code, 412)
    try:
      response = self.client.request_json(
          'Participant/{}'.format(participant_id), 'PATCH', response,
          headers = { 'If-Match': '12345' })
      self.fail("Wrong If-Match header for update")
    except HttpException, ex:
      self.assertEqual(ex.code, 412)    
    response = self.client.request_json(
          'Participant/{}'.format(participant_id), 'PATCH', response,
          headers = { 'If-Match': last_etag})

    self.assertEqual(response['zip_code'], zip_code)
    self.assertEqual(response['membership_tier'], 'VOLUNTEER')
    self.assertTrue('sign_up_time' in response)
    self.assertEqual(response['hpo_id'], '1234')
    self.assertEqual(response['biobank_id'], biobank_id)

    try:
      # List request must contain at least last name and birth date.
      response = self.client.request_json('Participant',
                                          query_args={"last_name": last_name})
      self.fail('List request without last name and birth date should fail.')
    except HttpException, ex:
      self.assertEqual(ex.code, 400)

    args = {
        "first_name": first_name.upper(),
        "last_name": last_name.upper(),
        "date_of_birth": date_of_birth,
    }

    response = self.client.request_json('Participant', query_args=args)
    # Make sure the newly created participant is in the list.
    for participant in response['items']:
      self.assertEqual(participant['first_name'], first_name)
      self.assertEqual(participant['last_name'], last_name)
      self.assertEqual(participant['date_of_birth'], date_of_birth)

      if participant['participant_id'] == participant_id:
        break
    else:
      raise self.fail('Did not encounter newly created participant')

    response = self.client.request_json('Participant/{}/Summary'.format(participant_id))
    expected = {
        'Participant.age_range': '36-45',
        'Participant.biospecimen': 'UNSET',
        'Participant.biospecimen_samples': 'UNSET',
        'Participant.ethnicity': 'UNSET',
        'Participant.biospecimen_summary': 'UNSET',
        'Participant.gender_identity': 'None',
        'Participant.hpo_id': '1234',
        'Participant.membership_tier': 'VOLUNTEER',
        'Participant.physical_evaluation': 'UNSET',
        'Participant.race': 'UNSET',
        'Participant.survey': 'UNSET',
    }
    self.assertEqual(expected, response)


if __name__ == '__main__':
  unittest.main()
