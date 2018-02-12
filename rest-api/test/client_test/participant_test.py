import unittest

from base import BaseClientTest
from client import HttpException


class ParticipantTest(BaseClientTest):
  def test_create_and_modify_participant(self):
    provider_link = {
      "primary": True,
      "organization": {
        "reference": "Organization/AZ_TUCSON"
      }
    }

    # Add a provider link to the participant.
    provider_link_2 = {
      "primary": True,
      "organization": {
        "reference": "Organization/PITT",
      }
    }
    # Create a new participant.
    participant = {
        'providerLink': [provider_link_2]
    }

    response = self.client.request_json('Participant', 'POST', participant)
    self.assertJsonEquals(response['providerLink'], [provider_link_2])
    biobank_id = response['biobankId']

    participant_id = response['participantId']

    # Fetch that participant.
    response = self.client.request_json('Participant/{}'.format(participant_id))
    # Test that hpo is == current provider link
    self.assertEqual(response['hpoId'], 'PITT')
    self.assertEqual(response['awardee'], 'PITT')
    new_response = self.client.request_json('Participant/%s' % participant_id, 'PUT', response,
                                            headers={'If-Match': 'W/"1"'})

    last_etag = self.client.last_etag
    # Test that hpo and provider link changed
    self.assertEqual(new_response['hpoId'], 'PITT')
    self.assertEqual(new_response['providerLink'], [provider_link_2])
    self.assertEqual(new_response['awardee'], 'PITT')


    new_response['providerLink'] = [provider_link]
    try:
      response = self.client.request_json(
          'Participant/{}'.format(participant_id), 'PUT', response)
      self.fail("Need If-Match header for update")
    except HttpException, ex:
      self.assertEqual(ex.code, 400)
    try:
      response = self.client.request_json(
          'Participant/{}'.format(participant_id), 'PUT', response,
          headers = { 'If-Match': 'W/"12345"' })
      self.fail("Wrong If-Match header for update")
    except HttpException, ex:
      self.assertEqual(ex.code, 412)
    new_response = self.client.request_json(
          'Participant/{}'.format(participant_id), 'PUT', new_response,
          headers = { 'If-Match': last_etag})

    self.assertEqual(new_response['biobankId'], biobank_id)
    self.assertJsonEquals(new_response['providerLink'], [provider_link])
    self.assertEqual(new_response['hpoId'], 'AZ_TUCSON')
    self.assertEqual(new_response['awardee'], 'AZ_TUCSON')

    new_response['providerLink'] = []
    last_etag = self.client.last_etag

    self.client.request_json(
      'Participant/{}'.format(participant_id), 'PUT', new_response,
      headers={'If-Match': last_etag})
    unset_response = self.client.request_json('Participant/{}'.format(participant_id))
    self.assertEqual(unset_response['providerLink'], [])
    self.assertEqual(unset_response['hpoId'], 'UNSET')
    self.assertEqual(unset_response['organization'], 'UNSET')
    self.assertEqual(unset_response['site'], 'UNSET')

    # Post a new participant
    response = self.client.request_json('Participant', 'POST', participant)
    participant_id = response['participantId']
    last_etag = self.client.last_etag

    response['awardee'] = 'PITT'
    # pair with awardee
    self.client.request_json(
      'Participant/{}'.format(participant_id), 'PUT', response,
      headers={'If-Match': last_etag})

    # Get the participant from DB.
    response = self.client.request_json('Participant/{}'.format(participant_id))

    self.assertEqual(response['awardee'], 'PITT')

    last_etag = self.client.last_etag
    response['awardee'] = 'UNSET'

    self.client.request_json(
      'Participant/{}'.format(participant_id), 'PUT', response,
      headers={'If-Match': last_etag})

    # Get the participant from DB.
    updated_response = self.client.request_json('Participant/{}'.format(participant_id))
    # Ensure that setting awardee to UNSET will unpair at all levels.
    unset_provider = [{u'organization': {u'reference': u'Organization/UNSET'}, u'primary': True}]
    self.assertEqual(updated_response['awardee'], 'UNSET')
    self.assertEqual(updated_response['providerLink'], unset_provider)
    self.assertEqual(updated_response['site'], 'UNSET')
    self.assertEqual(updated_response['organization'], 'UNSET')


if __name__ == '__main__':
  unittest.main()
