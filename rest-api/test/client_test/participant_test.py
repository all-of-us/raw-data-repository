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
    response['providerLink'] = [provider_link]
    new_response = self.client.request_json('Participant/%s' % participant_id, 'PUT', response,
                                            headers={'If-Match': 'W/"1"'})

    last_etag = self.client.last_etag
    # Test that hpo and provider link changed
    self.assertEqual(new_response['hpoId'], 'AZ_TUCSON')
    self.assertEqual(new_response['providerLink'], [provider_link])

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
    response = self.client.request_json(
          'Participant/{}'.format(participant_id), 'PUT', response,
          headers = { 'If-Match': last_etag})
    self.assertEqual(response['biobankId'], biobank_id)

    self.assertJsonEquals(response['providerLink'], [provider_link_2])
    self.assertJsonEquals(new_response['providerLink'], [provider_link])

if __name__ == '__main__':
  unittest.main()
