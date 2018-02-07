import unittest

from base import BaseClientTest
from client import HttpException


class ParticipantTest(BaseClientTest):
  def test_create_and_modify_participant(self):
    provider_link = {
      "primary": True,
      "organization": {
        "display": None,
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
        'providerLink': [provider_link]
    }

    response = self.client.request_json('Participant', 'POST', participant)
    self.assertJsonEquals(response['providerLink'], [provider_link])
    biobank_id = response['biobankId']

    participant_id = response['participantId']

    # Fetch that participant.
    response = self.client.request_json('Participant/{}'.format(participant_id))
    last_etag = self.client.last_etag


    response['providerLink'] = [  provider_link_2 ]
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

    self.assertJsonEquals(response['providerLink'], [ provider_link_2])

if __name__ == '__main__':
  unittest.main()
