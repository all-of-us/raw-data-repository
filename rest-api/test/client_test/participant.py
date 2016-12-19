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
    provider_link = {
      "primary": False,
      "organization": {
        "display": None,
        "reference": "columbia"
      },
      "site": [{
        "display": None,
        "reference": "columbia-harlem-free-clinic",
      }],
      "identifier": [{
        "system": "http://any-columbia-mrn-system",
        "value": "MRN123"
      }]
    }

    # Create a new participant.
    participant = {
        'providerLink': [provider_link]
    }

    response = self.client.request_json('Participant', 'POST', participant)
    test_util._compare_json(self, response['providerLink'], [provider_link])
    biobank_id = response['biobankId']
    self.assertTrue(biobank_id.startswith('B'))

    participant_id = response['participantId']

    # Fetch that participant.
    response = self.client.request_json('Participant/{}'.format(participant_id))
    last_etag = self.client.last_etag

    # Add a provider link to the participant.
    provider_link_2 = {
      "primary": True,
      "organization": {
        "display": None,
        "reference": "mayo",
      },
      "site": [{
        "display": None,
        "reference": "mayo-clinic",
      }],
      "identifier": [{
        "system": "http://any-columbia-mrn-system",
        "value": "MRN456"
      }]
    }
    response['providerLink'] = [ provider_link, provider_link_2 ]
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

    self.assertEqual(response['biobankId'], biobank_id)
    test_util._compare_json(self, response['providerLink'], [provider_link, provider_link_2])

if __name__ == '__main__':
  unittest.main()
