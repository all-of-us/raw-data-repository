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
        "reference": "Organization/PITT",
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
    
    # Fetch the participant summary
    summary_response = self.client.request_json('Participant/{}/Summary'.format(participant_id))
    self.assertEquals(biobank_id, summary_response['biobankId'])
    self.assertEquals('PITT', summary_response['hpoId'])
    self.assertEquals(participant_id, summary_response['participantId'])
    
    # Fetch all participant summaries; should be just one.
    response = self.client.request_json('ParticipantSummary?biobankId={}'.format(biobank_id))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    entry = response['entry'][0]
    self.assertEquals(summary_response, entry['resource'])
    self.assertEquals('http://localhost:8080/rdr/v1/Participant/{}/Summary'.format(participant_id), entry['fullUrl'])

if __name__ == '__main__':
  unittest.main()
