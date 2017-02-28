import httplib
import json

from test.unit_test.unit_test_util import FlaskTestBase

class ParticipantApiTest(FlaskTestBase):

  def setUp(self):
    super(ParticipantApiTest, self).setUp()
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
    self.participant = {
        'providerLink': [provider_link]
    }

  def test_insert(self):
    response = self.send_post('Participant', self.participant)
    participant_id = response['participantId']
    get_response = self.send_get('Participant/%s' % participant_id)
    self.assertEquals(response, get_response)
    biobank_id = response['biobankId']
    self.assertTrue(biobank_id.startswith('B'))
    del response['participantId']
    del response['biobankId']
    del response['signUpTime']
    del response['lastModified']

    self.assertJsonResponseMatches(self.participant, response)

  def test_update(self):
    response = self.send_post('Participant', self.participant)

    # Change the provider link for the participant
    provider_link = {
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
    participant_id = response['participantId']
    response['providerLink'] = [ provider_link ]
    path = 'Participant/%s' % participant_id
    self.send_put(path, response, expected_status=httplib.BAD_REQUEST)
