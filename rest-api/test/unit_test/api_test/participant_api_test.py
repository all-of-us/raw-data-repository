import httplib

from test.unit_test.unit_test_util import FlaskTestBase
from participant_enums import WithdrawalStatus, SuspensionStatus

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
    self.provider_link_2 = {
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


  def test_insert(self):
    response = self.send_post('Participant', self.participant)
    participant_id = response['participantId']
    get_response = self.send_get('Participant/%s' % participant_id)
    self.assertEquals(response, get_response)
    biobank_id = response['biobankId']
    self.assertTrue(biobank_id.startswith('B'))
    self.assertEquals(WithdrawalStatus.NOT_WITHDRAWN.number, response['withdrawalStatus'])
    self.assertEquals(SuspensionStatus.NOT_SUSPENDED.number, response['suspensionStatus'])
    for auto_generated in (
        'participantId',
        'biobankId',
        'signUpTime',
        'lastModified',
        'withdrawalStatus',
        'suspensionStatus'):
      del response[auto_generated]

    self.assertJsonResponseMatches(self.participant, response)

  def test_update_no_ifmatch_specified(self):
    response = self.send_post('Participant', self.participant)

    # Change the provider link for the participant
    participant_id = response['participantId']
    response['providerLink'] = [ self.provider_link_2 ]
    path = 'Participant/%s' % participant_id
    self.send_put(path, response, expected_status=httplib.BAD_REQUEST)

  def test_update_bad_ifmatch_specified(self):
    response = self.send_post('Participant', self.participant)

    # Change the provider link for the participant
    participant_id = response['participantId']
    response['providerLink'] = [ self.provider_link_2 ]
    path = 'Participant/%s' % participant_id
    self.send_put(path, response, headers={ 'If-Match': 'Blah' },
                  expected_status=httplib.BAD_REQUEST)

  def test_update_wrong_ifmatch_specified(self):
    response = self.send_post('Participant', self.participant)

    # Change the provider link for the participant
    participant_id = response['participantId']
    response['providerLink'] = [ self.provider_link_2 ]
    path = 'Participant/%s' % participant_id
    self.send_put(path, response, headers={ 'If-Match': 'W/"123"' },
                  expected_status=httplib.PRECONDITION_FAILED)

  def test_update_right_ifmatch_specified(self):
    response = self.send_post('Participant', self.participant)
    self.assertEquals('W/"1"', response['meta']['versionId'])
    # Change the provider link for the participant
    participant_id = response['participantId']
    response['providerLink'] = [ self.provider_link_2 ]
    path = 'Participant/%s' % participant_id
    update_response = self.send_put(path, response, headers={ 'If-Match': 'W/"1"' })
    response['meta']['versionId'] = 'W/"2"'
    self.assertJsonResponseMatches(response, update_response)

