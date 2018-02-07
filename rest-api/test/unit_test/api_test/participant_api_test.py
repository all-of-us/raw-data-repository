import httplib

from test.unit_test.unit_test_util import FlaskTestBase
from participant_enums import WithdrawalStatus, SuspensionStatus

class ParticipantApiTest(FlaskTestBase):

  def setUp(self):
    super(ParticipantApiTest, self).setUp()
    provider_link = {
      "primary": False,
      "organization": {
        "reference": "columbia"
      }
    }
    self.participant = {
        'providerLink': [provider_link]
    }
    self.provider_link_2 = {
      "primary": True,
      "organization": {
        "reference": "Organization/PITT",
      }
    }


  def test_insert(self):
    response = self.send_post('Participant', self.participant)
    participant_id = response['participantId']
    get_response = self.send_get('Participant/%s' % participant_id)
    self.assertEquals(response, get_response)
    biobank_id = response['biobankId']
    self.assertTrue(biobank_id.startswith('Z'))
    self.assertEquals(str(WithdrawalStatus.NOT_WITHDRAWN), response['withdrawalStatus'])
    self.assertEquals(str(SuspensionStatus.NOT_SUSPENDED), response['suspensionStatus'])
    for auto_generated in (
        'participantId',
        'site',
        'organization',
        'awardee',
        'hpoId',
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
    response['withdrawalStatus'] = 'NO_USE'
    response['suspensionStatus'] = 'NO_CONTACT'
    response['site'] = 'UNSET'
    response['organization'] = 'UNSET'
    response['awardee'] = 'PITT'
    response['hpoId'] = 'PITT'
    path = 'Participant/%s' % participant_id
    update_response = self.send_put(path, response, headers={ 'If-Match': 'W/"1"' })
    response['meta']['versionId'] = 'W/"2"'
    response['withdrawalTime'] = update_response['lastModified']
    response['suspensionTime'] = update_response['lastModified']
    self.assertJsonResponseMatches(response, update_response)

  def test_change_pairing_awardee_and_site(self):
    participant = self.send_post('Participant', self.participant)
    participant['providerLink'] = [ self.provider_link_2]
    participant_id = participant['participantId']
    participant['awardee'] = 'PITT'
    participant['site'] = 'hpo-site-monroeville'
    path = 'Participant/%s' % participant_id
    update_awardee = self.send_put(path, participant, headers={'If-Match': 'W/"1"'})
    self.assertEquals(participant['site'], update_awardee['site'])
    self.assertEquals(participant['awardee'], update_awardee['awardee'])

  def test_change_pairing_for_org_then_site(self):
    participant = self.send_post('Participant', self.participant)
    participant['providerLink'] = [ self.provider_link_2]
    participant_id = participant['participantId']
    path = 'Participant/%s' % participant_id

    update_1 = self.send_put(path, participant, headers={'If-Match': 'W/"1"'})
    participant['site'] = 'hpo-site-bannerphoenix'
    update_2 = self.send_put(path, participant, headers={'If-Match': 'W/"2"'})
    self.assertEqual(update_1['site'], 'UNSET')
    self.assertEqual(update_1['organization'], 'UNSET')
    self.assertEqual(update_2['site'], 'hpo-site-bannerphoenix')
    self.assertEqual(update_2['organization'], 'PITT_BANNER_HEALTH')
    participant['organization'] = 'AZ_TUCSON_BANNER_HEALTH'
    update_3 = self.send_put(path, participant, headers={'If-Match': 'W/"3"'})
    self.assertEqual(update_2['hpoId'], update_3['hpoId'])
    self.assertEqual(update_2['organization'], update_3['organization'])
    self.assertEqual(update_3['site'], 'hpo-site-bannerphoenix')
    participant['site'] = 'hpo-site-clinic-phoenix'
    update_4 = self.send_put(path, participant, headers={'If-Match': 'W/"4"'})
    self.assertEqual(update_4['site'], 'hpo-site-clinic-phoenix')
    self.assertEqual(update_4['organization'], 'AZ_TUCSON_BANNER_HEALTH')
    self.assertEqual(update_4['awardee'], 'AZ_TUCSON')
