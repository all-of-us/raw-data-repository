import httplib

from test.unit_test.unit_test_util import FlaskTestBase

class ParticipantSummaryApiTest(FlaskTestBase):

  def setUp(self):
    super(ParticipantSummaryApiTest, self).setUp()
    self.provider_link = {
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

  def testQuery_noParticipants(self):
    self.send_get('Participant/P1/Summary', expected_status=httplib.NOT_FOUND)
    response = self.send_get('ParticipantSummary')
    self.assertBundle([], response)

  def testQuery_oneParticipant(self):
    participant = self.send_post('Participant', {"providerLink": [self.provider_link]})
    participant_id = participant['participantId']
    ps = self.send_get('Participant/%s/Summary' % participant_id)
    expected_ps = {'questionnaireOnHealthcareAccess': 'UNSET',
                   'membershipTier': 'UNSET',
                   'questionnaireOnOverallHealth': 'UNSET',
                   'signUpTime': participant['signUpTime'],
                   'ethnicity': 'UNSET',
                   'biobankId': participant['biobankId'],
                   'numBaselineSamplesArrived': 0,
                   'questionnaireOnSociodemographics': 'UNSET',
                   'questionnaireOnPersonalHabits': 'UNSET',
                   'questionnaireOnFamilyHealth': 'UNSET',
                   'questionnaireOnMedications': 'UNSET',
                   'physicalMeasurementsStatus': 'UNSET',
                   'genderIdentity': 'UNSET',
                   'consentForElectronicHealthRecords': 'UNSET',
                   'questionnaireOnMedicalHistory': u'UNSET',
                   'participantId': participant_id,
                   'hpoId': 'PITT',
                   'numCompletedBaselinePPIModules': 0,
                   'consentForStudyEnrollment': 'UNSET',
                   'race': 'UNSET',
                   'ageRange': 'UNSET'}
    self.assertJsonResponseMatches(expected_ps, ps)
    response = self.send_get('ParticipantSummary')
    self.assertBundle([_make_entry(ps)], response)

def _make_entry(ps):
  return { 'fullUrl': 'http://localhost/rdr/v1/Participant/%s/Summary' % ps['participantId'],
           'resource': ps }
