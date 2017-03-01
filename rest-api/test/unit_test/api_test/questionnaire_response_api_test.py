import httplib
import json

from test.unit_test.unit_test_util import FlaskTestBase
from test.test_data import data_path

def _questionnaire_response_url(participant_id):
  return 'Participant/%s/QuestionnaireResponse' % participant_id

class QuestionnaireResponseApiTest(FlaskTestBase):

  def test_insert(self):
    participant_id = self.create_participant()
    questionnaire_id = self.create_questionnaire('questionnaire1.json')
    with open(data_path('questionnaire_response3.json')) as f:
      resource = json.load(f)
    # Sending response with the dummy participant id in the file is an error
    self.send_post(_questionnaire_response_url('{participant_id}'), resource,
                   expected_status=httplib.BAD_REQUEST)
    # Fixing participant id but not the questionnaire id is also an error
    resource['subject']['reference'] = \
        resource['subject']['reference'].format(participant_id=participant_id)
    self.send_post(_questionnaire_response_url(participant_id), resource,
                   expected_status=httplib.BAD_REQUEST)
    # Also fixing questionnaire id succeeds
    resource['questionnaire']['reference'] = \
        resource['questionnaire']['reference'].format(questionnaire_id=questionnaire_id)
    response = self.send_post(_questionnaire_response_url(participant_id), resource)
    resource['id'] = response['id']
    # The resource gets rewritten to include the version
    resource['questionnaire']['reference'] = 'Questionnaire/%s/_history/1' % questionnaire_id
    self.assertJsonResponseMatches(resource, response)


  '''
  TODO(DA-224): uncomment this once participant summary API is working again
  def test_demographic_questionnaire_responses(self):
    participant_id = self.create_participant()
    questionnaire_id = self.create_questionnaire('questionnaire_demographics.json')
    with open(data_path('questionnaire_response_demographics.json')) as f:
      resource = json.load(f)
    resource['subject']['reference'] = \
      resource['subject']['reference'].format(participant_id=participant_id)
    resource['questionnaire']['reference'] = \
      resource['questionnaire']['reference'].format(questionnaire_id=questionnaire_id)
    response = self.send_post(_questionnaire_response_url(participant_id), resource)

    participant = self.send_get('Participant/%s' % participant_id)
    summary = self.send_get('Participant/%s/Summary' % participant_id)
    expected = {'ageRange': 'UNSET',
                'genderIdentity': 'MALE_TO_FEMALE_TRANSGENDER',
                'ethnicity': 'UNSET',
                'race': 'UNSET',
                'hpoId': 'UNSET',
                'firstName': None,
                'lastName': None,
                'middleName': None,
                'membershipTier': 'UNSET',
                'numBaselineSamplesArrived': 0,
                'numCompletedBaselinePPIModules': 1,
                'biobankId': participant['biobankId'],
                'participantId': participant_id,
                'physicalMeasurementsStatus': 'UNSET',
                'zipCode': None,
                'consentForElectronicHealthRecords': 'UNSET',
                'consentForStudyEnrollment': 'UNSET',
                'questionnaireOnFamilyHealth': 'UNSET',
                'questionnaireOnHealthcareAccess': 'UNSET',
                'questionnaireOnMedicalHistory' : 'UNSET',
                'questionnaireOnMedications': 'UNSET',
                'questionnaireOnOverallHealth': 'SUBMITTED',
                'questionnaireOnPersonalHabits': 'UNSET',
                'questionnaireOnSociodemographics': 'UNSET',
                'signUpTime': participant['signUpTime'],
              }
    self.assertJsonResponseMatches(expected, summary)
   '''