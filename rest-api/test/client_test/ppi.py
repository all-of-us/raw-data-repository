"""Simple example for calling the ppi/fhir endpoints.

Also serves as end to end test to exercise each of these REST APIs.
"""

import datetime
import json
import unittest

import test_util
from client.client import HttpException
from dateutil.relativedelta import relativedelta

def _questionnaire_response_url(participant_id):
  return 'Participant/{}/QuestionnaireResponse'.format(participant_id)

class TestPPI(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')

  def test_questionnaires(self):
    questionnaire_files = [
        'test-data/questionnaire1.json',
        # Example from vibrent.
        'test-data/questionnaire2.json',
        'test-data/questionnaire_demographics.json',
    ]

    for json_file in questionnaire_files:
      with open(json_file) as f:
        questionnaire = json.load(f)
        test_util.round_trip(self, self.client, 'Questionnaire', questionnaire)

  def test_valid_and_invalid_questionnaire_responses(self):
    questionnaire_response_files = [
        # Stripped down version of the official FHIR example
        #'test-data/questionnaire_response1.json',
        # Example response from vibrent.  Doesn't pass validation.
        #'test-data/questionnaire_response2.json',
        'test-data/questionnaire_response3.json',
    ]
    participant_id = test_util.create_participant(self.client)
    questionnaire_id = test_util.create_questionnaire(
        self.client, 'test-data/questionnaire1.json')
    for json_file in questionnaire_response_files:
      with open(json_file) as f:
        resource = json.load(f)
        # Sending response with the dummy participant id in the file is an error
        with self.assertRaises(HttpException) as context:
          test_util.round_trip(self, self.client,
                               _questionnaire_response_url('{participant_id}'),
                               resource)
        # Fixing participant id but not the questionnaire id is also an error
        good_url = _questionnaire_response_url(participant_id)
        resource['subject']['reference'] = \
            resource['subject']['reference'].format(
                participant_id=participant_id)
        with self.assertRaises(HttpException) as context:
          test_util.round_trip(self, self.client, good_url, resource)
        # Also fixing participant id succeeds
        resource['questionnaire']['reference'] = \
            resource['questionnaire']['reference'].format(
                questionnaire_id=questionnaire_id)
        test_util.round_trip(self, self.client, good_url, resource)

  def test_demographic_questionnaire_responses(self):
    questionnaire_response_files = [
        'test-data/questionnaire_response_demographics.json',
    ]
    participant_id = test_util.create_participant(self.client)
    questionnaire_id = test_util.create_questionnaire(
        self.client, 'test-data/questionnaire_demographics.json')
    for json_file in questionnaire_response_files:
      with open(json_file) as f:
        resource = json.load(f)
        # Sending response with the dummy participant id in the file is an error
        good_url = _questionnaire_response_url(participant_id)
        resource['subject']['reference'] = \
            resource['subject']['reference'].format(
                participant_id=participant_id)
        resource['questionnaire']['reference'] = \
            resource['questionnaire']['reference'].format(
                questionnaire_id=questionnaire_id)
        test_util.round_trip(self, self.client, good_url, resource)
    participant = self.client.request_json('Participant/{}'.format(participant_id))
    response = self.client.request_json('Participant/{}/Summary'.format(participant_id))
    self.assertEqual(response['genderIdentity'], 'MALE_TO_FEMALE_TRANSGENDER')

    response = self.client.request_json('Participant/{}/Summary'.format(participant_id))
    # TODO: add more stuff here
    expected = { 'ageRange': 'UNSET',
                 'genderIdentity': 'MALE_TO_FEMALE_TRANSGENDER',
                 'ethnicity': 'UNSET',
                 'race': 'UNSET',
                 'hpoId': 'UNSET',
                 'firstName': None,
                 'lastName': None,
                 'middleName': None,
                 'membershipTier': 'UNSET',
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
                 'questionnaireOnSociodemographics': 'UNSET'
              }
    test_util._compare_json(self, expected, response)


if __name__ == '__main__':
  unittest.main()
