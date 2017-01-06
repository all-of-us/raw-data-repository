"""Simple end to end test to exercise the participant and evaluation APIs.
"""

import datetime
import json
import unittest
import time

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
    
  def testCreateAndListSummaries(self):
    consent_questionnaire = json.load(open('test-data/consent_questionnaire.json'))
    consent_questionnaire_id = self.client.request_json('Questionnaire', 'POST', consent_questionnaire)['id']
    questionnaire_response_template = open('test-data/consent_questionnaire_response.json').read()
    current_time = time.time()
    provider_link = {
      "primary": True,
      "organization": {
        "display": None,
        "reference": "Organization/COLUMBIA",
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
    participant_request = {
        'providerLink': [provider_link]
    }
    first_name = 'FN{}'.format(current_time)
    base_replacements = { 'consent_questionnaire_id': consent_questionnaire_id,
                          'middle_name': 'Quentin',
                          'first_name': first_name,    
                          'state': 'TX',
                          'consent_questionnaire_authored': '2016-12-30 11:23',
                          'gender_identity': 'male' } 
    # Create 9 participants
    start_date = datetime.date.today();
    dates_of_birth = []
    for i in range(1, 10):  
      participant_response = self.client.request_json('Participant', 'POST', participant_request)
      participant_id = participant_response['participantId']   
      # Subtract ten years on each step   
      date_of_birth = start_date - datetime.timedelta(days=3650*i)
      dates_of_birth.append(date_of_birth)
      replacements = dict(base_replacements.items() + { 'participant_id': participant_id,
                                           'last_name': 'LN{}'.format(i),                          
                                           'date_of_birth': date_of_birth.isoformat()}.items())   
      questionnaire_response = questionnaire_response_template
      for k, v in replacements.iteritems():
        questionnaire_response = questionnaire_response.replace('$%s'%k, v)
      self.client.request_json('Participant/{}/QuestionnaireResponse'.format(participant_id), 
                               'POST', json.loads(questionnaire_response))
    # Wait for up to 60 seconds for the indexes to update.        
    for i in range(60):
      response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&firstName={}'.format(first_name))
      if response.get('entry') and len(response['entry']) == 9:
        break
      time.sleep(1)
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(9, len(response['entry']))
    for i in range(9):
      self.assertEquals('LN{}'.format(i + 1), response['entry'][i]['resource']['lastName'])
          
    # Returns just one participant summary (exact match on last name)
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&firstName={}&lastName=LN7'.format(first_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    self.assertEquals('LN7', response['entry'][0]['resource']['lastName'])
    
    # Returns just one participant summary (exact match on date of birth)
    date_of_birth = dates_of_birth[3].isoformat()
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&firstName={}&dateOfBirth={}'.format(first_name, date_of_birth))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    self.assertEquals(date_of_birth, response['entry'][0]['resource']['dateOfBirth'])
    
    # Returns the one participant in the age range 18-25
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&firstName={}&ageRange=18-25'.format(first_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    self.assertEquals('18-25', response['entry'][0]['resource']['ageRange'])
    
    # Returns the one participant matching everything
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&firstName={}'.format(first_name) + \
                                        '&middleName=Quentin&lastName=LN2&genderIdentity=MALE' + \
                                        '&dateOfBirth={}&ageRange=18-25&ethnicity=UNSET'.format(dates_of_birth[1]) + \
                                        '&membershipTier=UNSET&consentForStudyEnrollment=SUBMITTED')
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    self.assertEquals('COLUMBIA', response['entry'][0]['resource']['hpoId'])
    self.assertEquals(first_name, response['entry'][0]['resource']['firstName'])
    self.assertEquals('Quentin', response['entry'][0]['resource']['middleName'])
    self.assertEquals('LN2', response['entry'][0]['resource']['lastName'])
    self.assertEquals('MALE', response['entry'][0]['resource']['genderIdentity'])
    self.assertEquals(dates_of_birth[1].isoformat(), response['entry'][0]['resource']['dateOfBirth'])
    self.assertEquals('18-25', response['entry'][0]['resource']['ageRange'])
    self.assertEquals('UNSET', response['entry'][0]['resource']['ethnicity'])
    self.assertEquals('UNSET', response['entry'][0]['resource']['membershipTier'])
    self.assertEquals('SUBMITTED', response['entry'][0]['resource']['consentForStudyEnrollment'])
    
    # Returns no participant summary (no match on date of birth)
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&firstName={}&dateOfBirth=2525-01-15'.format(first_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertFalse(response.get('entry'))
        
    # Returns 5 participant summaries, ordered by last name ascending, with pagination token
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&firstName={}&_count=5'.format(first_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])    
    self.assertTrue(response.get('link'))
    self.assertEquals('next', response['link'][0]['relation'])
    next_url = response['link'][0]['url']
    self.assertTrue(response.get('entry'))
    self.assertEquals(5, len(response['entry']))
    for i in range(5):
      self.assertEquals('LN{}'.format(i + 1), response['entry'][i]['resource']['lastName'])

    # Returns remaining 4 participant summaries, ordered by last name ascending
    response = self.client.request_json(next_url, absolute_path=True)
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])    
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(4, len(response['entry']))
    for i in range(4):
      self.assertEquals('LN{}'.format(i + 6), response['entry'][i]['resource']['lastName'])    
    
    # Query on last name and date of birth without HPO ID succeeds
    response = self.client.request_json('ParticipantSummary?firstName={}&lastName=LN9&dateOfBirth={}'.format(first_name, dates_of_birth[8]))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])    
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    self.assertEquals(dates_of_birth[8].isoformat(), response['entry'][0]['resource']['dateOfBirth'])
    self.assertEquals('LN9', response['entry'][0]['resource']['lastName'])
    
    # Query on last name without date of birth or HPO ID fails
    with self.assertRaises(HttpException):
      self.client.request_json('ParticipantSummary?lastName=LN7')
      
    # Query on date of birth without last name or HPO ID fails
    with self.assertRaises(HttpException):
      self.client.request_json('ParticipantSummary?dateOfBirth=2017-01-09')
      
    # Query with no HPO ID, last name, or date of birth fails
    with self.assertRaises(HttpException):
      self.client.request_json('ParticipantSummary?firstName={}'.format(first_name))
    
if __name__ == '__main__':
  unittest.main()
