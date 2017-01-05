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
    
    # Fetch all participant summaries; should be just one.
    response = self.client.request_json('ParticipantSummary?hpoId=PITT&biobankId={}'.format(biobank_id))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    entry = response['entry'][0]
    self.assertEquals(summary_response, entry['resource'])
    self.assertEquals('http://localhost:8080/rdr/v1/Participant/{}/Summary'.format(participant_id), entry['fullUrl'])

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
    last_name = 'LN{}'.format(current_time)
    base_replacements = { 'consent_questionnaire_id': consent_questionnaire_id,
                          'middle_name': 'Q.',
                          'last_name': last_name,    
                          'state': 'TX',
                          'consent_questionnaire_authored': '2016-12-30 11:23',
                          'gender_identity': 'male' } 
    # Create 9 participants
    for i in range(1, 10):  
      participant_response = self.client.request_json('Participant', 'POST', participant_request)
      participant_id = participant_response['participantId']      
      replacements = dict(base_replacements.items() + { 'participant_id': participant_id,
                                           'first_name': 'Bob{}'.format(i),                          
                                           'date_of_birth': '2017-01-0{}'.format(i)}.items())   
      questionnaire_response = questionnaire_response_template
      for k, v in replacements.iteritems():
        questionnaire_response = questionnaire_response.replace('$%s'%k, v)
      self.client.request_json('Participant/{}/QuestionnaireResponse'.format(participant_id), 
                               'POST', json.loads(questionnaire_response))
    # Wait a few seconds for indexes to update.
    time.sleep(5)
    
    # Returns all 9 participant summaries
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(9, len(response['entry']))
    
    # Returns all 9 participant summaries, ordered by first name ascending
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&_sort=firstName'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(9, len(response['entry']))
    for i in range(9):
      self.assertEquals('Bob{}'.format(i + 1), response['entry'][i]['resource']['firstName'])
      
    # Returns all 9 participant summaries, ordered by first name ascending (same as above)
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&_sort:asc=firstName'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(9, len(response['entry']))
    for i in range(9):
      self.assertEquals('Bob{}'.format(i + 1), response['entry'][i]['resource']['firstName'])  
    
    # Returns all 9 participant summaries, ordered by first name descending
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&_sort:desc=firstName'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(9, len(response['entry']))
    for i in range(9):
      self.assertEquals('Bob{}'.format(9 - i), response['entry'][i]['resource']['firstName'])

    # Returns just one participant summary (exact match on first name)
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&firstName=BOb7'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    self.assertEquals('Bob7', response['entry'][0]['resource']['firstName'])
    
    # Returns just one participant summary (exact match on date of birth)
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&dateOfBirth=2017-01-05'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    self.assertEquals('2017-01-05', response['entry'][0]['resource']['dateOfBirth'])
    
    # Returns no participant summary (no match on date of birth)
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&dateOfBirth=2017-01-15'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertFalse(response.get('entry'))
    
    # Returns 2 participant summaries (> 2017-01-07), ordered by dateOfBirth ascending
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&dateOfBirth=gt2017-01-07&_sort=dateOfBirth'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(2, len(response['entry']))
    for i in range(2):
      self.assertEquals('2017-01-0{}'.format(8 + i), response['entry'][i]['resource']['dateOfBirth'])

    # Returns 3 participant summaries (>= 2017-01-07), ordered by dateOfBirth ascending
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&dateOfBirth=ge2017-01-07&_sort=dateOfBirth'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(3, len(response['entry']))
    for i in range(3):
      self.assertEquals('2017-01-0{}'.format(7 + i), response['entry'][i]['resource']['dateOfBirth'])

    # Returns 2 participant summaries (< 2017-01-03), ordered by dateOfBirth ascending
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&dateOfBirth=lt2017-01-03&_sort=dateOfBirth'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(2, len(response['entry']))
    for i in range(2):
      self.assertEquals('2017-01-0{}'.format(1 + i), response['entry'][i]['resource']['dateOfBirth'])

    # Returns 3 participant summaries (<= 2017-01-03), ordered by dateOfBirth ascending
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&dateOfBirth=le2017-01-03&_sort=dateOfBirth'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(3, len(response['entry']))
    for i in range(3):
      self.assertEquals('2017-01-0{}'.format(1 + i), response['entry'][i]['resource']['dateOfBirth'])

    # Returns 5 participant summaries, ordered by first name ascending, with pagination token
    response = self.client.request_json('ParticipantSummary?hpoId=COLUMBIA&lastName={}&_sort=firstName&_count=5'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])    
    self.assertTrue(response.get('link'))
    self.assertEquals('next', response['link'][0]['relation'])
    next_url = response['link'][0]['url']
    self.assertTrue(response.get('entry'))
    self.assertEquals(5, len(response['entry']))
    for i in range(5):
      self.assertEquals('Bob{}'.format(i + 1), response['entry'][i]['resource']['firstName'])

    # Returns remaining 4 participant summaries, ordered by first name ascending
    response = self.client.request_json(next_url, absolute_path=True)
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])    
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(4, len(response['entry']))
    for i in range(4):
      self.assertEquals('Bob{}'.format(i + 6), response['entry'][i]['resource']['firstName'])    
    
    # Query on last name and date of birth without HPO ID succeeds
    response = self.client.request_json('ParticipantSummary?lastName={}&dateOfBirth=2017-01-09'.format(last_name))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])    
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    self.assertEquals('2017-01-09', response['entry'][0]['resource']['dateOfBirth'])

    # Query on last name without date of birth or HPO ID fails
    try:
      response = self.client.request_json('ParticipantSummary?lastName={}'.format(last_name))
      self.fail("Should have failed")
    except HttpException:
      pass
      
    # Query on date of birth without last name or HPO ID fails
    try:
      response = self.client.request_json('ParticipantSummary?dateOfBirth=2017-01-09')
      self.fail("Should have failed")
    except HttpException:
      pass 

    # Query with no HPO ID, last name, or date of birth fails
    try:
      response = self.client.request_json('ParticipantSummary?firstName=Bob1')
      self.fail("Should have failed")
    except HttpException:
      pass 
    
if __name__ == '__main__':
  unittest.main()
