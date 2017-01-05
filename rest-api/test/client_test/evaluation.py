"""Test for the evaluation endpoint."""

import json
import unittest

import test_util

class TestEvaluation(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')
    self.participant_id = test_util.create_participant(self.client)

  def test_insert_eval(self):
    evaluation_files = [
        'test-data/evaluation-as-fhir.json',
    ]

    for json_file in evaluation_files:
      with open(json_file) as f:
        evaluation = json.load(f)
        path = 'Participant/{}/PhysicalEvaluation'.format(self.participant_id)
        test_util.round_trip(self, self.client, path, evaluation)
    response = self.client.request_json('Participant/{}/PhysicalEvaluation'.format(self.participant_id))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])    
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))    

if __name__ == '__main__':
  unittest.main()
