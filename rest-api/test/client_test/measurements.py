import json
import unittest
import datetime

import test_util

from test.test_data import load_measurement_json


class TestPhysicalMeasurements(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')
    self.participant_id = test_util.create_participant(self.client)
    self.participant_id_2 = test_util.create_participant(self.client)
    self.when = datetime.datetime.now().isoformat()

  def test_insert_physical_measurements(self):
    measurements_1 = load_measurement_json(self.participant_id, now=self.when)
    measurements_2 = load_measurement_json(self.participant_id_2, now=self.when)
    path_1 = 'Participant/{}/PhysicalMeasurements'.format(self.participant_id)
    path_2 = 'Participant/{}/PhysicalMeasurements'.format(self.participant_id_2)
    test_util.round_trip(self, self.client, path_1, measurements_1)
    test_util.round_trip(self, self.client, path_2, measurements_2)

    response = self.client.request_json('Participant/{}/PhysicalMeasurements'
                                        .format(self.participant_id))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))

    response = self.client.request_json('Participant/{}/PhysicalMeasurements'
                                        .format(self.participant_id_2))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    
  def test_physical_measurements_sync(self):
    sync_response = self.client.request_json('PhysicalMeasurements/_history')
    self.assertEquals('Bundle', sync_response['resourceType'])
    self.assertEquals('history', sync_response['type'])
    link = sync_response.get('link')   
    self.assertTrue(link)
    self.assertTrue(sync_response.get('entry'))    
    self.assertTrue(len(sync_response['entry']) > 1)
    
    sync_response = self.client.request_json('PhysicalMeasurements/_history?_count=1')
    self.assertTrue(sync_response.get('entry'))    
    link = sync_response.get('link')
    self.assertTrue(link)
    self.assertEquals("next", link[0]['relation'])    
    self.assertEquals(1, len(sync_response['entry']))
    
    sync_response_2 = self.client.request_json(link[0]['url'], absolute_path=True)
    self.assertEquals(1, len(sync_response_2['entry']))
    self.assertNotEquals(sync_response['entry'][0], sync_response_2['entry'][0])
        
if __name__ == '__main__':
  unittest.main()
