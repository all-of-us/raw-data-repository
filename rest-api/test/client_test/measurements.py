"""Test for the physical measurements endpoint."""

import json
import unittest
import datetime

import test_util


class TestPhysicalMeasurements(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')
    self.participant_id = test_util.create_participant(self.client)
    self.when = datetime.datetime.now().isoformat()

  def test_insert_physical_measurements(self):
    measurements_files = [
        'test-data/measurements-as-fhir.json',
    ]

    for json_file in measurements_files:
      with open(json_file) as f:
        measurements = f.read() \
          .replace('$authored_time', self.when) \
          .replace('$participant_id', self.participant_id)

        measurements = json.loads(measurements)
        path = 'Participant/{}/PhysicalMeasurements'.format(self.participant_id)
        test_util.round_trip(self, self.client, path, measurements)
    response = self.client.request_json('Participant/{}/PhysicalMeasurements'
                                        .format(self.participant_id))
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))
    
  def test_physical_measurements_sync(self):
    sync_response = self.client.request_json('PhysicalMeasurementsSync')
    self.assertEquals('Bundle', sync_response['resourceType'])
    self.assertEquals('history', sync_response['type'])
    link = sync_response.get('link')
    self.assertTrue(link)
    self.assertEquals("next", link[0]['relation'])        
    self.assertTrue(sync_response.get('entry'))    
    self.assertTrue(len(sync_response['entry']) > 1)
    
    sync_response = self.client.request_json('PhysicalMeasurementsSync?_count=1')
    self.assertTrue(sync_response.get('entry'))    
    link = sync_response.get('link')
    self.assertTrue(link)
    print 'link = {}'.format(link[0]['url'])
    self.assertTrue('moreAvailable=true' in link[0]['url'])
    self.assertEquals(1, len(sync_response['entry']))
    
    sync_response_2 = self.client.request_json(link[0]['url'])
    self.assertEquals(1, len(sync_response_2['entry']))
    self.assertNotEquals(sync_response['entry'][0], sync_response_2['entry'][0])
        
if __name__ == '__main__':
  unittest.main()
