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

if __name__ == '__main__':
  unittest.main()
