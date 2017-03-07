import main

from clock import FakeClock
from test.unit_test.unit_test_util import FlaskTestBase
from test_data import load_measurement_json, load_measurement_json_amendment

class PhysicalMeasurementsApiTest(FlaskTestBase):

  def setUp(self):
    super(PhysicalMeasurementsApiTest, self).setUp()
    self.participant_id = self.create_participant()
    self.participant_id_2 = self.create_participant()

  def insert_measurements(self):
    measurements_1 = load_measurement_json(self.participant_id)
    measurements_2 = load_measurement_json(self.participant_id_2)
    path_1 = 'Participant/%s/PhysicalMeasurements' % self.participant_id
    path_2 = 'Participant/%s/PhysicalMeasurements' % self.participant_id_2
    self.send_post(path_1, measurements_1)
    self.send_post(path_2, measurements_2)

  def test_insert(self):
    self.insert_measurements()

    response = self.send_get('Participant/%s/PhysicalMeasurements' % self.participant_id)
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))

    response = self.send_get('Participant/%s/PhysicalMeasurements' % self.participant_id_2)
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertFalse(response.get('link'))
    self.assertTrue(response.get('entry'))
    self.assertEquals(1, len(response['entry']))

  def test_insert_and_amend(self):
    measurements_1 = load_measurement_json(self.participant_id)
    path_1 = 'Participant/%s/PhysicalMeasurements' % self.participant_id
    response = self.send_post(path_1, measurements_1)
    measurements_2 = load_measurement_json_amendment(self.participant_id, response['id'])
    self.send_post(path_1, measurements_2)

    response = self.send_get('Participant/%s/PhysicalMeasurements' % self.participant_id)
    self.assertEquals(2, len(response['entry']))
    self.assertEquals("amended", response['entry'][0]['resource']['entry'][0]['resource']['status'])


  def test_physical_measurements_sync(self):
    sync_response = self.send_get('PhysicalMeasurements/_history')
    self.assertEquals('Bundle', sync_response['resourceType'])
    self.assertEquals('history', sync_response['type'])
    link = sync_response.get('link')
    self.assertIsNone(link)
    self.assertTrue(len(sync_response['entry']) == 0)

    self.insert_measurements()

    sync_response = self.send_get('PhysicalMeasurements/_history?_count=1')
    self.assertTrue(sync_response.get('entry'))
    link = sync_response.get('link')
    self.assertTrue(link)
    self.assertEquals("next", link[0]['relation'])
    self.assertEquals(1, len(sync_response['entry']))
    prefix_index = link[0]['url'].index(main.PREFIX)
    relative_url = link[0]['url'][prefix_index + len(main.PREFIX):]

    sync_response_2 = self.send_get(relative_url)
    self.assertEquals(1, len(sync_response_2['entry']))
    self.assertNotEquals(sync_response['entry'][0], sync_response_2['entry'][0])
