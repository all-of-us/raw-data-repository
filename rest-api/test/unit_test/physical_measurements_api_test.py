import httplib

from test.unit_test.unit_test_util import FlaskTestBase
from test import test_data

import measurements

_PARTICIPANT = 'P123'
_URL = 'Participant/%s/PhysicalMeasurements' % _PARTICIPANT


class PhysicalMeasurementsAPITest(FlaskTestBase):
  def test_original_measurement(self):
    # Sanity check: Verify that there is no PhysicalMeasurement yet.
    existing = measurements.DAO().list(_PARTICIPANT)
    self.assertItemsEqual(existing['items'], [])
    # Simulate a POST to create a novel PhysicalMeasurement.
    response_data = self.send_post(_URL, test_data.load_measurement_json(_PARTICIPANT))

    # Verify that the request succeeded and 1 bundle was created.
    self.assertIn('id', response_data)
    self.assertIn('entry', response_data)

    stored_items = measurements.DAO().list(_PARTICIPANT)['items']
    self.assertEquals(len(stored_items), 1)
    self.assertEquals(response_data['id'], stored_items[0]['id'])

  def test_amended(self):
    # Set up: create a novel PhysicalMeasurement.
    response_data = self.send_post(_URL, test_data.load_measurement_json(_PARTICIPANT))
    created_id = response_data['id']

    # Create a new measurement that amends the previous one.
    response_data = self.send_post(
        _URL,
        test_data.load_measurement_json_amendment(_PARTICIPANT, created_id))
    amended_id = response_data['id']

    # After amendment, we should have two PhysicalMeasurements for the participant,
    # and the older one has Composition.status == 'amended'.
    stored_items = measurements.DAO().list(_PARTICIPANT)['items']
    self.assertEquals(len(stored_items), 2)
    for item in stored_items:
      if item['id'] == created_id:
        self.assertEquals(
            item['entry'][0]['resource']['status'],
            'amended',
            'previous measurement should be amended')
      elif item['id'] == amended_id:
        self.assertEquals(
            item['entry'][0]['resource']['status'],
            'final',
            'latest measurement should be final')
      else:
        self.fail('Unepxected PhysicalMeasurement %r.' % item['id'])

  def test_amended_invalid_id_fails(self):
    amendmant_with_bad_id = test_data.load_measurement_json_amendment(
        _PARTICIPANT, 'bogus-measurement-id')
    self.send_post(_URL, amendmant_with_bad_id, expected_status=httplib.BAD_REQUEST)

  def test_validation_does_not_block(self):
    # Remove one of the measurement sections from the valid FHIR document.
    measurement_data = test_data.load_measurement_json(_PARTICIPANT)
    rm_entry = 'urn:example:weight'
    found_to_rm = 0
    entries = measurement_data['entry']
    sections = entries[0]['resource']['section'][0]['entry']
    for i, sec in enumerate(sections):
      if sec['reference'] == rm_entry:
        sections.pop(i)
        found_to_rm += 1
        break
    for i, entry in enumerate(entries):
      if entry['fullUrl'] == rm_entry:
        entries.pop(i)
        found_to_rm += 1
        break
    self.assertEquals(found_to_rm, 2)

    response_data = self.send_post(_URL, measurement_data)
    self.assertIn('id', response_data, 'invalid request should still create a measurement')
