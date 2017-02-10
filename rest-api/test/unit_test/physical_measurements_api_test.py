import api_util
import config
import httplib
import json
import main
import measurements
import mock
import physical_measurements_api
import pprint

from test.unit_test.unit_test_util import NdbTestBase
from test import test_data

_AUTH_USER = 'authorized@gservices.act'
_CONFIG_USER_INFO = {
  _AUTH_USER: {
    'roles': api_util.ALL_ROLES,
  },
}
_PARTICIPANT = 'P123'
_URL = 'Participant/%s/PhysicalMeasurements' % _PARTICIPANT


class PhysicalMeasurementsAPITest(NdbTestBase):
  def setUp(self):
    super(PhysicalMeasurementsAPITest, self).setUp()
    # http://flask.pocoo.org/docs/0.12/testing/
    main.app.config['TESTING'] = True
    self.app = main.app.test_client()
    config.override_setting(config.USER_INFO, _CONFIG_USER_INFO)

  def post_json(self, local_path, post_data, expected_status=httplib.OK):
    response = self.app.post(
        main.PREFIX + local_path,
        data=json.dumps(post_data),
        content_type='application/json')
    self.assertEquals(response.status_code, expected_status, response.data)
    return json.loads(response.data)

  def tearDown(self):
    super(PhysicalMeasurementsAPITest, self).tearDown()
    config.remove_override(config.USER_INFO)

  @mock.patch('api_util.get_oauth_id')
  def test_original_measurement(self, mock_get_oauth_id):
    mock_get_oauth_id.return_value = _AUTH_USER
    # Sanity check: Verify that there is no PhysicalMeasurement yet.
    existing = measurements.DAO().list(_PARTICIPANT)
    self.assertItemsEqual(existing['items'], [])
    # Simulate a POST to create a novel PhysicalMeasurement.
    response_data = self.post_json(_URL, test_data.load_measurement_json(_PARTICIPANT))

    # Verify that the request succeeded and 1 bundle was created.
    self.assertIn('id', response_data)
    self.assertIn('entry', response_data)

    stored_items = measurements.DAO().list(_PARTICIPANT)['items']
    self.assertEquals(len(stored_items), 1)
    self.assertEquals(response_data['id'], stored_items[0]['id'])

  @mock.patch('api_util.get_oauth_id')
  def test_amended(self, mock_get_oauth_id):
    mock_get_oauth_id.return_value = _AUTH_USER

    # Set up: create a novel PhysicalMeasurement.
    response_data = self.post_json(_URL, test_data.load_measurement_json(_PARTICIPANT))
    created_id = response_data['id']

    # Create a new measurement that amends the previous one.
    response_data = self.post_json(
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

  @mock.patch('api_util.get_oauth_id')
  def test_amended_invalid_id_fails(self, mock_get_oauth_id):
    mock_get_oauth_id.return_value = _AUTH_USER

    amendmant_with_bad_id = test_data.load_measurement_json_amendment(
        _PARTICIPANT, 'bogus-measurement-id')
    response_data = self.post_json(_URL, amendmant_with_bad_id, expected_status=httplib.BAD_REQUEST)

  @mock.patch('api_util.get_oauth_id')
  def test_validation_does_not_block(self, mock_get_oauth_id):
    mock_get_oauth_id.return_value = _AUTH_USER

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

    response_data = self.post_json(_URL, measurement_data)
    self.assertIn('id', response_data, 'invalid request should still create a measurement')
