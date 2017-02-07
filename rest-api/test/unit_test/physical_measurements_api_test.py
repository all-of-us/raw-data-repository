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
from test.test_data import data_path, load_measurement_json

_AUTH_USER = 'authorized@gservices.act'
_CONFIG_USER_INFO = {
  _AUTH_USER: {
    'roles': api_util.ALL_ROLES,
  },
}
_PARTICIPANT = 'P123'


class PhysicalMeasurementsAPITest(NdbTestBase):
  def setUp(self):
    super(PhysicalMeasurementsAPITest, self).setUp()
    # http://flask.pocoo.org/docs/0.12/testing/
    main.app.config['TESTING'] = True
    self.app = main.app.test_client()
    config.override_setting(config.USER_INFO, _CONFIG_USER_INFO)

  def tearDown(self):
    super(PhysicalMeasurementsAPITest, self).tearDown()
    config.remove_override(config.USER_INFO)

  @mock.patch('api_util.get_oauth_id')
  def test_original_measurement(self, mock_get_oauth_id):
    # Sanity check: Verify that there is no PhysicalMeasurement yet.
    existing = measurements.DAO().list(_PARTICIPANT)
    self.assertItemsEqual(existing['items'], [])
    # Simulate a POST to create a novel PhysicalMeasurement.
    post_data = load_measurement_json(_PARTICIPANT)
    mock_get_oauth_id.return_value = _AUTH_USER
    response = self.app.post(
        main.PREFIX + 'Participant/P123/PhysicalMeasurements',
        data=json.dumps(post_data),
        content_type='application/json')

    # Verify that the request succeeded and 1 bundle was created.
    self.assertEquals(response.status_code, httplib.OK)
    response_data = json.loads(response.data)
    self.assertIn('id', response_data)
    self.assertIn('entry', response_data)

    stored_items = measurements.DAO().list(_PARTICIPANT)['items']
    self.assertEquals(len(stored_items), 1)
    self.assertEquals(response_data['id'], stored_items[0]['id'])
