import datetime
import unittest

import app_util
import clock
import config

from mock import patch
from test.unit_test.unit_test_util import NdbTestBase
from werkzeug.exceptions import Forbidden, Unauthorized


@app_util.auth_required('foo')
def foo_role(x):
  return x + 1

@app_util.auth_required(['foo', 'bar'])
def foo_bar_role(x):
  return x + 1

@app_util.auth_required_cron
def cron_required(x):
  return x + 1


@app_util.nonprod
def not_in_prod():
  pass


class AppUtilNdbTest(NdbTestBase):

  def setUp(self):
    super(AppUtilNdbTest, self).setUp()

    self.user_info = {
      "example@example.com": {
         "roles": ["role1", "role2"],
         "whitelisted_ip_ranges": {
           "ip6": ["1234:5678::/32"],
           "ip4": ["123.210.0.1/16"]
         }
      }
    }

    # Note that there is a ttl cache on this config value, so it can't be changed during the test.
    config.insert_config(config.USER_INFO, self.user_info)

  def test_date_header(self):
    response = lambda: None  # Dummy object; functions can have arbitrary attrs set on them.
    setattr(response, 'headers', {})

    with clock.FakeClock(datetime.datetime(1994, 11, 6, 8, 49, 37)):
      app_util.add_headers(response)

    self.assertEquals(response.headers['Date'], 'Sun, 06 Nov 1994 08:49:37 GMT')

  def test_expiry_header(self):
    response = lambda: None  # dummy object
    setattr(response, 'headers', {})
    app_util.add_headers(response)

    self.assertEqual(response.headers['Expires'], 'Thu, 01 Jan 1970 00:00:00 GMT')

  def test_headers_present(self):
    response = lambda: None  # dummy object
    setattr(response, 'headers', {})
    app_util.add_headers(response)

    self.assertItemsEqual(response.headers.keys(), (
        'Date',
        'Expires',
        'Pragma',
        'Cache-control',
        'Content-Disposition',
        'Content-Type',
        'X-Content-Type-Options',
    ))

  def test_valid_ip(self):
    allowed_ips = app_util.get_whitelisted_ips(self.user_info["example@example.com"])
    app_util.enforce_ip_whitelisted('123.210.0.1', allowed_ips)
    app_util.enforce_ip_whitelisted('123.210.111.0', allowed_ips)

    app_util.enforce_ip_whitelisted('1234:5678::', allowed_ips)
    app_util.enforce_ip_whitelisted('1234:5678:9999::', allowed_ips)

  def test_invalid_ip(self):
    allowed_ips = app_util.get_whitelisted_ips(self.user_info["example@example.com"])
    with self.assertRaises(Forbidden):
      app_util.enforce_ip_whitelisted('100.100.0.1', allowed_ips)

    with self.assertRaises(Forbidden):
      app_util.enforce_ip_whitelisted('5555::', allowed_ips)

  @patch('app_util.request')
  @patch('app_util.app_identity.get_application_id')
  def test_auth_required_http_identity_set(self, mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'app_id'
    mock_request.return_value.scheme = 'http'
    with self.assertRaises(Unauthorized):
      foo_role(1)


  @patch('app_util.request', spec=app_util.request)
  @patch('app_util.app_identity.get_application_id')
  @patch('app_util.get_oauth_id')
  @patch('app_util.lookup_user_info')
  def test_auth_required_https_identity_set_role_not_matched(self, mock_lookup_user_info,
                                                             mock_get_oauth_id,
                                                             mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_request.headers = {}
    mock_get_oauth_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {'place':'holder'}
    with self.assertRaises(Forbidden):
      foo_role(1)
    mock_get_oauth_id.assert_called_with()
    mock_lookup_user_info.assert_called_with(mock_get_oauth_id())


  @patch('app_util.request')
  @patch('app_util.app_identity.get_application_id')
  @patch('app_util.get_oauth_id')
  @patch('app_util.lookup_user_info')
  def test_auth_required_https_identity_set_role_wrong_match(self, mock_lookup_user_info,
                                                             mock_get_oauth_id,
                                                             mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'appid'
    mock_request.scheme = 'https'
    mock_request.remote_addr = 'ip'
    mock_request.headers = {}
    mock_get_oauth_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {'roles': ['bar']}
    with self.assertRaises(Forbidden):
      foo_role(1)
    mock_get_oauth_id.assert_called_with()

  @patch('app_util.request')
  @patch('app_util.app_identity.get_application_id')
  @patch('app_util.get_oauth_id')
  @patch('app_util.lookup_user_info')
  def test_auth_required_https_identity_set_multi_role_not_matched(self, mock_lookup_user_info,
                                                                   mock_get_oauth_id,
                                                                   mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'appid'
    mock_request.scheme = 'https'
    mock_request.remote_addr = 'ip'
    mock_request.headers = {}
    mock_get_oauth_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {'place':'holder'}

    with self.assertRaises(Forbidden):
      foo_bar_role(1)

    mock_lookup_user_info.return_value = {'roles': ['foo']}
    self.assertEquals(2, foo_bar_role(1))


  @patch('app_util.request')
  @patch('app_util.app_identity.get_application_id')
  @patch('app_util.get_oauth_id')
  @patch('app_util.lookup_user_info')
  def test_auth_required_https_identity_set_role_wrong_match(self, mock_lookup_user_info,
                                                             mock_get_oauth_id,
                                                             mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'https'
    mock_request.remote_addr = 'ip'
    mock_request.headers = {}
    mock_get_oauth_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {'roles': ['baz']}

    mock_request.headers = {}
    with self.assertRaises(Forbidden):
      foo_bar_role(1)
    mock_get_oauth_id.assert_called_with()
    mock_lookup_user_info.assert_called_with(mock_get_oauth_id())

  @patch('app_util.request')
  @patch('app_util.app_identity.get_application_id')
  @patch('app_util.get_oauth_id')
  @patch('app_util.lookup_user_info')
  def test_auth_required_https_identity_set_role_match(self, mock_lookup_user_info,
                                                       mock_get_oauth_id,
                                                       mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_request.headers = {}
    mock_get_oauth_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {'roles': ['bar']}
    self.assertEquals(2, foo_bar_role(1))
    mock_get_oauth_id.assert_called_with()
    mock_lookup_user_info.assert_called_with(mock_get_oauth_id())

  @patch('app_util.request')
  @patch('app_util.app_identity.get_application_id')
  @patch('app_util.get_oauth_id')
  @patch('app_util.lookup_user_info')
  def test_auth_required_ip_ranges(self, mock_lookup_user_info,
                                   mock_get_oauth_id,
                                   mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'appid'
    mock_request.scheme = 'https'
    mock_request.remote_addr = '10.0.0.1'
    mock_request.headers = {}
    mock_get_oauth_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {
            'roles': ['bar'],
            'whitelisted_ip_ranges': {'ip4': ['10.0.0.2/32'], 'ip6': []}}

    with self.assertRaises(Forbidden):
      foo_bar_role(1)

    mock_request.remote_addr = '10.0.0.2'
    self.assertEquals(2, foo_bar_role(1))

  @patch('app_util.request')
  @patch('app_util.app_identity.get_application_id')
  @patch('app_util.get_oauth_id')
  @patch('app_util.lookup_user_info')
  def test_auth_required_appid(self, mock_lookup_user_info,
                               mock_get_oauth_id,
                               mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'appid'
    mock_request.scheme = 'https'
    mock_request.remote_addr = '10.0.0.1'
    mock_request.headers = {}
    mock_get_oauth_id.return_value = 'bob@example.com'

    mock_lookup_user_info.return_value = {
            'roles': ['bar'],
            'whitelisted_appids': ['must-be-this-id'],
            }

    with self.assertRaises(Forbidden):
      foo_bar_role(1)

    mock_request.headers = {
        'X-Appengine-Inbound-Appid': 'must-be-this-id'
    }
    self.assertEquals(2, foo_bar_role(1))

  def test_no_roles_supplied_to_decorator(self):
    with self.assertRaises(TypeError):
      @app_util.auth_required()
      def _(): pass
    with self.assertRaises(AssertionError):
      @app_util.auth_required(None)
      def _(): pass

  @patch('app_util.request', spec=app_util.request)
  @patch('app_util.get_oauth_id')
  def test_check_auth_required_cron(self, mock_get_oauth_id, mock_request):
    mock_get_oauth_id.return_value = 'bob@example.com'

    mock_request.headers = {'X-Appengine-Cron': 'true'}
    self.assertEquals(2, cron_required(1))

    mock_request.headers = {}
    with self.assertRaises(Forbidden):
      cron_required(1)

  def test_nonprod(self):
    # The dev config is isntalled by default for tests, reset.
    config.override_setting(config.ALLOW_NONPROD_REQUESTS, False)
    with self.assertRaises(Forbidden):
      not_in_prod()

    config.override_setting(config.ALLOW_NONPROD_REQUESTS, True)
    not_in_prod()

if __name__ == '__main__':
  unittest.main()
