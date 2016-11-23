"""Tests for api_util."""

import api_util
import config

from mock import MagicMock, patch
from test.unit_test.unit_test_util import NdbTestBase
from werkzeug.exceptions import Unauthorized


@api_util.auth_required('foo')
def foo_role(x):
  return x + 1

@api_util.auth_required(['foo', 'bar'])
def foo_bar_role(x):
  return x + 1

@api_util.auth_required_cron_or_admin
def admin_required(x):
  return x + 1


class ApiUtilNdbTest(NdbTestBase):

  def setUp(self):
    super(ApiUtilNdbTest, self).setUp()

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

  def test_valid_ip(self):
    allowed_ips = api_util.whitelisted_ips(self.user_info["example@example.com"])
    api_util.enforce_ip_whitelisted('123.210.0.1', allowed_ips)
    api_util.enforce_ip_whitelisted('123.210.111.0', allowed_ips)

    api_util.enforce_ip_whitelisted('1234:5678::', allowed_ips)
    api_util.enforce_ip_whitelisted('1234:5678:9999::', allowed_ips)

  def test_invalid_ip(self):
    allowed_ips = api_util.whitelisted_ips(self.user_info["example@example.com"])
    with self.assertRaises(Unauthorized):
      api_util.enforce_ip_whitelisted('100.100.0.1', allowed_ips)

    with self.assertRaises(Unauthorized):
      api_util.enforce_ip_whitelisted('5555::', allowed_ips)

  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  def test_auth_required_http_identity_set(self, mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'app_id'
    mock_request.return_value.scheme = 'http'
    with self.assertRaises(Unauthorized):
      foo_role(1)


  @patch('api_util.request', spec=api_util.request)
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.get_client_id')
  @patch('api_util.lookup_user_info')
  def test_auth_required_https_identity_set_role_not_matched(self, mock_lookup_user_info,
                                                             mock_get_client_id,
                                                             mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_request.headers = {}
    mock_get_client_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {'place':'holder'}
    with self.assertRaises(Unauthorized):
      foo_role(1)
    mock_get_client_id.assert_called_with()
    mock_lookup_user_info.assert_called_with(mock_get_client_id())


  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.get_client_id')
  @patch('api_util.lookup_user_info')
  def test_auth_required_https_identity_set_role_wrong_match(self, mock_lookup_user_info,
                                                             mock_get_client_id,
                                                             mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'appid'
    mock_request.scheme = 'https'
    mock_request.remote_addr = 'ip'
    mock_request.headers = {}
    mock_get_client_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {'roles': ['bar']}
    with self.assertRaises(Unauthorized):
      foo_role(1)
    mock_get_client_id.assert_called_with()

  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.get_client_id')
  @patch('api_util.lookup_user_info')
  def test_auth_required_https_identity_set_multi_role_not_matched(self, mock_lookup_user_info,
                                                                   mock_get_client_id,
                                                                   mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'appid'
    mock_request.scheme = 'https'
    mock_request.remote_addr = 'ip'
    mock_request.headers = {}
    mock_get_client_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {'place':'holder'}

    with self.assertRaises(Unauthorized):
      foo_bar_role(1)

    mock_lookup_user_info.return_value = {'roles': ['foo']}
    self.assertEquals(2, foo_bar_role(1))


  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.get_client_id')
  @patch('api_util.lookup_user_info')
  def test_auth_required_https_identity_set_role_wrong_match(self, mock_lookup_user_info,
                                                             mock_get_client_id,
                                                             mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'https'
    mock_request.remote_addr = 'ip'
    mock_request.headers = {}
    mock_get_client_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {'roles': ['baz']}

    mock_request.headers = {}
    with self.assertRaises(Unauthorized):
      foo_bar_role(1)
    mock_get_client_id.assert_called_with()
    mock_lookup_user_info.assert_called_with(mock_get_client_id())

  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.get_client_id')
  @patch('api_util.lookup_user_info')
  def test_auth_required_https_identity_set_role_match(self, mock_lookup_user_info,
                                                       mock_get_client_id,
                                                       mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_request.headers = {}
    mock_get_client_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {'roles': ['bar']}
    self.assertEquals(2, foo_bar_role(1))
    mock_get_client_id.assert_called_with()
    mock_lookup_user_info.assert_called_with(mock_get_client_id())

  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.get_client_id')
  @patch('api_util.lookup_user_info')
  def test_auth_required_ip_ranges(self, mock_lookup_user_info,
                                   mock_get_client_id,
                                   mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'appid'
    mock_request.scheme = 'https'
    mock_request.remote_addr = '10.0.0.1'
    mock_request.headers = {}
    mock_get_client_id.return_value = 'bob@example.com'
    mock_lookup_user_info.return_value = {
            'roles': ['bar'],
            'whitelisted_ip_ranges': {'ip4': ['10.0.0.2/32'], 'ip6': []}}

    with self.assertRaises(Unauthorized):
      foo_bar_role(1)

    mock_request.remote_addr = '10.0.0.2'
    self.assertEquals(2, foo_bar_role(1))

  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.get_client_id')
  @patch('api_util.lookup_user_info')
  def test_auth_required_appid(self, mock_lookup_user_info,
                               mock_get_client_id,
                               mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'appid'
    mock_request.scheme = 'https'
    mock_request.remote_addr = '10.0.0.1'
    mock_request.headers = {}
    mock_get_client_id.return_value = 'bob@example.com'

    mock_lookup_user_info.return_value = {
            'roles': ['bar'],
            'whitelisted_appids': ['must-be-this-id'],
            }

    with self.assertRaises(Unauthorized):
      foo_bar_role(1)

    mock_request.headers = {
        'X-Appengine-Inbound-Appid': 'must-be-this-id'
    }
    self.assertEquals(2, foo_bar_role(1))

  def test_no_roles_supplied_to_decorator(self):
    with self.assertRaises(TypeError):
      @api_util.auth_required()
      def placeholder(): pass
    with self.assertRaises(AssertionError):
      @api_util.auth_required(None)
      def placeholder(): pass

  @patch('api_util.users.is_current_user_admin')
  def test_check_auth_required_cron_or_admin(self, mock_is_current_user_admin):
    mock_is_current_user_admin.return_value = True
    self.assertEquals(2, admin_required(1))

    mock_is_current_user_admin.return_value = False
    with self.assertRaises(Unauthorized):
      admin_required(1)
