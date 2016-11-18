"""Tests for api_util."""

import api_util
import config

from api_util import SCOPE
from mock import MagicMock, patch
from test.unit_test.unit_test_util import NdbTestBase
from werkzeug.exceptions import Unauthorized

@api_util.auth_required()
def no_roles(x):
  return x + 1
  
@api_util.auth_required('foo')
def foo_role(x):
  return x + 1
  
@api_util.auth_required(['foo', 'bar'])
def foo_bar_role(x):
  return x + 1

class ApiUtilNdbTest(NdbTestBase):

  def setUp(self):
    super(ApiUtilNdbTest, self).setUp()

    self.user_info = {
      "example@example.com": {
         "roles": ["role1", "role2"],
         "ip_ranges": {
          "ip6": ["1234:5678::/32"],
          "ip4": ["123.210.0.1/16"]
         }
      }
    }

    # Note that there is a ttl cache on this config value, so it can't be changed during the test.
    config.insert_config(config.USER_INFO, self.user_info)

  def test_valid_ip(self):
    allowed_ips = api_util.allowed_ips(self.user_info["example@example.com"])
    api_util.enforce_ip_whitelisted('123.210.0.1', allowed_ips)
    api_util.enforce_ip_whitelisted('123.210.111.0', allowed_ips)

    api_util.enforce_ip_whitelisted('1234:5678::', allowed_ips)
    api_util.enforce_ip_whitelisted('1234:5678:9999::', allowed_ips)

  def test_invalid_ip(self):
    allowed_ips = api_util.allowed_ips(self.user_info["example@example.com"])
    with self.assertRaises(Unauthorized):
      api_util.enforce_ip_whitelisted('100.100.0.1', allowed_ips)

    with self.assertRaises(Unauthorized):
      api_util.enforce_ip_whitelisted('5555::', allowed_ips)

  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')  
  def test_auth_required_http_identity_set(self, mock_get_application_id, mock_request):
    mock_get_application_id.return_value = 'app_id'
    mock_request.return_value.scheme = 'http'    
    try:
      no_roles(1)
      self.fail("Should have been forbidden")
    except Unauthorized:
      pass
  
  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.oauth.get_current_user')
  @patch('api_util.check_user_info')
  def test_auth_required_https_identity_set_no_roles(self, mock_check_user_info, 
                                                     mock_get_current_user, 
                                                     mock_get_application_id, mock_request):
    user = MagicMock()
    mock_get_application_id.return_value = 'app_id'
    mock_request.scheme = 'https'
    mock_request.remote_addr = 'ip'
    mock_get_current_user.return_value = user
    mock_check_user_info.return_value = {}    
    self.assertEquals(2,  no_roles(1))
    mock_get_current_user.assert_called_with(SCOPE)
    mock_check_user_info.assert_called_with(user, 'ip')
        
  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.oauth.get_current_user')
  @patch('api_util.check_user_info')
  def test_auth_required_http_no_identity_set_no_roles(self, mock_check_user_info, 
                                                       mock_get_current_user, 
                                                       mock_get_application_id, mock_request):
    user = MagicMock()
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_get_current_user.return_value = user
    mock_check_user_info.return_value = {}    
    self.assertEquals(2,  no_roles(1))
    mock_get_current_user.assert_called_with(SCOPE)
    mock_check_user_info.assert_called_with(user, 'ip')
    
  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.oauth.get_current_user')
  @patch('api_util.check_user_info')
  def test_auth_required_https_identity_set_role_not_matched(self, mock_check_user_info, 
                                                             mock_get_current_user, 
                                                             mock_get_application_id, mock_request):
    user = MagicMock()
    user.email.return_value = "bob"
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_get_current_user.return_value = user
    mock_check_user_info.return_value = { }    
    try:
      foo_role(1)
      self.fail("Should have been forbidden")
    except Unauthorized:
      pass
    mock_get_current_user.assert_called_with(SCOPE)
    mock_check_user_info.assert_called_with(user, 'ip')
    
  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.oauth.get_current_user')
  @patch('api_util.check_user_info')
  def test_auth_required_https_identity_set_role_wrong_match(self, mock_check_user_info, 
                                                             mock_get_current_user, 
                                                             mock_get_application_id, mock_request):
    user = MagicMock()
    user.email.return_value = "bob"
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_get_current_user.return_value = user
    mock_check_user_info.return_value = { 'roles': ['bar']}    
    try:
      foo_role(1)
      self.fail("Should have been forbidden")
    except Unauthorized:
      pass
    mock_get_current_user.assert_called_with(SCOPE)
    mock_check_user_info.assert_called_with(user, 'ip')

  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.oauth.get_current_user')
  @patch('api_util.check_user_info')
  def test_auth_required_https_identity_set_role_match(self, mock_check_user_info, 
                                                       mock_get_current_user, 
                                                       mock_get_application_id, mock_request):
    user = MagicMock()
    user.email.return_value = "bob"
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_get_current_user.return_value = user
    mock_check_user_info.return_value = { 'roles': ['foo']}    
    self.assertEquals(2, foo_role(1))
    mock_get_current_user.assert_called_with(SCOPE)
    mock_check_user_info.assert_called_with(user, 'ip')
    
  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.oauth.get_current_user')
  @patch('api_util.check_user_info')
  def test_auth_required_https_identity_set_multi_role_not_matched(self, mock_check_user_info, 
                                                                   mock_get_current_user, 
                                                                   mock_get_application_id, mock_request):
    user = MagicMock()
    user.email.return_value = "bob"
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_get_current_user.return_value = user
    mock_check_user_info.return_value = { }    
    try:
      foo_bar_role(1)
      self.fail("Should have been forbidden")
    except Unauthorized:
      pass
    mock_get_current_user.assert_called_with(SCOPE)
    mock_check_user_info.assert_called_with(user, 'ip')
    
  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.oauth.get_current_user')
  @patch('api_util.check_user_info')
  def test_auth_required_https_identity_set_role_wrong_match(self, mock_check_user_info, 
                                                             mock_get_current_user, 
                                                             mock_get_application_id, mock_request):
    user = MagicMock()
    user.email.return_value = "bob"
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_get_current_user.return_value = user
    mock_check_user_info.return_value = { 'roles': ['baz']}    
    try:
      foo_bar_role(1)
      self.fail("Should have been forbidden")
    except Unauthorized:
      pass
    mock_get_current_user.assert_called_with(SCOPE)
    mock_check_user_info.assert_called_with(user, 'ip')

  @patch('api_util.request')
  @patch('api_util.app_identity.get_application_id')
  @patch('api_util.oauth.get_current_user')
  @patch('api_util.check_user_info')
  def test_auth_required_https_identity_set_role_match(self, mock_check_user_info, 
                                                       mock_get_current_user, 
                                                       mock_get_application_id, mock_request):
    user = MagicMock()
    user.email.return_value = "bob"
    mock_get_application_id.return_value = 'None'
    mock_request.scheme = 'http'
    mock_request.remote_addr = 'ip'
    mock_get_current_user.return_value = user
    mock_check_user_info.return_value = { 'roles': ['bar']}    
    self.assertEquals(2, foo_bar_role(1))
    mock_get_current_user.assert_called_with(SCOPE)
    mock_check_user_info.assert_called_with(user, 'ip')

    