"""Tests for api_util."""

import api_util
import config

from test.unit_test.unit_test_util import NdbTestBase
from werkzeug.exceptions import Unauthorized

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
