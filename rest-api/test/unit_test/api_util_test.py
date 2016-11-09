"""Tests for api_util."""

import api_util
import config

from test.unit_test.unit_test_util import NdbTestBase
from werkzeug.exceptions import Unauthorized

class ApiUtilNdbTest(NdbTestBase):

  def setUp(self):
    super(ApiUtilNdbTest, self).setUp()

    # Note that there is a ttl cache on this config value, so it can't be changed during the test.
    config.insert_config(key=config.ALLOWED_IP,
                         value='{"ip6": ["1234:5678::/32"], "ip4": ["123.210.0.1/16"]}')

  def test_valid_ip(self):
    api_util.enforce_ip_whitelisted('123.210.0.1')
    api_util.enforce_ip_whitelisted('123.210.111.0')

    api_util.enforce_ip_whitelisted('1234:5678::')
    api_util.enforce_ip_whitelisted('1234:5678:9999::')

  def test_invalid_ip(self):
    with self.assertRaises(Unauthorized):
      api_util.enforce_ip_whitelisted('100.100.0.1')

    with self.assertRaises(Unauthorized):
      api_util.enforce_ip_whitelisted('5555::')
