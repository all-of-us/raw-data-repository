"""Tests for gcloud_ips"""

import unittest
from netaddr import IPNetwork

class GcloudIpsTest(unittest.TestCase):

  def test_gcloud_ips(self):
    from offline.gcloud_ips import START, get_ip_ranges
    response = get_ip_ranges(START)
    self.assertEqual(response.next_entries, [])
    self.assertTrue(len(response.ip4) > 0)
    self.assertTrue(len(response.ip6) > 0)
    for a in response.ip4 + response.ip6:
      # Parse to ensure values are valid CIDR ranges
      IPNetwork(a)
