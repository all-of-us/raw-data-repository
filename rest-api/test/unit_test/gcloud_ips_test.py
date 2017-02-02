"""Tests for gcloud_ips"""

import dns
import unittest
from mock import Mock, MagicMock

import offline.gcloud_ips
from offline.gcloud_ips import START, get_ip_ranges

class GcloudIpsTest(unittest.TestCase):
  DNS_RESPONSES = [
    '_cloud-netblocks.googleusercontent.com. 2677 IN TXT "v=spf1 include:_cloud-netblocks1.googleusercontent.com include:_cloud-netblocks2.googleusercontent.com include:_cloud-netblocks3.googleusercontent.com include:_cloud-netblocks4.googleusercontent.com include:_cloud-netblocks5.googleusercontent.com ?all"',
    '_cloud-netblocks1.googleusercontent.com. 3599 IN TXT "v=spf1 ip4:8.34.208.0/20 ip4:8.35.192.0/21 ip4:8.35.200.0/23 ip4:108.59.80.0/20 ip4:108.170.192.0/20 ip4:108.170.208.0/21 ip4:108.170.216.0/22 ip4:108.170.220.0/23 ip4:108.170.222.0/24 ?all"',
    '_cloud-netblocks2.googleusercontent.com. 3599 IN TXT "v=spf1 ip4:162.216.148.0/22 ip4:162.222.176.0/21 ip4:173.255.112.0/20 ip4:192.158.28.0/22 ip4:199.192.112.0/22 ip4:199.223.232.0/22 ip4:199.223.236.0/23 ip4:23.236.48.0/20 ip4:23.251.128.0/19 ?all"',
    '_cloud-netblocks3.googleusercontent.com. 3599 IN TXT "v=spf1 ip4:107.167.160.0/19 ip4:107.178.192.0/18 ip4:146.148.2.0/23 ip4:146.148.4.0/22 ip4:146.148.8.0/21 ip4:146.148.16.0/20 ip4:146.148.32.0/19 ip4:146.148.64.0/18 ip4:130.211.4.0/22 ?all"',
    '_cloud-netblocks4.googleusercontent.com. 3599 IN TXT "v=spf1 ip4:130.211.8.0/21 ip4:130.211.16.0/20 ip4:130.211.32.0/19 ip4:130.211.64.0/18 ip4:130.211.128.0/17 ip4:104.154.0.0/15 ip4:104.196.0.0/14 ip4:208.68.108.0/23 ip4:35.184.0.0/15 ip4:35.186.0.0/16 ?all"',
    '_cloud-netblocks5.googleusercontent.com. 3226 IN TXT "v=spf1 ip6:2600:1900::/35 ?all"'
  ]


  def test_gcloud_ips(self):
    offline.gcloud_ips.lookup_txt = MagicMock(side_effect=self.DNS_RESPONSES)
    response = get_ip_ranges(START)
    self.assertEqual(6, offline.gcloud_ips.lookup_txt.call_count)
    self.assertEqual(response.next_entries, [])
    self.assertEqual(response.ip4, [
        '107.167.160.0/19',
        '107.178.192.0/18',
        '146.148.2.0/23',
        '146.148.4.0/22',
        '146.148.8.0/21',
        '146.148.16.0/20',
        '146.148.32.0/19',
        '146.148.64.0/18',
        '130.211.4.0/22',
        '130.211.8.0/21',
        '130.211.16.0/20',
        '130.211.32.0/19',
        '130.211.64.0/18',
        '130.211.128.0/17',
        '104.154.0.0/15',
        '104.196.0.0/14',
        '208.68.108.0/23',
        '35.184.0.0/15',
        '35.186.0.0/16',
        '162.216.148.0/22',
        '162.222.176.0/21',
        '173.255.112.0/20',
        '192.158.28.0/22',
        '199.192.112.0/22',
        '199.223.232.0/22',
        '199.223.236.0/23',
        '23.236.48.0/20',
        '23.251.128.0/19',
        '8.34.208.0/20',
        '8.35.192.0/21',
        '8.35.200.0/23',
        '108.59.80.0/20',
        '108.170.192.0/20',
        '108.170.208.0/21',
        '108.170.216.0/22',
        '108.170.220.0/23',
        '108.170.222.0/24'])
    self.assertEqual(response.ip6, ['2600:1900::/35'])
