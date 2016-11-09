"""Test for the config endpoint."""

import random
import string
import time
import unittest

import test_util

class TestConfig(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')

  def test_insert_get_list(self):
    random_strs = [''.join(random.choice(string.uppercase) for _ in range(10)) for _ in range(3)]

    post_json = {'values': random_strs}
    self.client.request_json('Config/random_test', 'POST', post_json)
    time.sleep(2) # It takes a tiny bit to update the config index.
    expected = {'key': 'random_test', 'values': sorted(random_strs)}

    response = self.client.request_json('Config/random_test', 'GET')
    print 'RESPONSE', response
    response['values'] = sorted(response['values'])
    self.assertEquals(expected, response)

    vals = self.client.request_json('Config', 'GET')
    for val in vals:
      val['values'] = sorted(val['values'])

    self.assertIn(expected, vals)

if __name__ == '__main__':
  unittest.main()
