"""Test for the config endpoint."""

import random
import string
import time
import unittest

import test_util

# The amount of times we will retry while waiting for the index to be updated.
RETRIES = 60 # At least 30s at 0.5 second per sleep (doesn't count time to make request).

# How long to wait between attempts.
SLEEP_AMT = 0.5


class TestConfig(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')

  def test_insert_get_list(self):
    random_strs = [''.join(random.choice(string.uppercase) for _ in range(10)) for _ in range(3)]

    post_json = {'values': random_strs}
    self.client.request_json('Config/random_test', 'POST', post_json)

    expected = {'key': 'random_test', 'values': sorted(random_strs)}
    for _ in range(RETRIES): # Retries
      response = self.client.request_json('Config/random_test', 'GET')
      response['values'] = sorted(response['values'])
      time.sleep(SLEEP_AMT) # It takes a bit to update the config index.
      if expected == response:
        break
      print "Waiting on index"

    self.assertEquals(expected, response)

    vals = self.client.request_json('Config', 'GET')
    for val in vals:
      val['values'] = sorted(val['values'])

    self.assertIn(expected, vals)

  def test_replace(self):
    starting_vals = ['A', 'B', 'C']

    post_json = {'values': starting_vals}
    self.client.request_json('Config/replace_test', 'POST', post_json)

    for _ in range(RETRIES): # Retries
      response = self.client.request_json('Config/replace_test', 'GET')
      response['values'] = sorted(response['values'])
      time.sleep(SLEEP_AMT) # It takes a tiny bit to update the config index.
      if sorted(response['values']) == starting_vals:
        break
      print "Waiting on index"

    # The new set doesn't contain 'C', but contains a new entry 'D'.
    new_vals = ['A', 'B', 'D']
    post_json = {'values': new_vals}
    self.client.request_json('Config/replace_test', 'POST', post_json)

    for _ in range(RETRIES): # Retries
      response = self.client.request_json('Config/replace_test', 'GET')
      response['values'] = sorted(response['values'])
      time.sleep(SLEEP_AMT) # It takes a tiny bit to update the config index.
      if sorted(response['values']) == new_vals:
        break
      print "Waiting on index"

    expected = {'key': 'replace_test', 'values': sorted(new_vals)}
    response = self.client.request_json('Config/replace_test', 'GET')
    response['values'] = sorted(response['values'])
    self.assertEquals(expected, response)

if __name__ == '__main__':
  unittest.main()
