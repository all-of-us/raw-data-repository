"""Test for the config endpoint."""

import copy
import datetime
import random
import string
import time
import unittest

import test_util

class TestConfig(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')

  def test_replace_history(self):
    random_strs_1 = [''.join(random.choice(string.uppercase) for _ in range(10)) for _ in range(3)]
    old_config = self.client.request_json('Config', 'GET',
                                          dev_appserver_admin=True)
    new_config_1 = copy.deepcopy(old_config)
    new_config_1['some_config'] = sorted(random_strs_1)

    self.client.request_json('Config', 'PUT', new_config_1,
                             dev_appserver_admin=True)

    response = self.client.request_json('Config', 'GET',
                             dev_appserver_admin=True)
    response['some_config'] = sorted(response['some_config'])
    self.assertEquals(new_config_1, response)

    then = datetime.datetime.utcnow()

    random_strs_2 = [''.join(random.choice(string.uppercase) for _ in range(10)) for _ in range(3)]
    new_config_2 = copy.deepcopy(old_config)
    new_config_2['some_config'] = sorted(random_strs_2)
    self.client.request_json('Config', 'PUT', new_config_2,
                             dev_appserver_admin=True)

    response = self.client.request_json('Config', 'GET',
                             dev_appserver_admin=True)
    response['some_config'] = sorted(response['some_config'])
    self.assertEquals(new_config_2, response)

    for _ in range(40):
      # Make sure we get the the first config when we query by time.
      response = self.client.request_json(
          'Config/{}'.format(then.isoformat()), 'GET',
                             dev_appserver_admin=True)
      response['some_config'] = sorted(response['some_config'])
      if new_config_1 == response:
        break
      time.sleep(.25) # The history is based on an index which may take a little while to update.
      print 'Waiting on index update..'
      self.assertEquals(new_config_1, response)


if __name__ == '__main__':
  unittest.main()
