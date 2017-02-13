import copy
import datetime
import random
import string
import time
import unittest

import clock
import config
from test.unit_test.unit_test_util import FlaskTestBase


class TestConfig(FlaskTestBase):
  def test_replace_history(self):
    fake_clock = clock.FakeClock(datetime.datetime.utcnow())
    self.set_auth_user(self._ADMIN_USER)
    orig_config = self.get_json('Config')

    # Replace some data in the current config.
    test_key = 'testing_config_key'
    new_config_1 = copy.deepcopy(orig_config)
    new_config_1[test_key] = ['initially', 'injected', 'values']
    with fake_clock:
      self.open_json('PUT', 'Config', request_data=new_config_1)

    # Make sure the replacements show up when re-fetching the config.
    with fake_clock:
      response = self.get_json('Config')
    self.assertEquals(new_config_1, response)

    fake_clock.advance()
    between_updates = fake_clock.now
    fake_clock.advance()

    # Make sure another replacement takes effect.
    new_config_2 = copy.deepcopy(orig_config)
    new_config_2[test_key] = ['afterwards', 'replaced', 'values']
    with fake_clock:
      self.open_json('PUT', 'Config', new_config_2)
      response = self.get_json('Config')
    self.assertEquals(new_config_2, response)

    # Make sure we get the the first replacement config when we query by time.
    with fake_clock:
      response = self.get_json('Config/{}'.format(between_updates.isoformat()))
    self.assertEquals(new_config_1, response)


if __name__ == '__main__':
  unittest.main()
