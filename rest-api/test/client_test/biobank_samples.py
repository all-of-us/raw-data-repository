"""Test for the biobank samples endpoint."""

import json
import unittest
from client.client import HttpException

import test_util

class TestBiobankSamples(unittest.TestCase):
  def setUp(self):
    self.client = test_util.get_client('offline')

  def test_reload_no_files(self):
    self.client.request_json('BiobankSamplesReload', 'GET', cron=True)

if __name__ == '__main__':
  unittest.main()
