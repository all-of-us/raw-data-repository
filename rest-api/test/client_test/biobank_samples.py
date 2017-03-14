import unittest

import test_util


class TestBiobankSamples(unittest.TestCase):
  def setUp(self):
    self.client = test_util.get_client('offline')

  def test_reload_no_files(self):
    self.client.request_json('BiobankSamplesImport', 'GET', cron=True)


if __name__ == '__main__':
  unittest.main()
