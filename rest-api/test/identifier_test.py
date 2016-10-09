"""Tests for identifier."""
import time
import threading
import unittest

from google.appengine.api.datastore_errors import TransactionFailedError
from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.ext import testbed


class IdentifierTest(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    ndb.get_context().clear_cache()

  def test_reserve_candidate(self):
    import identifier
    self.assertTrue(identifier._reserve_candidate(1))
    # Can't reserve the same id twice.
    self.assertFalse(identifier._reserve_candidate(1))

  def test_conflict(self):
    import identifier

    def make_res():
      self.assertTrue(identifier._reserve_candidate(2, testing_sleep=.1))

    threading.Thread(target=make_res).start()
    try:
      identifier._reserve_candidate(2, testing_sleep=.22)
      self.fail()
    except TransactionFailedError as e:
      pass


if __name__ == '__main__':
  unittest.main()
