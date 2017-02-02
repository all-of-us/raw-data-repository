"""Tests for identifier."""
import identifier
import threading
import unittest

from google.appengine.api.datastore_errors import TransactionFailedError

from test.unit_test.unit_test_util import NdbTestBase

class IdentifierTest(NdbTestBase):

  def test_reserve_candidate(self):
    self.assertTrue(identifier._reserve_candidate(1))
    # Can't reserve the same id twice.
    self.assertFalse(identifier._reserve_candidate(1))

  def test_conflict(self):
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
