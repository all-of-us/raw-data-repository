"""Utils for unit tests."""

import unittest

from google.appengine.ext import ndb
from google.appengine.ext import testbed


class TestBase(unittest.TestCase):
  """Base class for unit tests."""

  def setUp(self):
    # Allow printing the full diff report on errors.
    self.maxDiff = None

class TestbedTestBase(TestBase):
  """Base class for unit tests that need the testbed."""

  def setUp(self):
    super(TestbedTestBase, self).setUp()
    self.testbed = testbed.Testbed()
    self.testbed.activate()

  def tearDown(self):
    self.testbed.deactivate()
    super(TestbedTestBase, self).tearDown()


class NdbTestBase(TestbedTestBase):
  """Base class for unit tests that need the NDB testbed."""

  def setUp(self):
    super(NdbTestBase, self).setUp()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    ndb.get_context().clear_cache()

def strip_last_modified(obj):
  assert obj.last_modified, 'Missing last_modified: {}'.format(obj)
  obj.last_modified = None
  return obj
