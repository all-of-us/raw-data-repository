"""Tests for data_access_object."""

import data_access_object
import unittest

from datetime import datetime

from google.appengine.api.datastore_errors import TransactionFailedError
from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.ext import testbed

from werkzeug.exceptions import Conflict
from werkzeug.exceptions import NotFound


class ParentModel(ndb.Model):
  foo = ndb.StringProperty()

class ChildModel(ndb.Model):
  bar = ndb.StringProperty()


class ParentModelDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ParentModelDAO, self).__init__(ParentModel)

class ChildModelDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ChildModelDAO, self).__init__(ChildModel, ParentModel)


PARENT_DAO = ParentModelDAO()
CHILD_DAO = ChildModelDAO()

class DataAccessObjectTest(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    ndb.get_context().clear_cache()

  def test_store_load(self):
    parent_id = 'parentID1'
    parent = ParentModel(key=ndb.Key(ParentModel, parent_id))
    parent.foo = "Foo"
    PARENT_DAO.store(parent)
    self.assertEquals(parent, PARENT_DAO.load(parent_id))

    child = ChildModel(key=ndb.Key(ParentModel, parent_id, ChildModel, "1"))
    child.bar = "1"
    CHILD_DAO.store(child)
    self.assertEquals(child, CHILD_DAO.load("1", parent_id))

  def test_insert(self):
    parent_id = 'parentID1'
    parent = ParentModel(key=ndb.Key(ParentModel, parent_id))
    parent.foo = "Foo"
    PARENT_DAO.insert(parent)
    self.assertEquals(parent, PARENT_DAO.load(parent_id))
    
    try:
      PARENT_DAO.insert(parent)
      self.fail('Repeated insert should fail.')
    except Conflict:
      pass
    
  def test_update(self):
    parent_id = 'parentID1'
    parent = ParentModel(key=ndb.Key(ParentModel, parent_id))
    parent.foo = "Foo"
    try:
      PARENT_DAO.update(parent)
      self.fail('Update before insert should fail.')
    except NotFound:
      pass
    PARENT_DAO.insert(parent)    
    parent.foo = "BAR"
    PARENT_DAO.update(parent)
    self.assertEquals(parent, PARENT_DAO.load(parent_id))  

  def test_history(self):
    dates = [datetime(2016, 10, 1) for i in range(3)]
    client_ids = ["client {}".format(i) for i in range(3)]

    for i in range(3):
      obj = ParentModel(key=ndb.Key(ParentModel, "1"))
      obj.foo = str(i)
      PARENT_DAO.store(obj, dates[i], client_id="client {}".format(i))
      self.assertEquals(obj, PARENT_DAO.load("1"))

    key = ndb.Key(ParentModel, "1")
    actual_history = PARENT_DAO.get_all_history(key)
    self.assertEquals(sorted(dates), sorted(h.date for h in actual_history))
    self.assertEquals(sorted(client_ids), 
                      sorted(h.client_id for h in actual_history))
    self.assertEquals(range(3), sorted(int(h.obj.foo) for h in actual_history))

  def test_history_child(self):
    dates = [datetime(2016, 10, 1) for i in range(3)]
    client_ids = ["client {}".format(i) for i in range(3)]
    parent_id = "p1"
    parent = ParentModel(key=ndb.Key(ParentModel, parent_id))
    PARENT_DAO.store(parent)

    for i in range(3):
      obj = ChildModel(key=ndb.Key(ParentModel, parent_id, ChildModel, "1"))
      obj.bar = str(i)
      CHILD_DAO.store(obj, dates[i], client_id="client {}".format(i))
      self.assertEquals(obj, CHILD_DAO.load("1", parent_id))

    key = ndb.Key(ParentModel, parent_id, ChildModel, "1")
    actual_history = CHILD_DAO.get_all_history(key)
    self.assertEquals(sorted(dates), sorted(h.date for h in actual_history))
    self.assertEquals(sorted(client_ids), 
                      sorted(h.client_id for h in actual_history))
    self.assertEquals(range(3), sorted(int(h.obj.bar) for h in actual_history))

if __name__ == '__main__':
  unittest.main()
