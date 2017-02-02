"""Tests for data_access_object."""

import api_util
import data_access_object
import unittest

from datetime import datetime
from query import Query, OrderBy, FieldFilter, Operator

from google.appengine.ext import ndb

from test.unit_test.unit_test_util import NdbTestBase, to_dict_strip_last_modified

from werkzeug.exceptions import Conflict, NotFound, PreconditionFailed

class ParentModel(ndb.Model):
  foo = ndb.StringProperty()
  fooSearch = ndb.ComputedProperty(
      lambda self: api_util.searchable_representation(self.foo))
  last_modified = ndb.DateTimeProperty(auto_now=True)

class ChildModel(ndb.Model):
  bar = ndb.StringProperty()
  last_modified = ndb.DateTimeProperty(auto_now=True)

class ParentModelDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ParentModelDAO, self).__init__(ParentModel)

  def validate_query(self, query_definition):
    return

class ChildModelDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ChildModelDAO, self).__init__(ChildModel, ParentModel)

PARENT_DAO = ParentModelDAO()
CHILD_DAO = ChildModelDAO()

class DataAccessObjectTest(NdbTestBase):

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
    expected_version_id = '12345'
    parent = ParentModel(key=ndb.Key(ParentModel, parent_id))
    parent.foo = "Foo"
    try:
      PARENT_DAO.update(parent, expected_version_id)
      self.fail('Update before insert should fail.')
    except NotFound:
      pass
    PARENT_DAO.insert(parent)
    parent.foo = "BAR"
    try:
      PARENT_DAO.update(parent, None)
      self.fail('Update without expected version id should fail')
    except PreconditionFailed:
      pass
    try:
      PARENT_DAO.update(parent, expected_version_id)
      self.fail('Update with wrong expected version id should fail')
    except PreconditionFailed:
      pass
    parent = PARENT_DAO.load(parent_id)
    parent.foo = "BAR"
    PARENT_DAO.update(parent, PARENT_DAO.make_version_id(parent.last_modified))

    self.assertEquals(to_dict_strip_last_modified(parent),
                      to_dict_strip_last_modified(PARENT_DAO.load(parent_id)))

  def test_query_no_filters(self):
    query = Query([], OrderBy(field_name='foo', ascending=True), 2, None)
    results = PARENT_DAO.query(query)
    self.assertEquals([], results.items)
    self.assertEquals(None, results.pagination_token)

    parent1 = ParentModel(key=ndb.Key(ParentModel, '321'), foo="a")
    PARENT_DAO.insert(parent1)
    results = PARENT_DAO.query(query)
    self.assertEquals([parent1], results.items)
    self.assertEquals(None, results.pagination_token)

    parent2 = ParentModel(key=ndb.Key(ParentModel, '123'), foo="bob")
    PARENT_DAO.insert(parent2)
    parent3 = ParentModel(key=ndb.Key(ParentModel, '456'), foo="AARDVARK")
    PARENT_DAO.insert(parent3)
    results = PARENT_DAO.query(query)
    self.assertEquals([parent1, parent3], results.items)
    self.assertTrue(results.pagination_token)

    query2 = Query([], OrderBy(field_name='foo', ascending=True), 2, results.pagination_token)
    results2 = PARENT_DAO.query(query2)
    self.assertEquals([parent2], results2.items)
    self.assertEquals(None, results2.pagination_token)

    # Now in descending order
    query3 = Query([], OrderBy(field_name='foo', ascending=False), 2, None)
    results3 = PARENT_DAO.query(query3)
    self.assertEquals([parent2, parent3], results3.items)
    self.assertTrue(results3.pagination_token)

    query4 = Query([], OrderBy(field_name='foo', ascending=False), 2, results3.pagination_token)
    results4 = PARENT_DAO.query(query4)
    self.assertEquals([parent1], results4.items)
    self.assertEquals(None, results4.pagination_token)

    # Now sort by last_modified
    query5 = Query([], OrderBy(field_name='last_modified', ascending=True), 3, None)
    results5 = PARENT_DAO.query(query5)
    self.assertEquals([parent1, parent2, parent3], results5.items)
    self.assertEquals(None, results5.pagination_token)

  def test_query_with_filters(self):
    query = Query([FieldFilter('foo', Operator.EQUALS, 'A')], OrderBy(field_name='foo', ascending=True), 2, None)
    results = PARENT_DAO.query(query)
    self.assertEquals([], results.items)
    self.assertEquals(None, results.pagination_token)

    parent1 = ParentModel(key=ndb.Key(ParentModel, '321'), foo="a")
    PARENT_DAO.insert(parent1)
    results = PARENT_DAO.query(query)
    self.assertEquals([parent1], results.items)
    self.assertEquals(None, results.pagination_token)

    parent2 = ParentModel(key=ndb.Key(ParentModel, '123'), foo="bob")
    PARENT_DAO.insert(parent2)
    parent3 = ParentModel(key=ndb.Key(ParentModel, '456'), foo="AARDVARK")
    PARENT_DAO.insert(parent3)
    results = PARENT_DAO.query(query)
    self.assertEquals([parent1], results.items)
    self.assertEquals(None, results.pagination_token)

    query2 = Query([FieldFilter('foo', Operator.LESS_THAN, "bob")], OrderBy(field_name='foo', ascending=True), 2, None)
    results2 = PARENT_DAO.query(query2)
    self.assertEquals([parent1, parent3], results2.items)
    self.assertEquals(None, results2.pagination_token)

    query3 = Query([FieldFilter('foo', Operator.LESS_THAN_OR_EQUALS, "bob")], OrderBy(field_name='foo', ascending=False), 2, None)
    results3 = PARENT_DAO.query(query3)
    self.assertEquals([parent2, parent3], results3.items)
    self.assertTrue(results3.pagination_token)

    query4 = Query([FieldFilter('foo', Operator.LESS_THAN_OR_EQUALS, "bob")], OrderBy(field_name='foo', ascending=False), 2, results3.pagination_token)
    results4 = PARENT_DAO.query(query4)
    self.assertEquals([parent1], results4.items)
    self.assertEquals(None, results4.pagination_token)

    query5 = Query([FieldFilter('foo', Operator.GREATER_THAN, "a")], OrderBy(field_name='foo', ascending=True), 2, None)
    results5 = PARENT_DAO.query(query5)
    self.assertEquals([parent3, parent2], results5.items)
    self.assertEquals(None, results5.pagination_token)

    query6 = Query([FieldFilter('foo', Operator.GREATER_THAN_OR_EQUALS, "a")], OrderBy(field_name='foo', ascending=True), 2, None)
    results6 = PARENT_DAO.query(query6)
    self.assertEquals([parent1, parent3], results6.items)
    self.assertTrue(results6.pagination_token)

    query7 = Query([FieldFilter('foo', Operator.GREATER_THAN_OR_EQUALS, "a")], OrderBy(field_name='foo', ascending=True), 2, results6.pagination_token)
    results7 = PARENT_DAO.query(query7)
    self.assertEquals([parent2], results7.items)
    self.assertEquals(None, results7.pagination_token)

    query8 = Query([FieldFilter('last_modified', Operator.EQUALS, results7.items[0].last_modified)], OrderBy(field_name='foo', ascending=True), 2, None)
    results8 = PARENT_DAO.query(query8)
    self.assertEquals([parent2], results8.items)
    self.assertEquals(None, results8.pagination_token)

    query9 = Query([FieldFilter('last_modified', Operator.EQUALS, parent2.last_modified),
                    FieldFilter('foo', Operator.EQUALS, parent2.foo)], OrderBy(field_name='foo', ascending=True), 2, None)
    results9 = PARENT_DAO.query(query9)
    self.assertEquals([parent2], results9.items)
    self.assertEquals(None, results9.pagination_token)

    query10 = Query([FieldFilter('last_modified', Operator.EQUALS, parent2.last_modified),
                    FieldFilter('foo', Operator.EQUALS, parent1.foo)], OrderBy(field_name='foo', ascending=True), 2, None)
    results10 = PARENT_DAO.query(query10)
    self.assertEquals([], results10.items)
    self.assertEquals(None, results10.pagination_token)

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
    new_history = PARENT_DAO.get_all_history(key, dates[0])
    self.assertEquals(actual_history, new_history)
    empty_history = PARENT_DAO.get_all_history(key, datetime(2016, 9, 30))
    self.assertEquals(0, len(empty_history))

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

  def test_update_computed_properties(self):
    parent_id = 'parentID1'
    parent = ParentModel(key=ndb.Key(ParentModel, parent_id))
    parent.foo = "Foo"
    PARENT_DAO.store(parent)
    parent = PARENT_DAO.load(parent_id)
    self.assertTrue(parent.last_modified)
    modified = parent.last_modified
    PARENT_DAO.update_computed_properties(parent.key)
    new_parent = PARENT_DAO.load(parent_id)
    self.assertTrue(new_parent.last_modified)
    # last_modified should have changed.
    self.assertNotEquals(parent.last_modified, new_parent.last_modified)

if __name__ == '__main__':
  unittest.main()
