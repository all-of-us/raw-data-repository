"""Tests for data_access_object."""

import collections
import unittest
import db_fake

from data_access_object import DataAccessObject
from protorpc import message_types
from protorpc import messages

# Set up a hierarchy of objects.
L3_KEY_COLUMNS = ('id', 'parent_id', 'l1_id')
L3_COLUMNS = L3_KEY_COLUMNS +  ('ordinal',)
class L3Resource(messages.Message):
  id = messages.StringField(1)
  parent_id = messages.StringField(2)
  l1_id = messages.StringField(3)
  ordinal = messages.IntegerField(4)

class L3DAO(DataAccessObject):
  def __init__(self):
    super(L3DAO, self).__init__(resource=L3Resource,
                                table='l3',
                                columns=L3_COLUMNS,
                                key_columns=L3_KEY_COLUMNS)

  def link(self, obj, parent, ordinal):
    obj.parent_id = parent.id
    obj.l1_id = parent.parent_id
    obj.ordinal = ordinal


L2_KEY_COLUMNS = ('id', 'parent_id')
L2_COLUMNS = L2_KEY_COLUMNS + ('ordinal',)


class L2Resource(messages.Message):
  id = messages.StringField(1)
  parent_id = messages.StringField(2)
  children = messages.MessageField(L3Resource, 3, repeated=True)
  ordinal = messages.IntegerField(4)

class L2DAO(DataAccessObject):
  def __init__(self):
    super(L2DAO, self).__init__(resource=L2Resource,
                                table='l2',
                                columns=L2_COLUMNS,
                                key_columns=L2_KEY_COLUMNS)

  def link(self, obj, parent, ordinal):
    obj.parent_id = parent.id
    obj.ordinal = ordinal


class L1Resource(messages.Message):
  id = messages.StringField(1)
  children = messages.MessageField(L2Resource, 2, repeated=True)

class L1DAO(DataAccessObject):
  def __init__(self):
    super(L1DAO, self).__init__(
        resource=L1Resource, table='l1', columns=['id'], key_columns=['id'])

  def assemble(self, obj):
    l2_objects = CHILD_DAO.list(L2Resource(parent_id=obj.id))
    l3_objects = GRANDCHILD_DAO.list(L3Resource(l1_id=obj.id))

    l1_to_l2 = collections.defaultdict(list)
    l2_to_l3 = collections.defaultdict(list)

    for l2 in l2_objects:
      l1_to_l2[l2.parent_id].append(l2)

    for l3 in l3_objects:
      l2_to_l3[l3.parent_id].append(l3)

    obj.children = sorted(l1_to_l2[obj.id], key=lambda l2: l2.ordinal)

    for l2 in l2_objects:
      l2.children = sorted(l2_to_l3[l2.id], key=lambda l3: l3.ordinal)


GRANDCHILD_DAO = L3DAO()

CHILD_DAO = L2DAO()
CHILD_DAO.add_child_message('children', GRANDCHILD_DAO)

PARENT_DAO = L1DAO()
PARENT_DAO.add_child_message('children', CHILD_DAO)


class L1ChildAsJsonDAO(DataAccessObject):
  def __init__(self):
    super(L1ChildAsJsonDAO, self).__init__(
        resource=L1Resource,
        table='l1',
        columns=['id', 'children'], key_columns=['id'])


class TestDataAccessObjects(unittest.TestCase):

  def test_simple(self):
    fake_db = db_fake.db_fake()
    fake_db.add_expectation(
        'INSERT INTO l3 (id,parent_id,l1_id,ordinal) VALUES (%s,%s,%s,%d)',
        ('c', 'b', 'a', 1))
    fake_db.add_expectation(
        'SELECT id,parent_id,l1_id,ordinal from'
        + ' l3 where id=%s and parent_id=%s and l1_id=%s',
        ('c', 'b', 'a'),
        (('c', 'b', 'a', 1),))

    obj = L3Resource(id='c', parent_id='b', l1_id='a', ordinal=1)
    result = GRANDCHILD_DAO.insert(obj)
    self.assertEqual(result, obj)

  def test_hierarchy(self):
    gc1a = L3Resource(id='gc1a')
    gc1b = L3Resource(id='gc1b')

    gc2a = L3Resource(id='gc2a')

    c_1 = L2Resource(id='c1')
    c_2 = L2Resource(id='c2')

    p_1 = L1Resource(id='p1')

    p_1.children = [c_1, c_2]
    c_1.children = [gc1a, gc1b]
    c_2.children = [gc2a]

    fake_db = db_fake.db_fake()
    # First the inserts.
    fake_db.add_expectation('INSERT INTO l1 (id) VALUES (%s)', ('p1',))
    fake_db.add_expectation('SELECT id from l1 where id=%s', ('p1',), [['p1']])

    fake_db.add_expectation(
        'INSERT INTO l2 (id,parent_id,ordinal) VALUES (%s,%s,%d)',
        ('c1', 'p1', 0))
    fake_db.add_expectation(
        'INSERT INTO l2 (id,parent_id,ordinal) VALUES (%s,%s,%d)',
        ('c2', 'p1', 1))

    fake_db.add_expectation(
        'INSERT INTO l3 (id,parent_id,l1_id,ordinal) VALUES (%s,%s,%s,%d)',
        ('gc1a', 'c1', 'p1', 0))
    fake_db.add_expectation(
        'INSERT INTO l3 (id,parent_id,l1_id,ordinal) VALUES (%s,%s,%s,%d)',
        ('gc1b', 'c1', 'p1', 1))
    fake_db.add_expectation(
        'INSERT INTO l3 (id,parent_id,l1_id,ordinal) VALUES (%s,%s,%s,%d)',
        ('gc2a', 'c2', 'p1', 0))

    # Then the selects.
    fake_db.add_expectation(
        'SELECT id,parent_id,ordinal from l2 where parent_id=%s',
        ('p1',),
        (('c1', 'p1', 0),
         ('c2', 'p1', 1)))
    fake_db.add_expectation(
        'SELECT id,parent_id,l1_id,ordinal from l3 where l1_id=%s',
        ('p1',),
        (('gc1a', 'c1', 'p1', 0),
         ('gc1b', 'c1', 'p1', 1),
         ('gc2a', 'c2', 'p1', 0)))

    result = PARENT_DAO.insert(p_1)
    self.assertEqual(result, p_1)

  def test_child_as_json(self):
    gc1a = L3Resource(id='gc1a')
    gc1b = L3Resource(id='gc1b')

    gc2a = L3Resource(id='gc2a')

    c_1 = L2Resource(id='c1')
    c_2 = L2Resource(id='c2')

    p_1 = L1Resource(id='p1')

    p_1.children = [c_1, c_2]
    c_1.children = [gc1a, gc1b]
    c_2.children = [gc2a]

    fake_db = db_fake.db_fake()
    # First the inserts.
    fake_db.add_expectation(
        'INSERT INTO l1 (id,children) VALUES (%s,%s)',
        ('p1',
         '[{"id": "c1", "children": [{"id": "gc1a"}, {"id": "gc1b"}]},'
         + ' {"id": "c2", "children": [{"id": "gc2a"}]}]'))
    fake_db.add_expectation(
        'SELECT id,children from l1 where id=%s',
        ('p1',),
        (('p1',
          '[{"id": "c1", "children": [{"id": "gc1a"}, {"id": "gc1b"}]},'
          + ' {"id": "c2", "children": [{"id": "gc2a"}]}]'),))

    dao = L1ChildAsJsonDAO()
    result = dao.insert(p_1)
    self.assertEqual(result, p_1)


if __name__ == '__main__':
  unittest.main()
