""" A Data Access Object.

Contains the logic for inserting, updating, listing object in
a SQL database.
"""

import db
import config
import MySQLdb
import sys
import threading

from protorpc import message_types
from protorpc import messages


# Types that should be represented in a query format with '%s'
STRING_TYPES = [
    messages.StringField,
    message_types.DateTimeField,
]

# Types that should be represented in a query format with '%d'
NUMERIC_TYPES = [
    messages.EnumField,
    messages.IntegerField,
]


class DbConfigurationException(BaseException):
  """Exception thrown when the DB isn't configured correctly"""
  def __init__(self, msg):
    BaseException.__init__(self)
    self.msg = msg

  def __str__(self):
    return 'DbConfigurationException: {}'.format(self.msg)


class MissingKeyException(BaseException):
  """Exception thrown when a Get request doesn't conain all required keys."""
  def __init__(self, msg):
    BaseException.__init__(self)
    self.msg = msg

  def __str__(self):
    return 'MissingKeyException: {}'.format(self.msg)



class DataAccessObject(object):

  def __init__(self, resource, collection, table, columns, key_columns):
    self.resource = resource
    self.table = table
    self.collection = collection
    self.columns = columns
    self.primary_key = _PrimaryKey(resource, key_columns)

  def Insert(self, obj):
    return self._InsertOrUpdate(obj, update=False)

  def Update(self, obj):
    return self._InsertOrUpdate(obj, update=True)

  def Get(self, obj):
    where_clause = self.primary_key.WhereClause()
    ids = self.primary_key.Keys(obj)
    if len(ids) != len(self.primary_key.Columns()):
      raise MissingKeyException(
          'Get {} requires of {} to be specified'.format(self.table,
                                                         self.key_columns))
    return self._Query(where_clause, ids)[0]

  def List(self, obj):
    where_clause = self.primary_key.WhereClause(obj)
    ids = self.primary_key.Keys(obj)
    return self.collection(items=self._Query(where_clause, ids))

  @db.connection
  def _InsertOrUpdate(self, connection, obj, update=False):
    placeholders = []
    vals = []
    cols = []
    for col in self.columns:
      field_type = type(getattr(self.resource, col))
      val = getattr(obj, col)
      # Only use values that are present, and don't update the primary keys.
      if val and not (update and col in self.primary_key.Columns()):
        cols.append(col)
        placeholders.append(_PlaceholderForType(field_type))
        vals.append(_ConvertField(field_type, val))

    keys = self.primary_key.Keys(obj)
    if update:
      vals += keys
      assignments = ','.join(
          '{}={}'.format(k,v) for k, v in zip(cols, placeholders))
      q = 'UPDATE {} SET {} {}'.format(self.table, assignments,
                                       self.primary_key.WhereClause())
    else:
      q = 'INSERT INTO {} ({}) VALUES ({})'.format(
          self.table, ','.join(cols), ','.join(placeholders))

    print q
    print vals
    connection.cursor().execute(q, vals)
    connection.commit()
    return self.Get(obj)

  @db.connection
  def _Query(self, connection, where='', where_vals=[]):
    q = 'SELECT {} from {} {}'.format(','.join(self.columns), self.table, where)
    print q
    print where_vals

    cursor = connection.cursor()
    cursor.execute(q, where_vals)
    results = cursor.fetchall()
    objs = []
    for result in results:
      obj = self.resource()
      for i, col in enumerate(self.columns):
        field = getattr(obj, col)
        field_type = type(getattr(self.resource, col))
        if field_type == messages.EnumField:
          setattr(obj, col, type(field)(result[i]))
        else:
          setattr(obj, col, result[i])
      objs.append(obj)
    return objs


class _PrimaryKey(object):
  """A potentially multi-column primary key."""
  def __init__(self, resource, keys):
    self.key_columns = keys
    self.resource = resource

  def WhereClause(self, obj=None):
    """Builds a where clause for this resource.

    Args:
      obj: If specified, the genereated where clause will only contain
          fields present in this object.

    Returns:
      A where clause for selecting this object using its primary key(s).
    """
    placeholders = []
    cols = []
    for col in self.key_columns:
      if obj == None or (hasattr(obj, col) and getattr(obj, col) != None):
        field_type = type(getattr(self.resource, col))
        placeholders.append(_PlaceholderForType(field_type))
        cols.append(col)

    if len(cols):
      return 'where ' + ' and '.join(
          '{}={}'.format(k,v) for k, v in zip(cols, placeholders))
    else:
      return ''

  def Keys(self, obj):
    """Returns: the primary keys as an array."""
    keys = []
    for col in self.key_columns:
      if hasattr(obj, col):
        val = getattr(obj, col)
        if val != None:
          field_type = type(getattr(self.resource, col))
          keys.append(_ConvertField(field_type, val))
    return keys

  def Columns(self):
    return self.key_columns


def _PlaceholderForType(field_type):
  """Returns: The proper placeholder for the given field."""

  if field_type in STRING_TYPES:
    return '%s'
  elif field_type in NUMERIC_TYPES:
    return '%s'
  else:
    raise DbConfigurationException(
        'Can\'t handle type: {}'.format(field_type))

def _ConvertField(field_type, val):
  """Converts a field value to db representation.

  For most objects it just returns the object. For Enums, for example, it
  converts to an int.
  """
  if field_type == messages.EnumField:
    return int(val)

  return val
