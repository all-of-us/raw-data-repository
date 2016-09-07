""" A Data Access Object.

Contains the logic for inserting, updating, listing object in
a SQL database.
"""

import db

from protorpc import message_types
from protorpc import messages


# Types that should be represented in a query format with '%s'
STRING_TYPES = (
    messages.StringField,
    message_types.DateTimeField,
)

# Types that should be represented in a query format with '%d'
NUMERIC_TYPES = (
    messages.EnumField,
    messages.IntegerField,
)


class DbException(BaseException):
  """Exception thrown when a DB error is encounterd"""
  def __init__(self, msg):
    BaseException.__init__(self)
    self.msg = msg

  def __str__(self):
    return 'DbException: {}'.format(self.msg)


class MissingKeyException(BaseException):
  """Exception thrown when a Get request doesn't conain all required keys."""
  def __init__(self, msg):
    BaseException.__init__(self)
    self.msg = msg

  def __str__(self):
    return 'MissingKeyException: {}'.format(self.msg)



class DataAccessObject(object):
  """A DataAccessObject handles the mapping of object to the datbase.

  Args:
    resource: The resource object. (The object containing the data).
  """
  def __init__(self, resource, table, columns, key_columns):
    self.resource = resource
    self.table = table
    self.columns = columns
    self.primary_key = _PrimaryKey(resource, key_columns)

  def insert(self, obj):
    """Inserts this object into the database"""
    return self._insert_or_update(obj, update=False)

  def update(self, obj):
    return self._insert_or_update(obj, update=True)

  def get(self, obj):
    where_clause, keys = self.primary_key.where_clause(obj)
    if len(keys) != len(self.primary_key.columns()):
      raise MissingKeyException('Get {} requires of {} to be specified'.format(
          self.table, self.primary_key.columns()))

    results = self._query(where_clause, keys)
    if not results or len(results) != 1:
      raise DbException("Get returned {} results. {} {}".format(
          len(results), where_clause, keys))
    return results[0]

  def list(self, obj):
    where_clause, keys = self.primary_key.where_clause(obj)
    return self._query(where_clause, keys)

  @db.connection
  def _insert_or_update(self, connection, obj, update=False):
    placeholders = []
    vals = []
    cols = []
    for col in self.columns:
      field_type = type(getattr(self.resource, col))
      val = getattr(obj, col)
      # Only use values that are present, and don't update the primary keys.
      if val and not (update and col in self.primary_key.columns()):
        cols.append(col)
        placeholders.append(_placeholder_for_type(field_type))
        vals.append(_convert_field(field_type, val))

    where_clause, keys = self.primary_key.where_clause(obj)
    if update:
      vals += keys
      assignments = ','.join(
          '{}={}'.format(k, v) for k, v in zip(cols, placeholders))
      query = 'UPDATE {} SET {} {}'.format(self.table, assignments,
                                           where_clause)
    else:
      query = 'INSERT INTO {} ({}) VALUES ({})'.format(
          self.table, ','.join(cols), ','.join(placeholders))

    connection.cursor().execute(query, vals)
    connection.commit()
    return self.get(obj)

  @db.connection
  def _query(self, connection, where='', where_vals=None):
    """Loads object that match the given where clause and values.

    If no where clause is specified, all objects are loaded.
    """
    where_vals = where_vals or []
    query = 'SELECT {} from {} {}'.format(','.join(self.columns), self.table,
                                          where)
    cursor = connection.cursor()
    cursor.execute(query, where_vals)
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

  def where_clause(self, obj):
    """Builds a where clause for this resource.

    Args:
      obj: If specified, the generated where clause will only contain
          fields present in this object.

    Returns:
     A tuple containing a where clause with placeholders and the values
      for those placeholders.
    """
    placeholders = []
    cols = []
    keys = []
    for col in self.key_columns:
      val = getattr(obj, col, None)
      if val:
        field_type = type(getattr(self.resource, col))
        placeholders.append(_placeholder_for_type(field_type))
        cols.append(col)
        keys.append(_convert_field(field_type, val))

    if len(cols):
      clause = 'where ' + ' and '.join(
          '{}={}'.format(k, v) for k, v in zip(cols, placeholders))
    else:
      clause = ''

    return (clause, keys)

  def columns(self):
    return self.key_columns


def _placeholder_for_type(field_type):
  """Returns: The proper placeholder for the given field."""

  if field_type in STRING_TYPES:
    return '%s'
  elif field_type in NUMERIC_TYPES:
    return '%s'
  else:
    raise DbException(
        'Can\'t handle type: {}'.format(field_type))

def _convert_field(field_type, val):
  """Converts a field value to db representation.

  For most objects it just returns the object. For Enums, for example, it
  converts to an int.
  """
  if field_type == messages.EnumField:
    return int(val)

  return val
