""" A Data Access Object.

Contains the logic for inserting, updating, listing object in
a SQL database.
"""

import db
import json

from protorpc import message_types
from protorpc import messages
from protorpc import protojson

# Types that should be represented in a query format with '%s'
STRING_TYPES = (
    messages.StringField,
    message_types.DateTimeField,
)

# Types that should be represented in a query format with '%d'
NUMERIC_TYPES = (
    messages.BooleanField,
    messages.EnumField,
    messages.IntegerField,
)


class DbException(BaseException):
  """Exception thrown when a DB error is encounterd"""

class MissingKeyException(BaseException):
  """Exception thrown when a Get request doesn't conain all required keys."""

class NotFoundException(BaseException):
  """Exception thrown when a db object is not found."""

class DataAccessObject(object):
  """A DataAccessObject handles the mapping of object to the datbase.

  Args:
    resource: The resource object. (The object containing the data).
  """
  def __init__(self, resource, table, columns, key_columns, column_map=None):
    self.resource = resource
    self.table = table
    self.columns = columns
    self.primary_key = _PrimaryKey(resource, key_columns)
    self.column_map = column_map or {}
    self.children = []

  def add_child_message(self, field_name, dao):
    self.children.append((field_name, dao))

  def insert(self, obj):
    """Inserts this object into the database"""
    return self._insert_or_update(obj, update=False)

  def update(self, obj):
    return self._insert_or_update(obj, update=True)

  def get(self, request_obj):
    where_clause, keys = self.primary_key.where_clause(request_obj)
    if len(keys) != len(self.primary_key.columns()):
      raise MissingKeyException('Get {} requires {} to be specified'.format(
          self.table, self.primary_key.columns()))

    results = self._query(where_clause, keys)
    if not results:
      raise NotFoundException("Object not found. {} {}".format(where_clause,
                                                               keys))
    if len(results) == 0:
      raise DbException("Get returned {} results. {} {}".format(
          len(results), where_clause, keys))
    return results[0]

  def list(self, request_obj):
    results = self._query(*self.primary_key.where_clause(request_obj))
    return results



  @db.connection
  def _insert_or_update(self, connection, obj, update=False):
    self._insert_update_with_connection(connection, obj, update)
    connection.commit()
    return self.get(obj)

  def _insert_update_with_connection(self, connection, obj, update):
    placeholders = []
    vals = []
    cols = []
    for col in self.columns:
      field_name = self._column_to_field(col)
      field = getattr(self.resource, field_name)
      val = getattr(obj, field_name)
      # Only use values that are present, and don't update the primary keys.
      if val is not None and not (update and col in self.primary_key.columns()):
        cols.append(col)
        placeholders.append(_placeholder_for_type(type(field), col))
        vals.append(_marshall_field(field, val))

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

    for field, dao in self.children:
      val = getattr(obj, field, None)
      if val is not None:
        field_def = getattr(self.resource, field)
        if field_def.repeated:
          children = val
        else:
          children = [val]
        for i, child in enumerate(children):
          dao.link(child, obj, ordinal=i)
          dao._insert_update_with_connection(connection, child, update)

  def link(self, obj, parent, ordinal):
    """Override this to propagate information across an object hierarchy.

    The default implementation does nothing.

    This is called once for every child object in an object hierarchy before
    insert or update.  This can be used to set the parent_id field of a child
    object to match parent.id.

    This can also be used to set ordinal fields to ensure that child object
    order is preserved.

    Args:
      obj: The child object.
      parent: The parent of this child object.
      ordinal: The order of this child object within the parent.
    """
    pass

  def assemble(self, obj):
    """Override this on the DAO to assemble the object hierarchy.

    Override this only for the root object in the hierarchy.

    This function should query all the child objects for the entire hierarchy
    and assemble them.

    Args:
     obj: The root object.
    """
    pass

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
        field_name = self._column_to_field(col)
        field = getattr(self.resource, field_name)
        setattr(obj, field_name, _unmarshall_field(field, result[i]))

      self.assemble(obj)
      objs.append(obj)

    return objs

  def _column_to_field(self, col):
    """Maps the column name to the resource field."""
    return self.column_map.get(col, col)


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
        field = getattr(self.resource, col)
        placeholders.append(_placeholder_for_type(type(field), col))
        cols.append(col)
        keys.append(_marshall_field(field, val))

    if len(cols):
      clause = 'where ' + ' and '.join(
          '{}={}'.format(k, v) for k, v in zip(cols, placeholders))
    else:
      clause = ''

    return (clause, keys)

  def columns(self):
    return self.key_columns


def _placeholder_for_type(field_type, field_name):
  """Returns: The proper placeholder for the given field."""

  if field_type in STRING_TYPES:
    return '%s'
  elif field_type in NUMERIC_TYPES:
    return '%d'
  elif field_type == messages.MessageField:
    return '%s'
  else:
    raise DbException(
        'Can\'t handle type: {} {}'.format(field_type, field_name))

def _marshall_field(field, val):
  """Converts a field value to db representation.

  For most objects it just returns the object. For Enums, for example, it
  converts to an int.
  """
  field_type = type(field)
  if (field_type == messages.EnumField or
      field_type == messages.BooleanField):
    return int(val)

  # If we are saving a message to a column, convert to JSON.
  if field_type == messages.MessageField:
    if field.repeated:
      result_list = []
      for msg in val:
        result_list.append(json.loads(protojson.encode_message(msg)))
      return json.dumps(result_list)
    else:
      return protojson.encode_message(val)
  return val


def _unmarshall_field(field, result):
  if result == None:
    return None

  if type(field) == messages.EnumField:
    return field.type(result)
  elif type(field) == messages.MessageField:
    if field.repeated:
      # Json decoder doesn't do protos. Proto decoder doesn't do arrays. Use
      # json to parse the array, dumps() each element, then parse as proto.
      return [protojson.decode_message(field.message_type, json.dumps(sub_obj))
              for sub_obj in json.loads(result)]
    else:
      return protojson.decode_message(field.message_type, result)
  elif type(field) == messages.BooleanField:
    return bool(result)
  else:
    return result
