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

  """
  def __init__(self, resource, table, columns, key_columns, column_map=None):
    """Constructs a DataAccessObject.

    Note: If a field in the resource contains an optionally repeating child
    object, it can be persisted two ways.  By specifying the field in the list
    of columns, the child object(s) will be converted to JSON and stored in that
    column.  If the child object is to be stored in a seperate table, the field
    should not be in the list of columns.  Instead, a DAO should be created for
    the child object type, and add_child_message function should be called to
    register it.  In addition, a link() should be overridden on the child DAO,
    and assemble() should be overridden on the parent DAO.

    Args:
    resource: The resource object. (The object containing the data).
    table: The table that these objects are stored in.
    columns: The columns of the database table that map to fields that should
        be persisted. The names of the columns should match the columns.  If
        that is not possible, pass in a column map.
    key_columns: The columns that are part of the object's primary key.
    column_map: A dictionary containing a map of column names to field values.
        Entries in this map are only necessary if they differ.  If all fields
        match the column names, this can be omitted.
    """
    self.resource = resource
    self.table = table
    self.columns = columns
    self.key_columns = key_columns
    self.column_map = column_map or {}
    self.children = []
    self.synthetic_fields = []

  def add_child_message(self, field_name, dao):
    """Registers a field as containing child messages.

    Args:
      field_name: The name of the field in the resource.
      dao: An instance of the Data Access Object that is responsible for the
          child object.
    """
    self.children.append((field_name, dao))

  def set_synthetic_fields(self, fields):
    """Sets a list of fields that should be stripped.

    Not all model fields need to go out to the client.  Some are used for
    linking child objects with parent objects in separate tables.  These fields
    should be specified here.
    """
    self.synthetic_fields = fields


  def insert(self, obj, strip=False):
    """Inserts this object into the database

    Args:
      obj: The object to insert.
      strip: If set to true, the returned object will have all of it's synthetic
          fields removed.  Use this to remove intenal identifiers, etc.
    """
    return self._insert_or_update(obj, update=False, strip=strip)

  def update(self, obj, strip=False):
    return self._insert_or_update(obj, update=True, strip=strip)

  def get(self, request_obj, strip=False):
    """Retrieves an object.

    Loads an object based on the key fields set in request_obj.

    Args:
      request_obj: An instance of the resource object with the primary key
          fields filled in.
      strip: If set to true, the object will have all of it's synthetic fields
          removed.  Use this to remove intenal identifiers, etc.
    Returns: The retrieved object.
    """
    where_clause, keys = self._where_clause(request_obj, key=True)
    if len(keys) != len(self.key_columns):
      msg = ('Get from {} requires {} columns to be specified. '
             + 'Current where clause contains only "{}".')
      raise MissingKeyException(msg.format(
              self.table, self.key_columns, where_clause))

    results = self._query(where_clause, keys, strip=strip)
    if not results:
      raise NotFoundException("Object not found. {} {}".format(where_clause,
                                                               keys))
    if len(results) == 0:
      raise DbException("Get returned {} results. {} {}".format(
          len(results), where_clause, keys))
    return results[0]

  def list(self, request_obj, strip=False):
    """Retrieves a list of objects.

    Loads a list of objects based on the key fields set in request_obj.

    Args:
      request_obj: An instance of the resource object with some of the primary
          key fields filled in (or empty if all objects should be returned)
      strip: If set to True, the returned objects will have all of their
          synthetic fields removed.  Use this to remove intenal
          identifiers, etc.
    Returns: The retrieved object.
    """
    return self._query(*self._where_clause(request_obj), strip=strip)

  @db.connection
  def _insert_or_update(self, connection, obj, update=False, strip=False):
    self._insert_or_update_with_connection(connection, obj, update)
    connection.commit()
    return self.get(obj, strip)

  def _insert_or_update_with_connection(self, connection, obj, update):
    vals = []
    cols = []
    for col in self.columns:
      field_name = self._column_to_field(col)
      field = getattr(self.resource, field_name)
      val = getattr(obj, field_name)
      # Only use values that are present, and don't update the primary keys.
      if val is not None and not (update and col in self.key_columns):
        cols.append(col)
        vals.append(_marshall_field(field, val))

    where_clause, keys = self._where_clause(obj, key=True)
    if update:
      vals += keys
      assignments = ','.join('{}=%s'.format(c) for c in cols)
      query = 'UPDATE {} SET {} {}'.format(self.table, assignments,
                                           where_clause)
    else:
      query = 'INSERT INTO {} ({}) VALUES ({})'.format(
          self.table, ','.join(cols), ','.join(['%s']*len(cols)))

    connection.cursor().execute(query, vals)

    for field, dao in self.children:
      for i, child in enumerate(self._child_objects(obj, field)):
        dao.link(child, obj, ordinal=i)
        dao._insert_or_update_with_connection(connection, child, update)

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
    and assemble them.  It is also the responsibility of this object to ensure
    that all child objects appear in the correct order.

    Args:
     obj: The root object.
    """
    pass

  def strip(self, obj):
    """Clears out any data from synthetic fields.

    Will also traverse the object hierarchy.
    """
    for field, dao in self.children:
      for child in self._child_objects(obj, field):
        dao.strip(child)

    for field in self.synthetic_fields:
      setattr(obj, field, None)

  @db.connection
  def _query(self, connection, where='', where_vals=None, strip=False):
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
      if strip:
        self.strip(obj)
      objs.append(obj)
    return objs

  def _column_to_field(self, col):
    """Maps the column name to the resource field."""
    return self.column_map.get(col, col)

  def _child_objects(self, obj, field):
    children = []
    val = getattr(obj, field, None)
    if val is not None:
      children = val
      if not getattr(self.resource, field).repeated:
        children = [val]

    return children

  def _where_clause(self, obj, key=False):
    """Builds a where clause for this resource.

    Args:
      obj: If specified, the generated where clause will only contain
          fields present in this object.
      key: If True, only builds a where clause that specifies the primary key
          columns.

    Returns:
     A tuple containing a where clause with placeholders and the values
      for those placeholders.

    """
    cols = []
    keys = []
    obj_columns = self.columns
    if key:
      obj_columns = self.key_columns

    for col in obj_columns:
      val = getattr(obj, col, None)
      if val:
        field = getattr(self.resource, col)
        cols.append(col)
        keys.append(_marshall_field(field, val))

    if cols:
      clause = 'where ' + ' and '.join('{}=%s'.format(c) for c in cols)
    else:
      clause = ''

    return (clause, keys)

def _marshall_field(field, val):
  """Converts a field value to db representation.

  For most objects it just returns the object. For Enums, for example, it
  converts to an int.
  """
  field_type = type(field)
  if field_type == messages.EnumField or field_type == messages.BooleanField:
    return int(val)

  # If we are saving a message to a column, convert to JSON.
  # Json encoder doesn't do protos. Proto encoder doesn't do arrays.
  if field_type == messages.MessageField:
    if field.repeated:
      return json.dumps([json.loads(protojson.encode_message(m)) for m in val])
    else:
      return protojson.encode_message(val)

  if field_type == message_types.DateTimeField:
    return val.strftime('%Y-%m-%d %H:%M:%S')

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
