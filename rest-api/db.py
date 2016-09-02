""" Abstraction level over the database.
"""

import config
import MySQLdb
import sys
import threading

from protorpc import message_types
from protorpc import messages

class DbConfigurationException(BaseException):
  """Exception thrown when the DB isn't configured correctly"""
  def __init__(self, msg):
    BaseException.__init__(self)
    self.msg = msg

  def __str__(self):
    return 'DbConfigurationException: {}'.format(self.msg)


resource_to_table = {}
table_to_resource = {}
resource_to_columns = {}

# Access to the pool of connections is guarded with this lock.
_db_lock = threading.Lock()

_connections = []

def GetConn():
  try:
    _db_lock.acquire()
    for conn in _connections:
      if conn._reserve():
        return conn

    new_conn = DB()
    _connections.append(new_conn)
    return new_conn
  finally:
    _db_lock.release()

class DB(object):

  def __init__(self):
    self.conn = MySQLdb.connect(
      unix_socket=config.CLOUDSQL_SOCKET,
      user=config.CLOUDSQL_USER,
      passwd='ApiPants123',
      db ='pmi_rdr')
    self.in_use = True

  def Release(self):
    # Clear out any transaction that may be still on this connection.
    self.conn.rollback()
    self.in_use = False

  def _reserve(self):
    if self.in_use:
      return False

    self.in_use = True
    return True

  def Commit(self):
    self.conn.commit()

  def InsertObject(self, resource, obj, update=False):
    placeholders = []
    vals = []
    cols = []
    table = resource_to_table[resource]
    for col in resource_to_columns[resource]:
      field_type = type(getattr(resource, col))
      val = getattr(obj, col)
      # Only use values that are present, and don't update the id column.
      if val and not (update and col == 'id'):
        cols.append(col)
        if field_type == messages.StringField:
          placeholders.append('%s')
          vals.append(val)
        elif field_type == messages.EnumField:
          placeholders.append('%s')
          vals.append(int(val))
        elif field_type == message_types.DateTimeField:
          placeholders.append('%s')
          vals.append(val)
        else:
          raise DbConfigurationException(
              'Can\'t handle type: {}'.format(field_type))

    if update:
      vals.append(obj.id)
      assignments = ','.join(
          '{}={}'.format(k,v) for k, v in zip(cols, placeholders))
      q = 'UPDATE {} SET {} where id = %s'.format(table, assignments)
    else:
      q = 'INSERT INTO {} ({}) VALUES ({})'.format(
          table, ','.join(cols), ','.join(placeholders))

    self.conn.cursor().execute(q, vals)
    return self.GetObject(resource, obj.id)


  def GetObject(self, resource, id):
    return self.ListObjects(resource, 'where id = %s', [id])[0]

  def ListObjects(self, resource, where='', where_vals=[]):
    table = resource_to_table[resource]
    columns = resource_to_columns[resource]

    q = 'SELECT {} from {} {}'.format(','.join(columns), table, where)
    cursor = self.conn.cursor()
    cursor.execute(q, where_vals)
    results = cursor.fetchall()
    objs = []
    for result in results:
      obj = resource()
      for i, col in enumerate(columns):
        field = getattr(obj, col)
        field_type = type(getattr(resource, col))
        if field_type == messages.EnumField:
          setattr(obj, col, type(field)(result[i]))
        else:
          setattr(obj, col, result[i])
      objs.append(obj)
    return objs



def RegisterType(resource, table, columns):
  """Registers a resource object.

  Each resource object should call this function to register a mapping of the
  resource to the database tables that back it.
  """
  resource_to_table[resource] = table
  table_to_resource[table] = resource
  resource_to_columns[resource] = columns
