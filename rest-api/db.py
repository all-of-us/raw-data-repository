""" Abstraction level over the database.

Provides a decorator for access to a threadsafe connection.
"""

import config
import MySQLdb
import sys
import threading

from protorpc import message_types
from protorpc import messages


# Access to the pool of connections is guarded with this lock.
_db_lock = threading.Lock()

_connections = []

def GetConnection():
  """Get a connection.

  Locks the db_lock, and then tries to find a connection that is not in use.  If
  one is found, it is reserved and returned.  If no unused connection is found,
  a new one is created, reserved and returned.

  Returns:
    A reserved connection wrapped in a _Connection object.
  """
  try:
    _db_lock.acquire()
    for conn in _connections:
      if conn._reserve():
        return conn

    new_conn = _Connection()
    _connections.append(new_conn)
    return new_conn
  finally:
    _db_lock.release()

class _Connection(object):
  """A wrapper around the MySQLdb connection.

  Used to ensure that seperate threads don't use the same connection object.
  Client code shouldn't use this class directly, instead it should call use the
  connection decorator.
  """
  def __init__(self):
    self.conn = MySQLdb.connect(
      unix_socket=config.CLOUDSQL_SOCKET,
      user=config.CLOUDSQL_USER,
      passwd='ApiPants123',
      db ='pmi_rdr')
    self.in_use = True

  def _release(self):
    # Clear out any transaction that may be still on this connection.
    self.conn.rollback()
    self.in_use = False

  def _reserve(self):
    if self.in_use:
      return False

    self.in_use = True
    return True


def connection(func):
  """A decorator for reserving a connection.

  This decorator will reserve a DB connection for the duration of the method.
  The connection will be passed as the first argument of the method.  The
  wrapped function should call commit() if any changes are to be committed, the
  decorator will call rollback() on every connection before releasing it.
  """
  def wrapped(self, *args, **kwargs):
    connection = GetConnection()
    try:
      return func(self, connection.conn, *args, **kwargs)
    finally:
      connection._release()

  return wrapped
