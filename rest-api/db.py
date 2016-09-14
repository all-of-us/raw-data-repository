""" Abstraction level over the database.

Provides a decorator for access to a threadsafe connection.
"""

import config
import logging
import MySQLdb
import threading

from protorpc import message_types
from protorpc import messages


def get_connection():
  """This at the module level makes it easier to mock out for testing."""
  return _Connection.get_connection()

class _Connection(object):
  """A wrapper around the MySQLdb connection.

  Used to ensure that seperate threads don't use the same connection object.
  Client code shouldn't use this class directly, instead it should call use the
  connection decorator.
  """
  # Access to the pool of connections is guarded with this lock.
  _db_lock = threading.Lock()

  _connections = []


  def __init__(self):
    self.conn = MySQLdb.connect(
      unix_socket=config.CLOUDSQL_SOCKET,
      user=config.CLOUDSQL_USER,
      passwd='ApiPants123',
      db='pmi_rdr')
    self.in_use = True

  def release(self):
    # Clear out any transaction that may be still on this connection.
    self.conn.rollback()
    self.in_use = False

  def reserve(self):
    if self.in_use:
      return False

    self.in_use = True
    return True

  @classmethod
  def _get_connection(cls):
    """Get a connection.

    Locks the db_lock, and then tries to find a connection that is not in use.
    If one is found, it is reserved and returned.  If no unused connection is
    found, a new one is created, reserved and returned.

    Returns:
    A reserved connection wrapped in a _Connection object.

    """
    try:
      cls._db_lock.acquire()
      for conn in cls._connections:
        if conn.reserve():
          return conn

      new_conn = _Connection()
      cls._connections.append(new_conn)
      logging.info('Creating database connection. There are now {}.'.format(
          len(cls._connections)))
      return new_conn
    finally:
      cls._db_lock.release()

def connection(func):
  """A decorator for reserving a connection.

  This decorator will reserve a DB connection for the duration of the method.
  The connection will be passed as the first argument of the method.  The
  wrapped function should call commit() if any changes are to be committed. The
  decorator will call rollback() on every connection before releasing it.
  """
  def wrapped(self, *args, **kwargs):
    conn = get_connection()
    try:
      return func(self, conn.conn, *args, **kwargs)
    finally:
      conn.release()

  return wrapped
