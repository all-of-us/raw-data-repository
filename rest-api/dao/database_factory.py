import os
import singletons
from MySQLdb.cursors import SSCursor

from model.database import Database
from singletons import SQL_DATABASE_INDEX

DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')

class _SqlDatabase(Database):
  def __init__(self, **kwargs):
    super(_SqlDatabase, self).__init__(get_db_connection_string(), **kwargs)

def get_database():
  """Returns a singleton _SqlDatabase."""
  return singletons.get(SQL_DATABASE_INDEX, _SqlDatabase)

def get_db_connection_string():
  if DB_CONNECTION_STRING:
    return DB_CONNECTION_STRING

  # Only import "config" on demand, as it depends on Datastore packages (and
  # GAE). When running via CLI or tests, we'll have this from the environment
  # instead (above).
  import config
  return config.get_db_config()['db_connection_string']

def make_server_cursor_database():
  """
  Returns a database object that uses a server-side cursor when talking to the database.
  Useful in cases where you're reading a very large amount of data.
  """
  if get_db_connection_string().startswith('sqlite'):
    # SQLite doesn't have cursors; use the normal database during tests.
    return get_database()
  else:
    return _SqlDatabase(connect_args={'cursorclass': SSCursor})
