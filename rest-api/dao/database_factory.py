from functools import wraps
import os

from MySQLdb.cursors import SSCursor
from sqlalchemy.engine.url import make_url

from model.database import Database
import singletons


DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')
# Exposed for testing.
SCHEMA_TRANSLATE_MAP = None


class _SqlDatabase(Database):
  def __init__(self, db_name, **kwargs):
    url = make_url(get_db_connection_string())
    if url.drivername != "sqlite" and not url.database:
      url.database = db_name
    super(_SqlDatabase, self).__init__(url, **kwargs)


def autoretry(func):
  """Wraps a function so that the database will run it in a transaction and automatically retry on
  lost connection
  """
  @wraps(func)
  def wrapper(*args, **kwargs):
    db = get_database()
    return db.autoretry(func, *args, **kwargs)
  return wrapper


def get_database():
  """Returns a singleton _SqlDatabase which USEs the rdr DB."""
  return singletons.get(singletons.SQL_DATABASE_INDEX, _SqlDatabase, db_name='rdr')


def get_generic_database():
  """Returns a singleton generic _SqlDatabase (no database USE).

  This should be used to access any tables outside of the primary 'rdr' schema,
  e.g. metrics. This could also be used for cross-DB joins/inserts - if needed.
  For simple access to the primary 'rdr' schema (most models - all extending
  from Base), use get_database() instead.
  """
  return singletons.get(singletons.GENERIC_SQL_DATABASE_INDEX,
                        _SqlDatabase,
                        db_name=None,
                        execution_options={'schema_translate_map': SCHEMA_TRANSLATE_MAP})


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
    return _SqlDatabase('rdr', connect_args={'cursorclass': SSCursor})
