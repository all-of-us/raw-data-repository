import config
import os
import singletons
import MySQLdb

from model.database import Database
from singletons import SQL_DATABASE_INDEX

DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')

class _SqlDatabase(Database):
  def __init__(self, **kwargs):
    super(_SqlDatabase, self).__init__(DB_CONNECTION_STRING or
                                       config.get_db_config()['db_connection_string'], **kwargs)

def get_database():
  """Returns a singleton _SqlDatabase."""
  return singletons.get(SQL_DATABASE_INDEX, _SqlDatabase)

def make_server_cursor_database():
  """
  Returns a database object that uses a server-side cursor when talking to the database.
  Useful in cases where you're reading a very large amount of data.
  """ 
  return _SqlDatabase(connect_args={'cursorclass': MySQLdb.cursors.SSCursor})