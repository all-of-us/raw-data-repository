import config
import os
import singletons

from model.database import Database
from singletons import SQL_DATABASE_INDEX

DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')

class _SqlDatabase(Database):
  def __init__(self):
    super(_SqlDatabase, self).__init__(DB_CONNECTION_STRING or
                                       config.get_db_config()['db_connection_string'])

def get_database():
  """Returns a singleton _SqlDatabase."""
  return singletons.get(SQL_DATABASE_INDEX, _SqlDatabase)

