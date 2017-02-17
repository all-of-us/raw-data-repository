import config
import singletons

from model.database import Database

DB_CONNECTION_STRING = None

class _SqlDatabase(Database):
  def __init__(self):
    super(_SqlDatabase, self).__init__(DB_CONNECTION_STRING or 
                                       config.get_db_config()['db_connection_string'])

def get_database():
  """Returns a singleton _SqlDatabase."""
  return singletons.get(_SqlDatabase)
  
  
    
    