import config
import singletons

from model.database import Database

DB_CONNECTION_STRING = None

class SqlDatabase(Database):
  def __init__(self):
    super(SqlDatabase, self).__init__(DB_CONNECTION_STRING or 
                                      config.get_db_config()['db_connection_string'])

def get_database():
  """Returns a singleton SqlDatabase."""
  return singletons.get(SqlDatabase)
  
  
    
    