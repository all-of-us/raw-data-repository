import config
import re
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

DATE_FORMAT="%Y-%m-%dT%H:%M:%SZ"
ISODATE_PATTERN='ISODATE\[([^\]]+)\]'
IS_SQLITE = None

def is_sqlite():
  global IS_SQLITE
  if IS_SQLITE is None:
    IS_SQLITE = get_database().db_type == 'sqlite'
  return IS_SQLITE
  
def format_date(expression, date_format=DATE_FORMAT):
  if is_sqlite():
    return "strftime('{}', {})".format(date_format, expression)
  else:
    return "DATE_FORMAT({}, '{}')".format(expression, date_format)


def replace_isodate(sql, date_format=DATE_FORMAT):
  if is_sqlite():
    return re.sub(ISODATE_PATTERN, r"strftime('{}', \1)".format(date_format), sql)
  else:
    return re.sub(ISODATE_PATTERN, r"DATE_FORMAT(\1, '{}')".format(date_format), sql)