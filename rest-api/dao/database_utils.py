"""Helpers for querying the SQL database."""
import re

from dao.database_factory import get_database
from datetime import datetime
from sqlalchemy.orm.base import SQL_OK

_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
_ISODATE_PATTERN = 'ISODATE\[([^\]]+)\]'
_IS_SQLITE = None


def get_sql_and_params_for_array(arr, name_prefix):
  """Returns an SQL expression and associated params dict for an array of values.

  SQLAlchemy can't format array parameters. Work around it by building the :param style expression
  and creating a dictionary of individual params for that.
  """
  array_values = {}
  for i, v in enumerate(arr):
    array_values['%s%d' % (name_prefix, i)] = v
  sql_expr = '(%s)' % ','.join([':' + param_name for param_name in array_values])
  return sql_expr, array_values


def _is_sqlite():
  global _IS_SQLITE
  if _IS_SQLITE is None:
    _IS_SQLITE = get_database().db_type == 'sqlite'
  return _IS_SQLITE


# MySQL uses percent-escaping
def escape(sql):
  if _is_sqlite():
    return sql
  return sql.replace('%', '%%')

def parse_datetime(datetime_str):
  return datetime.strptime(datetime_str, _DATE_FORMAT)

def replace_isodate(sql, date_format=_DATE_FORMAT):
  if _is_sqlite():
    return re.sub(_ISODATE_PATTERN, r"strftime('{}', \1)".format(date_format), sql)
  else:
    return re.sub(_ISODATE_PATTERN, r"DATE_FORMAT(\1, '{}')".format(date_format), 
                  sql)
