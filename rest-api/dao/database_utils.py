"""Helpers for querying the SQL database."""
import pytz
import re

from dao.database_factory import get_database
from datetime import datetime

_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
# MySQL uses %i for minutes
_MYSQL_DATE_FORMAT = '%Y-%m-%dT%H:%i:%SZ'
_ISODATE_PATTERN = 'ISODATE\[([^\]]+)\]'
_YEARS_OLD_PATTERN = 'YEARS_OLD\[([^\]]+)\]'


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
  return get_database().db_type == 'sqlite'


def parse_datetime(datetime_str):
  return datetime.strptime(datetime_str, _DATE_FORMAT)

def format_datetime(dt):
  """ISO formats a datetime. Converts naive datetimes to UTC first."""
  aware_dt = dt if dt.tzinfo is None else pytz.utc.localize(dt)
  return aware_dt.strftime(_DATE_FORMAT)

def replace_years_old(sql):
  if _is_sqlite():
    return re.sub(_YEARS_OLD_PATTERN,
                  r"CAST(((JULIANDAY('now') - JULIANDAY(\1)) / 365) AS int)",
                  sql)
  return re.sub(_YEARS_OLD_PATTERN,
                r"FLOOR(DATEDIFF(NOW(), \1) / 365)",
                sql)

def replace_isodate(sql):
  if _is_sqlite():
    return re.sub(_ISODATE_PATTERN, r"strftime('{}', \1)".format(_DATE_FORMAT), sql)
  return re.sub(_ISODATE_PATTERN, r"DATE_FORMAT(\1, '{}')".format(_MYSQL_DATE_FORMAT),
                sql)
