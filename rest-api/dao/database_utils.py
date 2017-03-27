"""Helpers for querying the SQL database."""
import datetime
import logging
import re

from dao.database_factory import get_database
from model.base import Base
from model.hpo import HPO
from model.code import Code, CodeHistory, CodeBook

_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
# MySQL uses %i for minutes
_MYSQL_DATE_FORMAT = '%Y-%m-%dT%H:%i:%SZ'
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


def parse_datetime(datetime_str):
  return datetime.datetime.strptime(datetime_str, _DATE_FORMAT)


def replace_isodate(sql):
  if _is_sqlite():
    return re.sub(_ISODATE_PATTERN, r"strftime('{}', \1)".format(_DATE_FORMAT), sql)
  else:
    return re.sub(_ISODATE_PATTERN, r"DATE_FORMAT(\1, '{}')".format(_MYSQL_DATE_FORMAT),
                  sql)


_NON_USER_GEN_TABLES = frozenset(
    [t.__tablename__ for t in (HPO, Code, CodeHistory, CodeBook)]
    + ['alembic_version'])


def reset_for_tests():
  """Resets all user data. For test use only. Skips HPOs and codes."""
  logging.info('Deleting all user data! This should never be called in prod.')
  db = get_database()
  session = db.make_session()
  try:
    for table in reversed(Base.metadata.sorted_tables):
      if table.name not in _NON_USER_GEN_TABLES:
        session.execute(table.delete())  # DELETE FROM, does not drop table
    session.commit()
  except Exception, e:
    logging.error('Failed clearing db, aborting.', exc_info=True)
    session.rollback()
  finally:
    session.close()
