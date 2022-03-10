"""Helpers for querying the SQL database."""
import logging
from datetime import datetime
import re

import pytz
from sqlalchemy.orm import Session

from rdr_service.dao.database_factory import get_database

_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
# MySQL uses %i for minutes
MYSQL_ISO_DATE_FORMAT = "%Y-%m-%dT%H:%i:%SZ"
_ISODATE_PATTERN = "ISODATE\[([^\]]+)\]"
_YEARS_OLD_PATTERN = "YEARS_OLD\[([^\],]+), +([^\],]+)\]"
_NULL_SAFE_PATTERN = "<=>"


def get_sql_and_params_for_array(arr, name_prefix):
    """Returns an SQL expression and associated params dict for an array of values.

  SQLAlchemy can't format array parameters. Work around it by building the :param style expression
  and creating a dictionary of individual params for that.
  """
    array_values = {}
    for i, v in enumerate(arr):
        array_values["%s%d" % (name_prefix, i)] = v
    sql_expr = "(%s)" % ",".join([":" + param_name for param_name in array_values])
    return sql_expr, array_values


def _is_sqlite():
    return get_database().db_type == "sqlite"


def parse_datetime_from_iso_format(datetime_str):
    return datetime.strptime(datetime_str, _ISO_FORMAT)


def parse_datetime(datetime_str):
    return datetime.strptime(datetime_str, _DATE_FORMAT)


def format_datetime(dt):
    """ISO formats a datetime. Converts naive datetimes to UTC first."""
    aware_dt = dt if dt.tzinfo is None else pytz.utc.localize(dt)
    return aware_dt.strftime(_DATE_FORMAT)


def replace_years_old(sql):
    if _is_sqlite():
        return re.sub(_YEARS_OLD_PATTERN, r"CAST(((JULIANDAY(\1) - JULIANDAY(\2)) / 365) AS int)", sql)
    return re.sub(_YEARS_OLD_PATTERN, r"FLOOR(DATEDIFF(\1, \2) / 365)", sql)


def replace_isodate(sql):
    if _is_sqlite():
        return re.sub(_ISODATE_PATTERN, r"strftime('{}', \1)".format(_DATE_FORMAT), sql)
    return re.sub(_ISODATE_PATTERN, r"DATE_FORMAT(\1, '{}')".format(MYSQL_ISO_DATE_FORMAT), sql)


def replace_null_safe_equals(sql):
    if _is_sqlite():
        return re.sub(_NULL_SAFE_PATTERN, r"is", sql)
    return sql


class NamedLock:
    """
    Retrieve an explicit and mutually exclusive database lock to ensure a specific piece of code is the only instance
    of that code running at the time that the lock is held.
    """

    def __init__(self, name: str, session: Session, lock_timeout_seconds=30, lock_failure_exception=None):
        self._name = name
        self._session = session
        self._lock_timout_seconds = lock_timeout_seconds
        self.is_locked = False
        self._lock_failure_exception = lock_failure_exception

    def __enter__(self):
        self.obtain_lock()
        return self

    def __exit__(self, *_, **__):
        self.release_lock()

    def obtain_lock(self):
        """
        Execute the database command to obtain the lock.
        This will wait until either the lock is successfully obtained, or the timeout occurs.
        """
        lock_result = self._session.execute(f"SELECT GET_LOCK('{self._name}', {self._lock_timout_seconds})").scalar()

        if lock_result == 1:
            self.is_locked = True
        else:
            logging.error(f'Database error retrieving named lock for {self._name}, received result: "{lock_result}"')
            if self._lock_failure_exception is not None:
                raise self._lock_failure_exception
            else:
                raise Exception('Unable to obtain database lock.')

    def release_lock(self):
        if self.is_locked:
            release_result = self._session.execute(f"SELECT RELEASE_LOCK('{self._name}')").scalar()

            if release_result is None:
                logging.error(f'Database lock did not exist for {self._name}!')
            elif release_result == 0:
                logging.error(f'Database lock for {self._name} was not taken by this connection, did not release!')
            else:
                self.is_locked = False
