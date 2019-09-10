import os

from MySQLdb.cursors import SSCursor
from sqlalchemy.engine.url import make_url

from rdr_service import singletons
from rdr_service.model.database import Database

# Exposed for testing.
SCHEMA_TRANSLATE_MAP = None


class _SqlDatabase(Database):
    def __init__(self, db_name, backup=False, instance_name=None, **kwargs):
        url = make_url(get_db_connection_string(backup, instance_name))
        if url.drivername != "sqlite" and not url.database:
            url.database = db_name
        super(_SqlDatabase, self).__init__(url, **kwargs)


class _BackupSqlDatabase(_SqlDatabase):
    def __init__(self, db_name, **kwargs):
        super(_BackupSqlDatabase, self).__init__(db_name, backup=True, **kwargs)


def get_database(db_name="rdr") -> Database:
    """Returns a singleton _SqlDatabase which USEs the rdr DB."""
    return singletons.get(singletons.SQL_DATABASE_INDEX, _SqlDatabase, db_name=db_name)


def get_backup_database() -> Database:
    """Returns a singleton _BackupSqlDatabase which USEs the rdr failover DB."""
    return singletons.get(singletons.BACKUP_SQL_DATABASE_INDEX, _BackupSqlDatabase, db_name="rdr")


def get_generic_database() -> Database:
    """Returns a singleton generic _SqlDatabase (no database USE).

  This should be used to access any tables outside of the primary 'rdr' schema,
  e.g. metrics. This could also be used for cross-DB joins/inserts - if needed.
  For simple access to the primary 'rdr' schema (most models - all extending
  from Base), use get_database() instead.
  """
    return singletons.get(
        singletons.GENERIC_SQL_DATABASE_INDEX,
        _SqlDatabase,
        db_name=None,
        execution_options={"schema_translate_map": SCHEMA_TRANSLATE_MAP},
    )


def get_db_connection_string(backup=False, instance_name=None):
    # Only import "config" on demand, as it depends on Datastore packages (and
    # GAE). When running via CLI or tests, we'll have this from the environment
    # instead (above).
    from rdr_service import config

    if os.environ.get("UNITTEST_FLAG", None):
        connection_string_key = "unittest_connection_string"
    elif backup:
        connection_string_key = "backup_db_connection_string"
    else:
        connection_string_key = "db_connection_string"

    result = config.get_db_config()[connection_string_key]
    if instance_name:
        if backup:
            raise Exception("backup and instance_name should not be used together")
        # Connect to the specified instance.
        return result.replace("rdrmaindb", instance_name)
    return result


def make_server_cursor_database(backup=False, instance_name=None):
    """
  Returns a database object that uses a server-side cursor when talking to the database.
  Useful in cases where you're reading a very large amount of data.
  """
    if get_db_connection_string().startswith("sqlite"):
        # SQLite doesn't have cursors; use the normal database during tests.
        return get_database()
    else:
        if backup:
            return _BackupSqlDatabase("rdr", connect_args={"cursorclass": SSCursor})
        return _SqlDatabase("rdr", instance_name=instance_name, connect_args={"cursorclass": SSCursor})
