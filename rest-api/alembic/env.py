import os
# Importing this is what gets our model available for Alembic.
import model.database # pylint: disable=unused-import
import sqlalchemy as sa

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import BLOB
from sqlalchemy.dialects.mysql.types import TINYINT, SMALLINT
from alembic import context
from sqlalchemy import create_engine
from logging.config import fileConfig
from model.base import Base


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

# In MySQL, make BLOB fields be LONGBLOB (which supports large blobs)
@compiles(BLOB, "mysql")
def compile_blob_in_mysql_to_longblob(type_, compiler, **kw):
  #pylint: disable=unused-argument
  return "LONGBLOB"

def get_url():
  return os.environ['DB_CONNECTION_STRING']

def my_compare_type(ctx, inspected_column, metadata_column, inspected_type, metadata_type):
   #pylint: disable=unused-argument

  # return True if the types are different,
  # False if not, or None to allow the default implementation
  # to compare these types
  if isinstance(metadata_type, sa.Boolean) and isinstance(inspected_type, TINYINT):
    return False
  if isinstance(metadata_type, model.utils.Enum) and isinstance(inspected_type, SMALLINT):
    return False
  return None

def run_migrations_offline():
  """Run migrations in 'offline' mode.

  This configures the context with just a URL
  and not an Engine, though an Engine is acceptable
  here as well.  By skipping the Engine creation
  we don't even need a DBAPI to be available.

  Calls to context.execute() here emit the given string to the
  script output.

  """
  url = get_url()
  context.configure(
      url=url, target_metadata=target_metadata, literal_binds=True, compare_type=my_compare_type)

  with context.begin_transaction():
    context.run_migrations()


def run_migrations_online():
  """Run migrations in 'online' mode.

  In this scenario we need to create an Engine
  and associate a connection with the context.

  """
  connectable = create_engine(get_url())

  with connectable.connect() as connection:
    context.configure(
          connection=connection,
          target_metadata=target_metadata,
          compare_type=my_compare_type
    )

    with context.begin_transaction():
      context.run_migrations()

if context.is_offline_mode():
  run_migrations_offline()
else:
  run_migrations_online()
