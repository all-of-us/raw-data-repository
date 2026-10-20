import logging
import os
import re
from logging.config import fileConfig
from rdr_service.model import database
import sqlalchemy as sa
from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlalchemy.dialects.mysql.types import SMALLINT, TINYINT
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.compiler import compiles  # pylint: disable=unused-import
from sqlalchemy.types import BLOB  # pylint: disable=unused-import
from rdr_service.model.field_types import BlobUTF8  # pylint: disable=unused-import
from rdr_service.model import compiler  # pylint: disable=unused-import

# Importing this is what gets our model available for Alembic.
from rdr_service.model import utils  # pylint: disable=unused-import
from rdr_service.model.base import Base, MetricsBase, RexBase, NphBase, PPSCBase

USE_TWOPHASE = False

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)
logger = logging.getLogger("alembic.env")

# gather section names referring to different
# databases.  These are named "engine1", "engine2"
# in the sample .ini file.
db_names = config.get_main_option("databases")

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = {
    "rdr": Base.metadata,
    "metrics": MetricsBase.metadata,
    "nph": NphBase.metadata,
    "rex": RexBase.metadata,
    "ppsc": PPSCBase.metadata
}

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def get_url():
    return make_url(os.environ["DB_CONNECTION_STRING"])


# MySQL 8.4 users authenticate with 'caching_sha2_password'. Over the Cloud SQL proxy's plain TCP port the client
# refuses to send the password unless it can RSA-encrypt it with the server's public key.
# The key comes from: SHOW GLOBAL STATUS LIKE 'Caching_sha2_password_rsa_public_key';
# Override the location with the MYSQL_SERVER_PUBLIC_KEY_PATH environment variable (e.g. per GCP project).
# The key file is kept outside the repo so it isn't committed.
DEFAULT_SERVER_PUBLIC_KEY_PATH = os.path.expanduser("~/mysql_server_public_key.pem")


def get_connect_args():
    key_path = os.path.expanduser(os.environ.get("MYSQL_SERVER_PUBLIC_KEY_PATH", DEFAULT_SERVER_PUBLIC_KEY_PATH))
    if not os.path.isfile(key_path):
        logger.warning("MySQL server public key not found at %s, connecting without it.", key_path)
        return {}
    with open(key_path) as key_file:
        contents = key_file.read()
    if "BEGIN PUBLIC KEY" not in contents or "PASTE_THE_KEY_BODY_HERE" in contents:
        logger.warning("MySQL server public key file %s is not a valid PEM key, connecting without it.", key_path)
        return {}
    return {"server_public_key_path": key_path}


def my_compare_type(ctx, inspected_column, metadata_column, inspected_type, metadata_type):
    # pylint: disable=unused-argument

    # return True if the types are different,
    # False if not, or None to allow the default implementation
    # to compare these types
    if isinstance(metadata_type, sa.Boolean) and isinstance(inspected_type, TINYINT):
        return False
    if isinstance(metadata_type, utils.Enum) and isinstance(inspected_type, SMALLINT):
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
    engines = {}
    for name in re.split(r",\s*", db_names):
        url = get_url()
        url.database = name
        engines[name] = {"url": str(url)}

    for name, rec in list(engines.items()):
        logger.info("Migrating database %s" % name)
        file_ = "%s.sql" % name
        logger.info("Writing output to %s" % file_)
        with open(file_, "w") as buf:
            context.configure(
                url=rec["url"],
                output_buffer=buf,
                target_metadata=target_metadata.get(name),
                literal_binds=True,
                include_schemas=True,
                include_object=include_object_fn(name),
            )
            with context.begin_transaction():
                context.run_migrations(engine_name=name)


autogen_denied_list = set(["aggregate_metrics_ibfk_1"])


def include_object_fn(engine_name):
    def f(obj, name, type_, reflected, compare_to):
        # pylint: disable=unused-argument
        # Workaround what appears to be an alembic bug for multi-schema foreign
        # keys. This should still generate the initial foreign key contraint, but
        # stops repeated create/destroys of the constraint on subsequent runs.
        if type_ == "foreign_key_constraint" and obj.table.schema == "metrics":
            return False
        if type_ == "foreign_key_constraint" and engine_name in ["nph", "ppsc"]:
            return False
        if name in autogen_denied_list:
            logger.info("skipping not allowed %s", name)
            return False
        if type_ == "table" and reflected:
            # This normally wouldn't be necessary, except that our RDR models do not
            # specify a schema, so we would otherwise attempt to apply their tables
            # to the metrics DB. See option 1c on
            # https://docs.google.com/document/d/1FTmH-DDVlyY7BNsBzj0FV9m_kWQpCjKM__zK0GmIiCc
            return obj.schema == engine_name
        return True

    return f


def run_migrations_online():
    """Run migrations in 'online' mode.

  In this scenario we need to create an Engine
  and associate a connection with the context.

  """

    # for the direct-to-DB use case, start a transaction on all
    # engines, then run all migrations, then commit all transactions.

    engines = {}
    for name in re.split(r",\s*", db_names):
        url = get_url()
        url.database = name

        config_dict = {"url": str(url), "connect_args": get_connect_args()}
        engines[name] = {"engine": engine_from_config(config_dict, prefix="", poolclass=pool.NullPool)}
    for name, rec in list(engines.items()):
        engine = rec["engine"]
        rec["connection"] = conn = engine.connect()

        if USE_TWOPHASE:
            rec["transaction"] = conn.begin_twophase()
        else:
            rec["transaction"] = conn.begin()

    try:
        for name, rec in list(engines.items()):
            logger.info("Migrating database %s" % name)
            context.configure(
                connection=rec["connection"],
                upgrade_token="%s_upgrades" % name,
                downgrade_token="%s_downgrades" % name,
                target_metadata=target_metadata.get(name),
                include_schemas=True,
                include_object=include_object_fn(name),
                process_revision_directives=database.AutoHistoryRevisionGenerator.process_revision_directives
            )
            context.run_migrations(engine_name=name)

        if USE_TWOPHASE:
            for rec in list(engines.values()):
                rec["transaction"].prepare()

        for rec in list(engines.values()):
            rec["transaction"].commit()
    except:
        for rec in list(engines.values()):
            rec["transaction"].rollback()
        raise
    finally:
        for rec in list(engines.values()):
            rec["connection"].close()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
