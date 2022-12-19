"""Defines the declarative base. Import this and extend from Base for all rdr
tables. Extend MetricsBase for all metrics tables."""
import json
from collections import OrderedDict
from datetime import date, datetime

from dictalchemy import DictableModel
from sqlalchemy import MetaData, inspect
from sqlalchemy.ext.declarative import declarative_base

from rdr_service import clock

# Due to the use of DictableModel, all model objects that extend from Base
# define asdict() and fromdict() methods, which can be used in tests and when
# copying rows from the database (e.g. creating history rows.)
#
# asdict() produces a dictionary from the fields of the model object;
# see https://pythonhosted.org/dictalchemy/#using-asdict. Subclasses can define
# asdict_with_children() methods to provide a convenience wrapper around asdict(),
# supplying fields for child collections in a follow parameter.
#
# fromdict() populates fields in the model object based on an input dictionary;
# see https://pythonhosted.org/dictalchemy/#using-fromdict. fromdict() does not
# populate fields that contain lists.
Base = declarative_base(cls=DictableModel)

# MetricsBase is the parent for all models in the "metrics" DB. These are
# collected separately for DB migration purposes.
MetricsBase = declarative_base(cls=DictableModel, metadata=MetaData(schema="metrics"))

RexBase = declarative_base(cls=DictableModel, metadata=MetaData(schema="rex"))

NphBase = declarative_base(cls=DictableModel, metadata=MetaData(schema="nph"))


class ModelMixin(object):
    """
  Mixin for models, includes methods for importing/exporting JSON data.
  """

    def from_json(self, *args, **kwargs):
        """
    If parameter values in args, then the value is expected to be a dictionary from a json response
    from the server. If parameter values are in kwargs, then they are named parameters passed
    when the object is instantiated.
    # TODO: Needs to identify ModelEnum fields and set the Enum value from a string. See to_dict().
    """
        if args is not None and len(args) is not 0 and args[0] is not None:
            for key, value in list(args[0].items()):
                self.__dict__[key] = value
                # print('{0} : {1}'.format(key, value))

        else:
            for key, value in list(kwargs.items()):
                self.__dict__[key] = value
                # print('{0} : {1}'.format(key, value))

    def to_json(self, pretty=False):
        """
    Dump class to json string. Enhanced version of `to_client_json()`
    :return: json string
    """

        def json_serial(obj):
            """JSON serializer for objects not serializable by default json code"""
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            return obj.__repr__()

        data = self.to_dict()

        if pretty:
            output = json.dumps(data, default=json_serial, indent=4)
        else:
            output = json.dumps(data, default=json_serial)

        return output

    def to_dict(self):
        """
    Dump class to python dict
    :return: dict
    """
        data = OrderedDict()
        mapper = inspect(self)

        for column in mapper.attrs:
            key = str(column.key)
            value = getattr(self, key)

            if isinstance(value, (datetime, date)):
                data[key] = value.isoformat()
            # Check for Enum and return name
            elif hasattr(value, "name"):
                data[key] = value.name
            else:
                data[key] = value

        return data

    def __repr__(self):
        return self.to_json()


# pylint: disable=unused-argument
def model_insert_listener(mapper, connection, target):
    """ On insert auto set `created` and `modified` column values """
    now = clock.CLOCK.now()
    target.created = now
    target.modified = now


# pylint: disable=unused-argument
def model_update_listener(mapper, connection, target):
    """ On update auto set `modified` column value """
    target.modified = clock.CLOCK.now()


def get_column_name(model_type, field_name):
    return getattr(model_type, field_name).property.columns[0].name


def add_table_history_table(table, op, id_field_name='id'):
    """
  Create a history table and add triggers so we automatically capture record changes.
  Note: !!! Remember to drop all unique indexes (not primary key) on the new history table. !!!
        !!! Ex: "call sp_drop_index_if_exists('xxxxx_history', 'idx_unique_index_name')" !!!
  :param table: table name
  :param op: sqlalchemy op object
  :param id_field_name: Name of the SQL column that is the primary key on the table
  """

    # https://stackoverflow.com/questions/12563706/is-there-a-mysql-option-feature-to-track-history-of-changes-to-records
    sql = f"""
      CREATE TABLE {table}_history LIKE {table};

      ALTER TABLE {table}_history
        CHANGE COLUMN `{id_field_name}` `{id_field_name}` INTEGER NOT NULL,
        DROP PRIMARY KEY,
        ADD revision_action VARCHAR(8) DEFAULT 'insert' FIRST,
        ADD revision_id INT(6) NOT NULL AFTER revision_action,
        ADD revision_dt DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) AFTER revision_id;

      ALTER TABLE {table}_history
        ADD INDEX idx_revision (revision_id),
        CHANGE COLUMN `revision_id` `revision_id` INT(6) NOT NULL AUTO_INCREMENT,
        ADD PRIMARY KEY (`{id_field_name}`, revision_id);

      CREATE TRIGGER {table}__ai AFTER INSERT ON {table} FOR EACH ROW
          INSERT INTO {table}_history SELECT 'insert', NULL, NOW(6), d.*
          FROM {table} AS d WHERE d.{id_field_name} = NEW.{id_field_name};

      CREATE TRIGGER {table}__au AFTER UPDATE ON {table} FOR EACH ROW
          INSERT INTO {table}_history SELECT 'update', NULL, NOW(6), d.*
          FROM {table} AS d WHERE d.{id_field_name} = NEW.{id_field_name};

      CREATE TRIGGER {table}__bd BEFORE DELETE ON {table} FOR EACH ROW
          INSERT INTO {table}_history SELECT 'delete', NULL, NOW(6), d.*
          FROM {table} AS d WHERE d.{id_field_name} = OLD.{id_field_name};
    """

    op.execute(sql)


def drop_table_history_table(table, op):
    """
  Drop the history table associated with given table
  :param table: table name associated with history table
  :param op: sqlalchemy op object
  """

    sql = """
      DROP TRIGGER IF EXISTS {0}__ai;
      DROP TRIGGER IF EXISTS {0}__au;
      DROP TRIGGER IF EXISTS {0}__bd;
      DROP TABLE {0}_history;
  """.format(
        table
    )

    op.execute(sql)
