"""Defines the declarative base. Import this and extend from Base for all rdr
tables. Extend MetricsBase for all metrics tables."""

from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from dictalchemy import DictableModel

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
MetricsBase = declarative_base(cls=DictableModel, metadata=MetaData(schema='metrics'))

def get_column_name(model_type, field_name):
  return getattr(model_type, field_name).property.columns[0].name

def add_table_history_table(table, op):
  """
  Create a history table and add triggers so we automatically capture record changes.
  :param table: table name
  :param op: sqlalchemy op object
  """

  # https://stackoverflow.com/questions/12563706/is-there-a-mysql-option-feature-to-track-history-of-changes-to-records
  sql = """
      CREATE TABLE {0}_history LIKE {0};

      ALTER TABLE {0}_history
        CHANGE COLUMN `id` `id` INTEGER NOT NULL,
        DROP PRIMARY KEY,
        ADD revision_action VARCHAR(8) DEFAULT 'insert' FIRST,
        ADD revision_id INT(6) NOT NULL AFTER revision_action,
        ADD revision_dt DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) AFTER revision_id;

      ALTER TABLE {0}_history
        ADD INDEX idx_revision (revision_id),
        CHANGE COLUMN `revision_id` `revision_id` INT(6) NOT NULL AUTO_INCREMENT,
        ADD PRIMARY KEY (`id`, revision_id);
      
      CREATE TRIGGER {0}__ai AFTER INSERT ON {0} FOR EACH ROW
          INSERT INTO {0}_history SELECT 'insert', NULL, NOW(6), d.*
          FROM {0} AS d WHERE d.id = NEW.id;

      CREATE TRIGGER {0}__au AFTER UPDATE ON {0} FOR EACH ROW
          INSERT INTO {0}_history SELECT 'update', NULL, NOW(6), d.*
          FROM {0} AS d WHERE d.id = NEW.id;

      CREATE TRIGGER {0}__bd BEFORE DELETE ON {0} FOR EACH ROW
          INSERT INTO {0}_history SELECT 'delete', NULL, NOW(6), d.*
          FROM {0} AS d WHERE d.id = OLD.id;
      """.format(table)

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
  """.format(table)

  op.execute(sql)
