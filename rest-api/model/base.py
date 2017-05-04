"""Defines the declarative base. Import this and extend from Base for all tables."""

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

def get_column_name(model_type, field_name):
  return getattr(model_type, field_name).property.columns[0].name
