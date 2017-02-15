"""Defines the declarative base. Import this and extend from Base for all tables."""
from sqlalchemy.ext.declarative import declarative_base
from dictalchemy import DictableModel

Base = declarative_base(cls=DictableModel)

