from model.base import Base
from sqlalchemy import Column, Integer, BLOB

class Config(Base):
  """The config resource definition."""
  __tablename__ = 'config'
  id = Column('id', Integer, primary_key=True)
  configuration = Column('configuration', BLOB, nullable=False)

    
