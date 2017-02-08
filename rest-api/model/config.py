from model.base import Base
from sqlalchemy import Column, Integer, BLOB

class Config(Base):  
  __tablename__ = 'config'
  configId = Column('config_id', Integer, primary_key=True)
  configuration = Column('configuration', BLOB, nullable=False)

    
