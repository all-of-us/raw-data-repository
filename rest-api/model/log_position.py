from model.base import Base
from sqlalchemy import Column, Integer

class LogPosition(Base):
  """A position in a log, incremented whenever writes to particular tables occur. 
  Used for syncing changes to other clients."""
  __tablename__ = 'log_position'
  logPositionId = Column('log_position_id', Integer, primary_key=True)
