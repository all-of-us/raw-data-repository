import clock

from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, DateTime, BLOB, ForeignKey

class PhysicalMeasurements(Base):
  """The physical measurements resource definition"""
  __tablename__ = 'physical_measurements'
  id = Column('id', Integer, primary_key=True, autoincrement=False)
  participantId = Column('participant_id', Integer, ForeignKey('participant.id'), nullable=False)
  created = Column('created', DateTime, default=clock.CLOCK.now, nullable=False)
  resource = Column('resource', BLOB, nullable=False)
  amendedMeasurementsId = Column('amended_measurements_id', Integer, 
                                 ForeignKey('physical_measurements.id'))
  logPositionId = Column('log_position_id', Integer, ForeignKey('log_position.id'), nullable=False)
  logPosition = relationship('LogPosition')
  
                                 