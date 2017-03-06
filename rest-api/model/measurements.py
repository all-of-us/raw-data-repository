import clock

from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Boolean, Integer, DateTime, BLOB, ForeignKey

class PhysicalMeasurements(Base):
  __tablename__ = 'physical_measurements'
  physicalMeasurementsId = Column('physical_measurements_id', Integer, primary_key=True,
                                  autoincrement=False)
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         nullable=False)
  created = Column('created', DateTime, default=clock.CLOCK.now, nullable=False)
  resource = Column('resource', BLOB, nullable=False)
  final = Column('final', Boolean, nullable=False)
  # The ID that these measurements are an amendment of (points from new to old)
  amendedMeasurementsId = Column('amended_measurements_id', Integer,
                                 ForeignKey('physical_measurements.physical_measurements_id'))
  logPositionId = Column('log_position_id', Integer, ForeignKey('log_position.log_position_id'),
                         nullable=False)
  logPosition = relationship('LogPosition')

