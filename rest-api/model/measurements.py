from model.base import Base
from sqlalchemy import Column, Integer, DateTime, BLOB, ForeignKey

class PhysicalMeasurements(Base):
  """The physical measurements resource definition"""
  __tablename__ = 'physical_measurements'
  id = Column('id', Integer, primary_key=True)
  participantId = Column('participant_id', Integer, ForeignKey('participant.id'))
  created = Column('created', DateTime)
  resource = Column('resource', BLOB)
