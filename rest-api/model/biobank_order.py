import clock

from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index, Boolean

class BiobankOrder(Base):  
  """The Biobank order resource definition."""
  __tablename__ = 'biobank_order'
  id = Column('id', Integer, primary_key=True, autoincrement=False)
  participantId = Column('participant_id', Integer, ForeignKey('participant.id'), nullable=False)  
  created = Column('created', DateTime, default=clock.CLOCK.now, nullable=False)  
  sourceSiteSystem = Column('source_site_system', String(80))
  sourceSiteValue = Column('source_site_value', String(80))  
  collected = Column('collected', String(255))
  processed = Column('processed', String(255))
  finalized = Column('finalized', String(255))
  identifiers = relationship('BiobankOrderIdentifier', cascade='all, delete-orphan')    
  samples = relationship('BiobankOrderSample', cascade='all, delete-orphan')
  logPositionId = Column('log_position_id', Integer, ForeignKey('log_position.id'), nullable=False)
  logPosition = relationship('LogPosition')  

class BiobankOrderIdentifier(Base):
  """An identifier in a Biobank order"""
  __tablename__ = 'biobank_order_identifier'  
  system = Column('system', String(80), primary_key=True)
  value = Column('value', String(80), primary_key=True)
  orderId = Column('order_id', Integer, ForeignKey('biobank_order.id'), nullable=False)

class BiobankOrderSample(Base):
  """A sample in a Biobank order"""
  __tablename__ = 'biobank_order_sample'
  orderId = Column('order_id', Integer, ForeignKey('biobank_order.id'), primary_key=True)  
  test = Column('test', String(80), primary_key=True)
  description = Column('description', String(255))
  processingRequired = Column('processing_required', Boolean, nullable=False)
  collected = Column('collected', DateTime)
  processed = Column('processed', DateTime)
  finalized = Column('finalized', DateTime)

