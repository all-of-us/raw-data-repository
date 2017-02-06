from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index, Boolean

"""The Biobank order resource definition. Note we don't support updating orders, so no history
is maintained.
"""
class BiobankOrder(Base):  
  __tablename__ = 'biobank_order'
  id = Column('id', Integer, primary_key=True)
  participantId = Column('participant_id', Integer, ForeignKey('participant.id'))  
  created = Column('created', DateTime)  
  sourceSiteSystem = Column('source_site_system', String(80))
  sourceSiteValue = Column('source_site_value', String(80))  
  collected = Column('collected', String(255))
  processed = Column('processed', String(255))
  finalized = Column('finalized', String(255))
  identifiers = relationship('BiobankOrderIdentifier', cascade='all, delete-orphan')    
  samples = relationship('BiobankOrderSample', cascade='all, delete-orphan')

"""An identifier in a Biobank order"""
class BiobankOrderIdentifier(Base):
  __tablename__ = 'biobank_order_identifier'  
  system = Column('system', String(80), primary_key=True)
  value = Column('value', String(80), primary_key=True)
  orderId = Column('order_id', Integer, ForeignKey('biobank_order.id'))

"""A sample in a Biobank order"""
class BiobankOrderSample(Base):
  __tablename__ = 'biobank_order_sample'
  orderId = Column('order_id', Integer, ForeignKey('biobank_order.id'), primary_key=True)  
  test = Column('test', String(80), primary_key=True)
  description = Column('description', String(255))
  processingRequired = Column('processing_required', Boolean)
  collected = Column('collected', DateTime)
  processed = Column('processed', DateTime)
  finalized = Column('finalized', DateTime)

