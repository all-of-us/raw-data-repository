import clock

from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Index, Boolean, Text

class BiobankOrder(Base):    
  __tablename__ = 'biobank_order'
  biobankOrderId = Column('biobank_order_id', Integer, primary_key=True, autoincrement=False)
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'), 
                         nullable=False)  
  created = Column('created', DateTime, default=clock.CLOCK.now, nullable=False)  
  sourceSiteSystem = Column('source_site_system', String(80))
  sourceSiteValue = Column('source_site_value', String(80))  
  collected = Column('collected', Text)
  processed = Column('processed', Text)
  finalized = Column('finalized', Text)
  identifiers = relationship('BiobankOrderIdentifier', cascade='all, delete-orphan')    
  samples = relationship('BiobankOrderedSample', cascade='all, delete-orphan')
  logPositionId = Column('log_position_id', Integer, ForeignKey('log_position.log_position_id'), 
                         nullable=False)
  logPosition = relationship('LogPosition')  

class BiobankOrderIdentifier(Base):  
  __tablename__ = 'biobank_order_identifier'  
  system = Column('system', String(80), primary_key=True)
  value = Column('value', String(80), primary_key=True)
  orderId = Column('order_id', Integer, ForeignKey('biobank_order.biobank_order_id'), 
                   nullable=False)

class BiobankOrderedSample(Base):  
  __tablename__ = 'biobank_ordered_sample'
  orderId = Column('order_id', Integer, ForeignKey('biobank_order.biobank_order_id'), 
                   primary_key=True)  
  test = Column('test', String(80), primary_key=True)
  description = Column('description', Text)
  processingRequired = Column('processing_required', Boolean, nullable=False)
  collected = Column('collected', DateTime)
  processed = Column('processed', DateTime)
  finalized = Column('finalized', DateTime)

