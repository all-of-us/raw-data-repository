import clock

from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, UnicodeText


class BiobankOrder(Base):
  """An order requesting samples. The order contains a list of samples stored in
  BiobankOrderedSample; the actual delivered and stored samples are stored in BiobankStoredSample.
  Our reconciliation report compares the two.
  """
  __tablename__ = 'biobank_order'
  # We want autoincrement=False for the ID, but omit it to avoid warnings and enforce a
  # client-specified ID in the DAO layer.
  biobankOrderId = Column('biobank_order_id', Integer, primary_key=True)
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         nullable=False)
  created = Column('created', DateTime, default=clock.CLOCK.now, nullable=False)
  sourceSiteSystem = Column('source_site_system', String(80))
  sourceSiteValue = Column('source_site_value', String(80))
  collected = Column('collected', UnicodeText)
  processed = Column('processed', UnicodeText)
  finalized = Column('finalized', UnicodeText)
  identifiers = relationship('BiobankOrderIdentifier', cascade='all, delete-orphan')
  samples = relationship('BiobankOrderedSample', cascade='all, delete-orphan')
  logPositionId = Column('log_position_id', Integer, ForeignKey('log_position.log_position_id'),
                         nullable=False)
  logPosition = relationship('LogPosition')


class BiobankOrderIdentifier(Base):
  """Arbitrary IDs for a BiobankOrder in other systems.

  Other clients may create these, but they must be unique within each system.
  """
  __tablename__ = 'biobank_order_identifier'
  system = Column('system', String(80), primary_key=True)
  value = Column('value', String(80), primary_key=True)
  biobankOrderId = Column(
      'order_id', Integer, ForeignKey('biobank_order.biobank_order_id'), nullable=False)


class BiobankOrderedSample(Base):
  """Samples listed by a Biobank order.

  These are distinct from BiobankSamples, which tracks received samples. The two should eventually
  match up, but we see BiobankOrderedSamples first and track them separately.
  """
  __tablename__ = 'biobank_ordered_sample'
  biobankOrderId = Column(
      'order_id', Integer, ForeignKey('biobank_order.biobank_order_id'), primary_key=True)
  test = Column('test', String(80), primary_key=True)
  description = Column('description', UnicodeText, nullable=False)
  processingRequired = Column('processing_required', Boolean, nullable=False)
  collected = Column('collected', DateTime)
  processed = Column('processed', DateTime)
  finalized = Column('finalized', DateTime)
