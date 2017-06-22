from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, ForeignKey, Boolean, UnicodeText

from model.base import Base
from model.utils import UTCDateTime


class BiobankOrder(Base):
  """An order requesting samples.

  The order contains a list of samples stored in BiobankOrderedSample; the actual delivered and
  stored samples are tracked in BiobankStoredSample. Our reconciliation report compares the two.
  """
  __tablename__ = 'biobank_order'
  _MAIN_ID_SYSTEM = 'https://orders.mayomedicallaboratories.com'

  # A GUID for the order, provided by Biobank. This is the ID assigned in HealthPro, which is sent
  # to us as an identifier with the mayomedicallaboritories.com "system".
  biobankOrderId = Column('biobank_order_id', String(80), primary_key=True)

  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         nullable=False)

  # For syncing new orders.
  logPositionId = Column('log_position_id', Integer, ForeignKey('log_position.log_position_id'),
                         nullable=False)
  logPosition = relationship('LogPosition')

  # The site that created the order -- createdInfo['site'] in the resulting JSON
  sourceSiteId = Column('source_site_id', Integer, ForeignKey('site.site_id'))
  # The username / email of the HealthPro user that created the order -- createdInfo['author']
  # in the resulting JSON.
  sourceUsername = Column('source_username', String(255))

  # The site that collected the order -- collectedInfo['site'] in the resulting JSON
  collectedSiteId = Column('collected_site_id', Integer, ForeignKey('site.site_id'))
  # The username / email of the HealthPro user that collected the order -- collectedInfo['author']
  # in the resulting JSON.
  collectedUsername = Column('collected_username', String(255))

  # The site that processed the order -- processedInfo['site'] in the resulting JSON
  processedSiteId = Column('processed_site_id', Integer, ForeignKey('site.site_id'))
  # The username / email of the HealthPro user that processed the order -- processedInfo['author']
  # in the resulting JSON.
  processedUsername = Column('processed_username', String(255))

  # The site that finalized the order -- finalizedInfo['site'] in the resulting JSON
  finalizedSiteId = Column('finalized_site_id', Integer, ForeignKey('site.site_id'))
  # The username / email of the HealthPro user that finalized the order -- finalizedInfo['author']
  # in the resulting JSON.
  finalizedUsername = Column('finalized_username', String(255))

  # Additional fields stored for future use.
  created = Column('created', UTCDateTime, nullable=False)
  collectedNote = Column('collected_note', UnicodeText)
  processedNote = Column('processed_note', UnicodeText)
  finalizedNote = Column('finalized_note', UnicodeText)
  identifiers = relationship('BiobankOrderIdentifier', cascade='all, delete-orphan')
  samples = relationship('BiobankOrderedSample', cascade='all, delete-orphan')


class BiobankOrderIdentifier(Base):
  """Arbitrary IDs for a BiobankOrder in other systems.

  Other clients may create these, but they must be unique within each system.
  """
  __tablename__ = 'biobank_order_identifier'
  system = Column('system', String(80), primary_key=True)
  value = Column('value', String(80), primary_key=True)
  biobankOrderId = Column(
      'biobank_order_id', String(80), ForeignKey('biobank_order.biobank_order_id'), nullable=False)


class BiobankOrderedSample(Base):
  """Samples listed by a Biobank order.

  These are distinct from BiobankStoredSamples, which tracks received samples. The two should
  eventually match up, but we see BiobankOrderedSamples first and track them separately.
  """
  __tablename__ = 'biobank_ordered_sample'
  biobankOrderId = Column(
      'order_id', String(80), ForeignKey('biobank_order.biobank_order_id'), primary_key=True)

  # Unique within an order, though the same test may be redone in another order for the participant.
  test = Column('test', String(80), primary_key=True)

  # Free text description of the sample.
  description = Column('description', UnicodeText, nullable=False)

  processingRequired = Column('processing_required', Boolean, nullable=False)
  collected = Column('collected', UTCDateTime)
  processed = Column('processed', UTCDateTime)
  finalized = Column('finalized', UTCDateTime)
