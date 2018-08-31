from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, ForeignKey, Boolean, UnicodeText
from sqlalchemy.ext.declarative import declared_attr

from model.base import Base
from model.utils import UTCDateTime, Enum
from participant_enums import BiobankOrderStatus


class BiobankOrderBase(object):
  """An order requesting samples.

  The order contains a list of samples stored in BiobankOrderedSample; the actual delivered and
  stored samples are tracked in BiobankStoredSample. Our reconciliation report compares the two.
  """
  _MAIN_ID_SYSTEM = 'https://orders.mayomedicallaboratories.com'

  # A GUID for the order, provided by Biobank. This is the ID assigned in HealthPro, which is sent
  # to us as an identifier with the mayomedicallaboritories.com "system".
  biobankOrderId = Column('biobank_order_id', String(80), primary_key=True)

  # Incrementing version, starts at 1 and is incremented on each update.
  version = Column('version', Integer, nullable=False)

  # The username / email of the HealthPro user that created the order -- createdInfo['author']
  # in the resulting JSON.
  sourceUsername = Column('source_username', String(255))

  # The username / email of the HealthPro user that collected the order -- collectedInfo['author']
  # in the resulting JSON.
  collectedUsername = Column('collected_username', String(255))

  # The username / email of the HealthPro user that processed the order -- processedInfo['author']
  # in the resulting JSON.
  processedUsername = Column('processed_username', String(255))

  # The username / email of the HealthPro user that finalized the order -- finalizedInfo['author']
  # in the resulting JSON.
  finalizedUsername = Column('finalized_username', String(255))

  # cancelled finalized order may still be shipped to biobank for destruction
  # orderstatus can be cancelled/amended/restored
  # A null value or UNSET == finalized (i.e. the current accepted value)

  orderStatus = Column('order_status', Enum(BiobankOrderStatus))
  # a cancelled or edited order must have a reason. Set on the old row because cancelled orders
  # don't create a new row like amended orders do.
  amendedReason = Column('amended_reason', UnicodeText)
  lastModified = Column('last_modified', UTCDateTime)

  restoredUsername = Column('restored_username', String(255))
  restoredTime = Column('restored_time', UTCDateTime)

  amendedUsername = Column('amended_username', String(255))
  amendedTime = Column('amended_time', UTCDateTime)

  cancelledUsername = Column('cancelled_username', String(255))
  cancelledTime = Column('cancelled_time', UTCDateTime)

  # Additional fields stored for future use.
  created = Column('created', UTCDateTime, nullable=False)
  collectedNote = Column('collected_note', UnicodeText)
  processedNote = Column('processed_note', UnicodeText)
  finalizedNote = Column('finalized_note', UnicodeText)

  @declared_attr
  def participantId(cls):
    return Column('participant_id', Integer, ForeignKey(
      'participant.participant_id'), nullable=False)

  @declared_attr
  def amendedBiobankOrderId(cls):
    return Column('amended_biobank_order_id', String(80),
                  ForeignKey('biobank_order.biobank_order_id'))

  # For syncing new orders.
  @declared_attr
  def logPositionId(cls):
    return Column('log_position_id', Integer, ForeignKey(
      'log_position.log_position_id'), nullable=False)


  # The site that created the order -- createdInfo['site'] in the resulting JSON
  @declared_attr
  def sourceSiteId(cls):
    return Column('source_site_id', Integer, ForeignKey('site.site_id'))


  # The site that collected the order -- collectedInfo['site'] in the resulting JSON
  @declared_attr
  def collectedSiteId(cls):
    return Column('collected_site_id', Integer, ForeignKey('site.site_id'))


  # The site that processed the order -- processedInfo['site'] in the resulting JSON
  @declared_attr
  def processedSiteId(cls):
    return Column('processed_site_id', Integer, ForeignKey('site.site_id'))


  # The site that finalized the order -- finalizedInfo['site'] in the resulting JSON
  @declared_attr
  def finalizedSiteId(cls):
    return Column('finalized_site_id', Integer, ForeignKey('site.site_id'))

  @declared_attr
  def restoredSiteId(cls):
    return Column('restored_site_id', Integer, ForeignKey('site.site_id'))

  @declared_attr
  def amendedSiteId(cls):
    return Column('amended_site_id', Integer, ForeignKey('site.site_id'))

  @declared_attr
  def cancelledSiteId(cls):
    return Column('cancelled_site_id', Integer, ForeignKey('site.site_id'))



class BiobankOrder(BiobankOrderBase, Base):
  __tablename__ = 'biobank_order'
  logPosition = relationship('LogPosition')
  identifiers = relationship('BiobankOrderIdentifier', cascade='all, delete-orphan')
  samples = relationship('BiobankOrderedSample', cascade='all, delete-orphan')


class BiobankOrderIdentifierBase(object):
  @declared_attr
  def system(cls):
    return Column('system', String(80), primary_key=True)
  @declared_attr
  def value(cls):
    return Column('value', String(80), primary_key=True)
  @declared_attr
  def biobankOrderId(cls):
    return Column(
    'biobank_order_id', String(80), ForeignKey('biobank_order.biobank_order_id'), nullable=False)


class BiobankOrderIdentifier(BiobankOrderIdentifierBase, Base):
  """Arbitrary IDs for a BiobankOrder in other systems.

  Other clients may create these, but they must be unique within each system.
  """
  __tablename__ = 'biobank_order_identifier'


class BiobankOrderedSampleBase(object):
  @declared_attr
  def biobankOrderId(cls):
    return Column(
    'order_id', String(80), ForeignKey('biobank_order.biobank_order_id'), primary_key=True)

  # Unique within an order, though the same test may be redone in another order for the participant.
  @declared_attr
  def test(cls):
    return Column('test', String(80), primary_key=True)

  # Free text description of the sample.
  @declared_attr
  def description(cls):
    return Column('description', UnicodeText, nullable=False)
  @declared_attr
  def processingRequired(cls):
    return Column('processing_required', Boolean, nullable=False)
  @declared_attr
  def collected(cls):
    return Column('collected', UTCDateTime)
  @declared_attr
  def processed(cls):
    return Column('processed', UTCDateTime)
  @declared_attr
  def finalized(cls):
    return Column('finalized', UTCDateTime)


class BiobankOrderedSample(BiobankOrderedSampleBase, Base):
  """Samples listed by a Biobank order.

  These are distinct from BiobankStoredSamples, which tracks received samples. The two should
  eventually match up, but we see BiobankOrderedSamples first and track them separately.
  """
  __tablename__ = 'biobank_ordered_sample'


class BiobankOrderHistory(BiobankOrderBase, Base):
  __tablename__ = 'biobank_history'

  version = Column('version', Integer, primary_key=True)


class BiobankOrderedSampleHistory(BiobankOrderedSampleBase, Base):
  __tablename__ = 'biobank_ordered_sample_history'

  version = Column('version', Integer, primary_key=True)


class BiobankOrderIdentifierHistory(BiobankOrderIdentifierBase, Base):
  __tablename__ = 'biobank_order_identifier_history'

  version = Column('version', Integer, primary_key=True)
