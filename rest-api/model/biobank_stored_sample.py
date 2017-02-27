from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey


class BiobankStoredSample(Base):
  """Physical sampels which have been reported as received at Biobank.

  Each participant has an associated list of samples. Biobank uploads a list of all received
  samples periodically, and we update our list of stored samples to match. The output is a
  reconciliation report of ordered and stored samples; see BiobankOrder.
  """
  __tablename__ = 'biobank_stored_sample'
  biobankStoredSampleId = Column('biobank_stored_sample_id', Integer, primary_key=True)
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'))
  familyId = Column('family_id', String(80))
  sampleId = Column('sample_id', String(80))
  storageStatus = Column('storage_status', String(80))
  type = Column('type', String(80))
  testCode = Column('test_code', String(80))
  treatments = Column('treatments', String(80))
  expectedVolume = Column('expected_volume', String(80))
  quantity = Column('quantity', String(80))
  containerType = Column('container_type', String(80))
  collectionDate = Column('collection_date', DateTime)
  disposalStatus = Column('disposal_status', String(80))
  disposedDate = Column('disposed_date', DateTime)

  # One sample taken from a participant (the parent sample) may be physically divided up for
  # storage (into multiple child samples).
  parentSampleId = Column('parent_sample_id', Integer,
                          ForeignKey('biobank_stored_sample.biobank_stored_sample_id'))

  confirmedDate = Column('confirmed_date', DateTime)
  logPositionId = Column('log_position_id', Integer, ForeignKey('log_position.log_position_id'),
                         nullable=False)
  logPosition = relationship('LogPosition')
