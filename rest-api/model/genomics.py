from model.base import Base
from model.utils import Enum
from protorpc import messages
from sqlalchemy import Column, DateTime, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship


class GenomicSetStatus(messages.Enum):
  """ Status of Genomic Set"""
  UNSET = 0
  VALID = 1
  INVALID = 2

class GenomicValidationStatus(messages.Enum):
  """ Validation Status """
  UNSET = 0
  VALID = 1
  INVALID_BIOBANK_ORDER = 2
  INVALID_NY_ZIPCODE = 3
  INVALID_SEX_AT_BIRTH = 4
  INVALID_GENOME_TYPE = 5
  INVALID_CONSENT = 6
  INVALID_WITHDRAW_STATUS = 7
  INVALID_AGE = 8
  INVALID_DUP_PARTICIPANT = 9


class GenomicSet(Base):
  """
  Genomic Set model
  """
  __tablename__ = 'genomic_set'

  genomicSetMember = relationship('GenomicSetMember', cascade='all, delete-orphan')

  # Primary Key
  id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
  # have mysql set the creation data for each new order
  created = Column('created', DateTime, nullable=True)
  # have mysql always update the modified data when the record is changed
  modified = Column('modified', DateTime, nullable=True)

  genomicSetName = Column('genomic_set_name', String(80), nullable=False)
  genomicSetCriteria = Column('genomic_set_criteria', String(80), nullable=False)
  genomicSetVersion = Column('genomic_set_version', Integer, nullable=False)
  # genomic set file
  genomicSetFile = Column('genomic_set_file', String(250), nullable=True)
  # genomic set file timestamp
  genomicSetFileTime = Column('genomic_set_file_time', DateTime, nullable=True)

  genomicSetStatus = Column('genomic_set_status',
                            Enum(GenomicSetStatus), default=GenomicSetStatus.UNSET)
  validatedTime = Column('validated_time', DateTime, nullable=True)

  __table_args__ = (
    UniqueConstraint('genomic_set_name', 'genomic_set_version', name='uidx_genomic_name_version'),
  )


class GenomicSetMember(Base):
  """
  Genomic Set Member model
  """
  __tablename__ = 'genomic_set_member'

  # Primary Key
  id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
  # have mysql set the creation data for each new order
  created = Column('created', DateTime, nullable=True)
  # have mysql always update the modified data when the record is changed
  modified = Column('modified', DateTime, nullable=True)

  genomicSetId = Column('genomic_set_id', Integer, ForeignKey('genomic_set.id'), nullable=False)

  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         nullable=False)
  nyFlag = Column('ny_flag', Integer, nullable=True)

  sexAtBirth = Column('sex_at_birth', String(20), nullable=True)
  genomeType = Column('genome_type', String(80), nullable=True)

  biobankOrderId = Column('biobank_order_id', String(80),
                          ForeignKey('biobank_order.biobank_order_id'), unique=True, nullable=True)

  validationStatus = Column('validation_status', Enum(GenomicValidationStatus),
                            default=GenomicValidationStatus.UNSET)

  validatedTime = Column('validated_time', DateTime, nullable=True)
