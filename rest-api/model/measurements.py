from model.base import Base
from model.utils import UTCDateTime
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Boolean, Integer, BLOB, ForeignKey, String, Float, Table

measurement_to_qualifier = Table('measurement_to_qualifier', Base.metadata,
    Column('measurement_id', Integer, ForeignKey('measurement.measurement_id'), primary_key=True),
    Column('qualifier_id', Integer, ForeignKey('measurement.measurement_id'), primary_key=True)
)

class PhysicalMeasurements(Base):
  __tablename__ = 'physical_measurements'
  physicalMeasurementsId = Column('physical_measurements_id', Integer, primary_key=True,
                                  autoincrement=False)
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         nullable=False)
  created = Column('created', UTCDateTime, nullable=False)
  resource = Column('resource', BLOB, nullable=False)
  final = Column('final', Boolean, nullable=False)
  # The ID that these measurements are an amendment of (points from new to old)
  amendedMeasurementsId = Column('amended_measurements_id', Integer,
                                 ForeignKey('physical_measurements.physical_measurements_id'))
  logPositionId = Column('log_position_id', Integer, ForeignKey('log_position.log_position_id'),
                         nullable=False)
  logPosition = relationship('LogPosition')
  measurements = relationship('Measurement', cascade='all, delete-orphan')

class Measurement(Base):
  """An individual measurement; child of PhysicalMeasurements."""
  __tablename__ = 'measurement'
  # Note: measurementId will be physicalMeasurementsId * 100 + an index. (This way we don't have to
  # generate N random unique IDs.)
  measurementId = Column('measurement_id', Integer, primary_key=True, autoincrement=False)
  physicalMeasurementsId = Column('physical_measurements_id', Integer,
                                  ForeignKey('physical_measurements.physical_measurements_id'),
                                  nullable=False)
  codeSystem = Column('code_system', String(255), nullable=False)
  codeValue = Column('code_value', String(255), nullable=False)
  measurementTime = Column('measurement_time', UTCDateTime, nullable=False)
  bodySiteCodeSystem = Column('body_site_code_system', String(255))
  bodySiteCodeValue = Column('body_site_code_value', String(255))
  valueString = Column('value_string', String(1024))
  valueDecimal = Column('value_decimal', Float)
  valueUnit = Column('value_unit', String(255))
  valueCodeSystem = Column('value_code_system', String(255))
  valueCodeValue = Column('value_code_value', String(255))
  valueDateTime = Column('value_datetime', UTCDateTime)
  parentId = Column('parent_id', Integer, ForeignKey('measurement.measurement_id'))
  qualifierId = Column('qualifier_id', Integer, ForeignKey('measurement.measurement_id'))
  measurements = relationship('Measurement', cascade='all, delete-orphan', foreign_keys=[parentId])
  qualifiers = relationship('Measurement', secondary=measurement_to_qualifier,
                        primaryjoin=measurementId == measurement_to_qualifier.c.measurement_id,
                        secondaryjoin=measurementId == measurement_to_qualifier.c.qualifier_id)
