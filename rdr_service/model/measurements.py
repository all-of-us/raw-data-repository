from sqlalchemy import BIGINT, Boolean, Column, Float, ForeignKey, Integer, String, Table, Text, UnicodeText
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base
from rdr_service.model.utils import Enum, UTCDateTime
from rdr_service.participant_enums import PhysicalMeasurementsStatus, PhysicalMeasurementsCollectType, \
    OriginMeasurementUnit


measurement_to_qualifier = Table(
    "measurement_to_qualifier",
    Base.metadata,
    Column("measurement_id", BIGINT, ForeignKey("measurement.measurement_id"), primary_key=True),
    Column("qualifier_id", BIGINT, ForeignKey("measurement.measurement_id"), primary_key=True),
)


class PhysicalMeasurements(Base):
    __tablename__ = "physical_measurements"
    physicalMeasurementsId = Column("physical_measurements_id", Integer, primary_key=True, autoincrement=False)
    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), nullable=False)
    """ID of the participant that the physical measurements are for"""
    created = Column("created", UTCDateTime, nullable=False)
    final = Column("final", Boolean, nullable=False)
    """
    A flag indicating whether the physical measurements have been amended. Set to 1/True when the
    measurements are created, and then set to 0/False if the measurements are amended.
    """
    amendedMeasurementsId = Column(
        "amended_measurements_id", Integer, ForeignKey("physical_measurements.physical_measurements_id")
    )
    """The ID that these measurements are an amendment of (points from new to old)"""
    logPositionId = Column("log_position_id", Integer, ForeignKey("log_position.log_position_id"), nullable=False)
    createdSiteId = Column("created_site_id", Integer, ForeignKey("site.site_id"))
    """The site that created the physical measurements."""
    createdUsername = Column("created_username", String(255))
    """The username / email of the HealthPro user that created the physical measurements."""
    finalizedSiteId = Column("finalized_site_id", Integer, ForeignKey("site.site_id"))
    """The site that finalized the physical measurements."""
    finalizedUsername = Column("finalized_username", String(255))
    """The username / email of the HealthPro user that finalized the physical measurements."""
    logPosition = relationship("LogPosition")
    finalized = Column("finalized", UTCDateTime)
    """The time at which the physical measurements were finalized"""
    status = Column("status", Enum(PhysicalMeasurementsStatus))
    """status of the physical measurements data (restored/amended measurements will be UNSET)"""
    cancelledUsername = Column("cancelled_username", String(255))
    cancelledSiteId = Column("cancelled_site_id", Integer, ForeignKey("site.site_id"))
    cancelledTime = Column("cancelled_time", UTCDateTime)
    """The datetime at which the physical measurements were cancelled"""
    reason = Column("reason", UnicodeText)
    """If measurements are edited or cancelled, user notes to detail change"""
    measurements = relationship("Measurement", cascade="all, delete-orphan")
    origin = Column("origin", String(255))
    collectType = Column("collect_type", Enum(PhysicalMeasurementsCollectType),
                         default=PhysicalMeasurementsCollectType.UNSET)
    originMeasurementUnit = Column("origin_measurement_unit", Enum(OriginMeasurementUnit),
                                   default=OriginMeasurementUnit.UNSET)
    questionnaireResponseId = Column("questionnaire_response_id", Integer,
                                     ForeignKey("questionnaire_response.questionnaire_response_id"), nullable=True)
    resource = Column("resource", JSON, nullable=True)
    """Original resource value; whole payload request that was sent"""


class Measurement(Base):
    """An individual measurement; child of PhysicalMeasurements."""

    __tablename__ = "measurement"
    # Note: measurementId will be physicalMeasurementsId * 100 + an index. (This way we don't have to
    # generate N random unique IDs.)
    measurementId = Column("measurement_id", BIGINT, primary_key=True, autoincrement=False)
    """A unique identifier for each Measurement (definition from CDR)"""
    physicalMeasurementsId = Column(
        "physical_measurements_id",
        Integer,
        ForeignKey("physical_measurements.physical_measurements_id"),
        nullable=False,
    )
    codeSystem = Column("code_system", String(255), nullable=False)
    codeValue = Column("code_value", String(255), nullable=False)
    measurementTime = Column("measurement_time", UTCDateTime, nullable=False)
    bodySiteCodeSystem = Column("body_site_code_system", String(255))
    bodySiteCodeValue = Column("body_site_code_value", String(255))
    valueString = Column("value_string", Text)
    valueDecimal = Column("value_decimal", Float)
    valueUnit = Column("value_unit", String(255))
    valueCodeSystem = Column("value_code_system", String(255))
    valueCodeValue = Column("value_code_value", String(255))
    valueCodeDescription = Column("value_code_description", String(512))
    valueDateTime = Column("value_datetime", UTCDateTime)
    parentId = Column("parent_id", BIGINT, ForeignKey("measurement.measurement_id"))
    qualifierId = Column("qualifier_id", BIGINT, ForeignKey("measurement.measurement_id"))
    measurements = relationship("Measurement", cascade="all, delete-orphan", foreign_keys=[parentId])
    qualifiers = relationship(
        "Measurement",
        secondary=measurement_to_qualifier,
        primaryjoin=measurementId == measurement_to_qualifier.c.measurement_id,
        secondaryjoin=measurementId == measurement_to_qualifier.c.qualifier_id,
    )
