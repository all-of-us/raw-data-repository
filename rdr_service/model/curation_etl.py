from sqlalchemy import Column, ForeignKey, Integer, String, Date, event
from sqlalchemy.orm import relationship
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, UTCDateTime6
from rdr_service.participant_enums import CdrEtlSurveyStatus, CdrEtlCodeType


class CdrEtlRunHistory(Base):
    __tablename__ = "cdr_etl_run_history"

    CdrEtlSurveyHistory = relationship("CdrEtlSurveyHistory", cascade="all, delete-orphan")

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""
    startTime = Column("start_time", UTCDateTime6, nullable=False)
    """The start time of the curation ETL"""
    endTime = Column("end_time", UTCDateTime6, nullable=True)
    """The end time of the curation ETL"""
    vocabularyPath = Column("vocabulary_path", String(256), nullable=True)
    """The curation ETL vocabulary path"""
    cutoffDate = Column("cut_off_date", Date, nullable=True)
    """The cut off date of this ETL run"""


class CdrEtlSurveyHistory(Base):
    __tablename__ = "cdr_etl_survey_history"

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""
    etlRunId = Column("etl_run_id", Integer, ForeignKey("cdr_etl_run_history.id"), nullable=False)
    """The ID of the ETL run, foreign key of cdr_etl_run_history.id"""
    codeId = Column("code_id", Integer, ForeignKey("code.code_id"), nullable=True)
    """code id"""
    codeValue = Column("code_value", String(80), nullable=True)
    """code value"""
    codeType = Column("code_type", Enum(CdrEtlCodeType), nullable=True)
    """code type: module or question or answer"""
    status = Column("status", Enum(CdrEtlSurveyStatus), nullable=True)
    """survey code status in this ETL run"""


class CdrExcludedCode(Base):
    __tablename__ = "cdr_excluded_code"

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""
    codeId = Column("code_id", Integer, ForeignKey("code.code_id"), nullable=True)
    """excluded code id"""
    codeValue = Column("code_value", String(80), nullable=True)
    """excluded code value"""
    codeType = Column("code_type", Enum(CdrEtlCodeType), nullable=True)
    """code type: module or question or answer"""


event.listen(CdrEtlRunHistory, "before_insert", model_insert_listener)
event.listen(CdrEtlRunHistory, "before_update", model_update_listener)
event.listen(CdrEtlSurveyHistory, "before_insert", model_insert_listener)
event.listen(CdrEtlSurveyHistory, "before_update", model_update_listener)
event.listen(CdrExcludedCode, "before_insert", model_insert_listener)
event.listen(CdrExcludedCode, "before_update", model_update_listener)
