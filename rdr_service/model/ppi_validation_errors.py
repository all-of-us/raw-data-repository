from enum import Enum as enum

from sqlalchemy import (
    BigInteger,
    Column,
    Enum,
    ForeignKey,
    Integer,
    String
)
from sqlalchemy.orm import relationship
from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime


class ValidationErrorType(enum):
    INVALID_DATA_TYPE = 1
    INVALID_VALUE = 2
    BRANCHING_ERROR = 3


class PpiValidationErrors(Base):
    __tablename__ = "ppi_validation_errors"

    id = Column(BigInteger, primary_key=True, autoincrement=True, nullable=False)
    created = Column(UTCDateTime, nullable=False)
    modified = Column(UTCDateTime, nullable=True)
    survey_code_value = Column(String(80), nullable=False)
    question_code = Column(String(512), nullable=False)
    error_str = Column(String(512), nullable=False)
    error_type = Column(Enum(ValidationErrorType), nullable=False)

    participant_id = Column(Integer, ForeignKey("participant.participant_id"), nullable=False)
    survey_code_id = Column(Integer, ForeignKey("code.code_id"), nullable=False)
    questionnaire_response_id = Column(Integer, ForeignKey("questionnaire_response.questionnaire_response_id"),
                                       nullable=False)
    questionnaire_response_answer_id = Column(
        Integer,
        ForeignKey("questionnaire_response_answer.questionnaire_response_answer_id"),
        nullable=False
    )
    results_id = Column(BigInteger, ForeignKey('ppi_validation_results.id'))

    result = relationship('PpiValidationResults', foreign_keys=results_id, back_populates='errors')
