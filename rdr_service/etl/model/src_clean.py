from sqlalchemy import BigInteger, Column, DateTime, Index, String, SmallInteger, Integer
from sqlalchemy.dialects.mysql import DECIMAL, TINYINT

from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime


class QuestionnaireAnswersByModule(Base):
    __tablename__ = 'questionnaire_answers_by_module'
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    participant_id = Column(BigInteger)
    authored = Column(DateTime)
    created = Column(DateTime)
    survey = Column(String(200))
    response_id = Column(BigInteger)
    question_code_id = Column(BigInteger)
    __table_args__ = (Index(
        'idx_participant_questionnaire_answers_by_module_and_code',
        participant_id,
        survey,
        question_code_id
    ), )


class SrcClean(Base):
    __tablename__ = 'src_clean'
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    participant_id = Column(BigInteger)
    research_id = Column(BigInteger)
    survey_name = Column(String(200))
    date_of_survey = Column(DateTime)
    question_ppi_code = Column(String(200))
    question_code_id = Column(BigInteger)
    value_ppi_code = Column(String(200))
    topic_value = Column(String(200))
    value_code_id = Column(BigInteger)
    value_number = Column(DECIMAL(precision=20, scale=6))
    value_boolean = Column(TINYINT)
    value_date = Column(DateTime)
    value_string = Column(String(1024))
    questionnaire_response_id = Column(BigInteger)
    unit_id = Column(String(50))
    filter = Column(SmallInteger)
    __table_args__ = (Index('idx_src_clean_participant_id', participant_id), )


class TemporaryQuestionnaireResponse(Base):
    """"A temporary table to store duplicated QuestionnaireResponses."""

    __tablename__ = "tmp_questionnaire_response"
    questionnaireResponseId = Column("questionnaire_response_id", Integer, primary_key=True, autoincrement=False)
    participantId = Column("participant_id", Integer, nullable=True)
    questionnaireId = Column("questionnaire_id", Integer, nullable=True)
    created = Column("created", UTCDateTime, nullable=True)
    authored = Column("authored", UTCDateTime, nullable=True)
    identifier = Column("identifier", String(50), nullable=True)
    duplicate = Column("duplicate", Integer, nullable=True)
    removed = Column("removed", Integer, nullable=True)


TemporaryQuestionnaireResponse.__table__.schema = 'cdm'
QuestionnaireAnswersByModule.__table__.schema = 'cdm'
SrcClean.__table__.schema = 'cdm'
