from sqlalchemy import BigInteger, Column, DateTime, Index, String, SmallInteger
from sqlalchemy.dialects.mysql import DECIMAL, TINYINT

from rdr_service.model.base import Base


class QuestionnaireVibrentForms(Base):
    __tablename__ = 'questionnaire_vibrent_forms'
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    questionnaire_id = Column(BigInteger)
    version = Column(BigInteger)
    vibrent_form_id = Column(String(200))
    __table_args__ = (Index('idx_questionnaire_vibrent_forms_qid_version', questionnaire_id, version), )


class QuestionnaireResponsesByModule(Base):
    __tablename__ = 'questionnaire_responses_by_module'
    id = Column(BigInteger, autoincrement=True, primary_key=True)
    participant_id = Column(BigInteger)
    authored = Column(DateTime)
    survey = Column(String(200))
    __table_args__ = (Index('idx_questionnaire_responses_by_module_pid_survey', participant_id, survey), )


class SrcClean(Base):
    __tablename__ = 'src_clean'
    id = Column(BigInteger, autoincrement=True, primary_key=True)  # TODO: make sure this doesn't get to the export
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
