from sqlalchemy import BigInteger, Column, DateTime, Float, String, SmallInteger
from sqlalchemy.dialects.mysql import TINYINT

from rdr_service.model.base import Base

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
    value_number = Column(Float(precision=20, decimal_return_scale=6))
    value_boolean = Column(TINYINT)
    value_date = Column(DateTime)
    value_string = Column(String(1024))
    questionnaire_response_id = Column(BigInteger)
    unit_id = Column(String(50))
    filter = Column(SmallInteger)
