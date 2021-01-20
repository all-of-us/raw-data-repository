from protorpc import messages
from sqlalchemy import Column, event, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from typing import List

from rdr_service.model.base import Base, model_insert_listener
from rdr_service.model.code import Code
from rdr_service.model.utils import EnumZeroBased, UTCDateTime


class Survey(Base):
    __tablename__ = 'survey'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    codeId = Column('code_id', Integer, ForeignKey(Code.codeId))
    code = relationship(Code)

    importTime = Column('import_time', UTCDateTime)
    replacedTime = Column('replaced_time', UTCDateTime)
    # TODO: test replace/update of survey

    redcapProjectId = Column('redcap_project_id', Integer)
    redcapProjectTitle = Column('redcap_project_title', String(1024))

    questions: List['SurveyQuestion'] = relationship('SurveyQuestion', back_populates='survey')


class SurveyQuestionType(messages.Enum):
    """Question types available on Redcap"""
    TEXT = 0
    NOTES = 1
    CALC = 2
    DROPDOWN = 3
    RADIO = 4
    CHECKBOX = 5
    YESNO = 6
    TRUEFALSE = 7
    FILE = 8
    SLIDER = 9


class SurveyQuestion(Base):
    __tablename__ = 'survey_question'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    surveyId = Column('survey_id', Integer, ForeignKey(Survey.id))
    survey = relationship(Survey, back_populates='questions')

    codeId = Column('code_id', Integer, ForeignKey(Code.codeId))
    code = relationship(Code)

    questionType = Column('question_type', EnumZeroBased(SurveyQuestionType))
    validation = Column(String(256))
    display = Column(String(2048))

    options: List['SurveyQuestionOption'] = relationship('SurveyQuestionOption', back_populates='question')


class SurveyQuestionOption(Base):
    __tablename__ = 'survey_question_option'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    questionId = Column('question_id', Integer, ForeignKey(SurveyQuestion.id))
    question = relationship(SurveyQuestion, back_populates='options')

    codeId = Column('code_id', Integer, ForeignKey(Code.codeId))
    code = relationship(Code)

    display = Column(String(2048))


event.listen(Survey, 'before_insert', model_insert_listener)
