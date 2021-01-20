from sqlalchemy import Column, event, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from typing import List

from rdr_service.model.base import Base, model_insert_listener
from rdr_service.model.code import Code
from rdr_service.model.utils import UTCDateTime


class Survey(Base):
    __tablename__ = 'survey'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    codeId = Column('code_id', Integer, ForeignKey(Code.codeId))
    code = relationship(Code)

    importTime = Column('import_time', UTCDateTime)
    replacedTime = Column('replaced_time', UTCDateTime)

    redcapProjectId = Column('redcap_project_id', Integer)
    redcapProjectTitle = Column('redcap_project_title', String(200))

    questions: List['SurveyQuestion'] = relationship('SurveyQuestion', back_populates='survey')


class SurveyQuestion(Base):
    __tablename__ = 'survey_question'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    surveyId = Column('survey_id', Integer, ForeignKey(Survey.id))
    survey = relationship(Survey, back_populates='questions')

    codeId = Column('code_id', Integer, ForeignKey(Code.codeId))
    code = relationship(Code)

    questionType = Column('question_type', String(200))
    validation = Column(String(200))
    display = Column(String(200))

    options: List['SurveyQuestionOption'] = relationship('SurveyQuestionOption', back_populates='question')


class SurveyQuestionOption(Base):
    __tablename__ = 'survey_question_option'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)

    questionId = Column('question_id', Integer, ForeignKey(SurveyQuestion.id))
    question = relationship(SurveyQuestion, back_populates='options')

    codeId = Column('code_id', Integer, ForeignKey(Code.codeId))
    code = relationship(Code)

    display = Column(String(200))


event.listen(Survey, 'before_insert', model_insert_listener)
