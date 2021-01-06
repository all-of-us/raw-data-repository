from sqlalchemy import Column, event, ForeignKey, Integer, String

from rdr_service.model.base import Base, model_insert_listener
from rdr_service.model.code import Code
from rdr_service.model.utils import UTCDateTime


class Survey(Base):
    __tablename__ = 'survey'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    codeId = Column('code_id', Integer, ForeignKey(Code.codeId))

    importTime = Column('import_time', UTCDateTime)
    replacedTime = Column('replaced_time', UTCDateTime)

    redcapProjectId = Column('redcap_project_id', Integer)
    redcapProjectTitle = Column('redcap_project_title', String(200))


class SurveyQuestion(Base):
    __tablename__ = 'survey_question'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    codeId = Column('code_id', Integer, ForeignKey(Code.codeId))
    surveyId = Column('survey_id', Integer, ForeignKey(Survey.id))

    questionType = Column('question_type', String(200))
    validation = Column(String(200))
    display = Column(String(200))


class SurveyQuestionOption(Base):
    __tablename__ = 'survey_question_option'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    codeId = Column('code_id', Integer, ForeignKey(Code.codeId))
    questionId = Column('question_id', Integer, ForeignKey(SurveyQuestion.id))

    display = Column(String(200))


event.listen(Survey, 'before_insert', model_insert_listener)
