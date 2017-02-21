import clock

from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, DateTime, BLOB, String, ForeignKeyConstraint, Index
from sqlalchemy import UniqueConstraint

class QuestionnaireBase(object):
  """Mixin containing columns for Questionnaire and QuestionnaireHistory"""
  questionnaireId = Column('questionnaire_id', Integer, primary_key=True)
  # Incrementing version, starts at 1 and is incremented on each update.
  version = Column('version', Integer, nullable=False)      
  created = Column('created', DateTime, nullable=False)
  lastModified = Column('last_modified', DateTime, nullable=False)
  resource = Column('resource', BLOB, nullable=False)  
  

class Questionnaire(QuestionnaireBase, Base):  
  __tablename__ = 'questionnaire'
  concepts = relationship('QuestionnaireConcept', cascade="expunge", cascade_backrefs=False,
                          primaryjoin='Questionnaire.questionnaireId==' + \
                            'foreign(QuestionnaireConcept.questionnaireId)')
  questions = relationship('QuestionnaireQuestion', cascade="expunge", cascade_backrefs=False,
                           primaryjoin='Questionnaire.questionnaireId==' + \
                            'foreign(QuestionnaireQuestion.questionnaireId)')

class QuestionnaireHistory(QuestionnaireBase, Base):  
  __tablename__ = 'questionnaire_history'
  version = Column('version', Integer, primary_key=True)
  concepts = relationship('QuestionnaireConcept', cascade='all, delete-orphan')
  questions = relationship('QuestionnaireQuestion', cascade='all, delete-orphan')  

class QuestionnaireConcept(Base):
  """Concepts for the questionnaire as a whole. These should be copied whenever a new version of 
  a questionnaire is created."""
  __tablename__ = 'questionnaire_concept'
  questionnaireConceptId = Column('questionnaire_concept_id', Integer, primary_key=True)
  questionnaireId = Column('questionnaire_id', Integer)
  questionnaireVersion = Column('questionnaire_version', Integer)
  conceptSystem = Column('concept_system', String(50))
  conceptCode = Column('concept_code', String(20))
  __table_args__ = (
    ForeignKeyConstraint(['questionnaire_id', 'questionnaire_version'], 
                         ['questionnaire_history.questionnaire_id', 'questionnaire_history.version']),
    UniqueConstraint('questionnaire_id', 'questionnaire_version', 'concept_system', 'concept_code')
  )
Index('questionnaire_concept_system_code', QuestionnaireConcept.conceptSystem, 
      QuestionnaireConcept.conceptCode)

class QuestionnaireQuestion(Base):
  """A question in a questionnaire. These should be copied whenever a new version of a 
  questionnaire is created."""
  __tablename__ = 'questionnaire_question'
  questionnaireQuestionId = Column('questionnaire_question_id', Integer, primary_key=True)
  questionnaireId = Column('questionnaire_id', Integer)
  questionnaireVersion = Column('questionnaire_version', Integer)
  linkId = Column('link_id', String(20))
  conceptSystem = Column('concept_system', String(50))
  conceptCode = Column('concept_code', String(20))
  # Should we also include valid answers here?  
  __table_args__ = (
    ForeignKeyConstraint(['questionnaire_id', 'questionnaire_version'], 
                         ['questionnaire_history.questionnaire_id', 'questionnaire_history.version']),
    UniqueConstraint('questionnaire_id', 'questionnaire_version', 'link_id')
  )
  
Index('questionnaire_question_system_code', QuestionnaireQuestion.conceptSystem, 
      QuestionnaireQuestion.conceptCode)
