import clock

from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, DateTime, BLOB, String, ForeignKeyConstraint
from sqlalchemy import UniqueConstraint

class QuestionnaireBase(object):
  """Mixin containing columns for Questionnaire and QuestionnaireHistory"""
  id = Column('id', Integer, primary_key=True)
  # Incrementing version, starts at 1 and is incremented on each update.
  version = Column('version', Integer, nullable=False)      
  created = Column('created', DateTime, default=clock.CLOCK.now, nullable=False)
  lastModified = Column('last_modified', DateTime, default=clock.CLOCK.now, 
                        onupdate=clock.CLOCK.now, nullable=False)  
  resource = Column('resource', BLOB, nullable=False)  
  

class Questionnaire(QuestionnaireBase, Base):
  """Questionnaire resource definition"""
  __tablename__ = 'questionnaire'

class QuestionnaireHistory(QuestionnaireBase, Base):
  """History table for questionnaires"""
  __tablename__ = 'questionnaire_history'
  version = Column('version', Integer, primary_key=True)
  concepts = relationship('QuestionnaireConcept', cascade='all, delete-orphan')
  questions = relationship('QuestionnaireQuestion', cascade='all, delete-orphan')  

class QuestionnaireConcept(Base):
  """Concepts for the questionnaire as a whole"""
  __tablename__ = 'questionnaire_concept'
  id = Column('id', Integer, primary_key=True)
  questionnaireId = Column('questionnaire_id', Integer)
  questionnaireVersion = Column('questionnaire_version', Integer)
  conceptSystem = Column('concept_system', String(50))
  conceptCode = Column('concept_code', String(20))
  __table_args__ = (
    ForeignKeyConstraint(['questionnaire_id', 'questionnaire_version'], 
                         ['questionnaire_history.id', 'questionnaire_history.version']),
    UniqueConstraint('questionnaire_id', 'questionnaire_version', 'concept_system', 'concept_code')
  )

class QuestionnaireQuestion(Base):
  """A question in a questionnaire. These should be copied whenever a new version of a 
  questionnaire is created."""
  __tablename__ = 'questionnaire_question'
  id = Column('id', Integer, primary_key=True)
  questionnaireId = Column('questionnaire_id', Integer)
  questionnaireVersion = Column('questionnaire_version', Integer)
  linkId = Column('link_id', String(20))
  # Is this big enough?
  text = Column('text', String(1024), nullable=False)
  concept_system = Column('concept_system', String(50))
  concept_code = Column('concept_code', String(20))
  concept_display = Column('concept_display', String(80))  
  # Should we also include valid answers here?  
  __table_args__ = (
    ForeignKeyConstraint(['questionnaire_id', 'questionnaire_version'], 
                         ['questionnaire_history.id', 'questionnaire_history.version']),
    UniqueConstraint('questionnaire_id', 'questionnaire_version', 'link_id')
  )