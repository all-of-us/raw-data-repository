import clock

from model.base import Base
from sqlalchemy import Column, Integer, DateTime, BLOB

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

