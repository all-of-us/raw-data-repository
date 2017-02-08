import clock

from model.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, Date, DateTime, BLOB, ForeignKey, String, ForeignKeyConstraint

class QuestionnaireResponse(Base):
  """Questionnaire response resource"""
  __tablename__  = 'questionnaire_response'
  questionnaireResponseId = Column('questionnaire_response_id', Integer, primary_key=True, 
                                   autoincrement=False)
  questionnaireId = Column('questionnaire_id', Integer, nullable=False)
  questionnaireVersion = Column('questionnaire_version', Integer, nullable=False)  
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'), 
                         nullable=False)
  created = Column('created', DateTime, default=clock.CLOCK.now, nullable=False)
  resource = Column('resource', BLOB, nullable=False)
  answers = relationship('QuestionnaireResponseAnswer', cascade='all, delete-orphan')
  __table_args__ = (
    ForeignKeyConstraint(['questionnaire_id', 'questionnaire_version'], 
                         ['questionnaire_history.questionnaire_id', 
                          'questionnaire_history.version']),
  )

class QuestionnaireResponseAnswer(Base):
  """An answer found in a questionnaire response. Note that there could be multiple answers to 
  the same question, if the questionnaire allows for multiple answers."""
  __tablename__  = 'questionnaire_response_answer'
  questionnaireResponseAnswerId = Column('questionnaire_response_answer_id', Integer, primary_key=True,
                                        autoincrement=False)
  questionnaireResponseId = Column('questionnaire_response_id', Integer, 
                                   ForeignKey('questionnaire_response.questionnaire_response_id'))
  questionId = Column('question_id', Integer, 
                      ForeignKey('questionnaire_question.questionnaire_question_id'))
  # The time at which this answer was replaced by another answer. Not set if this answer is the
  # latest answer to the question.
  endTime = Column('end_time', DateTime)
  valueSystem = Column('value_system', String(50))
  valueCode = Column('value_code', String(20))
  valueDecimal = Column('value_decimal', Integer)
  # Is this big enough?
  valueString = Column('value_string', String(1024))
  valueDate = Column('value_date', Date)
