from model.base import Base
from model.utils import UTCDateTime
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, Date, BLOB, ForeignKey, String, Boolean
from sqlalchemy import ForeignKeyConstraint, Float, Text


class QuestionnaireResponse(Base):
  """"A response to a questionnaire for a participant. Contains answers to questions found in the
  questionnaire."""
  __tablename__ = 'questionnaire_response'
  questionnaireResponseId = Column('questionnaire_response_id', Integer, primary_key=True,
                                   autoincrement=False)
  questionnaireId = Column('questionnaire_id', Integer, nullable=False)
  questionnaireVersion = Column('questionnaire_version', Integer, nullable=False)
  participantId = Column('participant_id', Integer, ForeignKey('participant.participant_id'),
                         nullable=False)
  created = Column('created', UTCDateTime, nullable=False)
  resource = Column('resource', BLOB, nullable=False)
  answers = relationship('QuestionnaireResponseAnswer', cascade='all, delete-orphan')
  __table_args__ = (
    ForeignKeyConstraint(['questionnaire_id', 'questionnaire_version'],
                         ['questionnaire_history.questionnaire_id',
                          'questionnaire_history.version']),
  )


class QuestionnaireResponseAnswer(Base):
  """An answer found in a questionnaire response. Note that there could be multiple answers to
  the same question, if the questionnaire allows for multiple answers.

  An answer is given to a particular question which has a particular concept code. The answer is
  the current answer for a participant from the time period between its parent response's creation
  field and the endTime field (or now, if endTime is not set.)

  When an answer is given by a participant in a questionnaire response, the endTime of any previous
  answers to questions with the same concept codes that don't have endTime set yet should have
  endTime set to the current time.
  """
  # This is the maximum # bytes that can be stored in a MySQL TEXT field, which
  # our field valueString should resolve to.
  VALUE_STRING_MAXLEN = 65535

  __tablename__ = 'questionnaire_response_answer'
  questionnaireResponseAnswerId = Column('questionnaire_response_answer_id', Integer,
                                         primary_key=True)
  questionnaireResponseId = Column('questionnaire_response_id', Integer,
      ForeignKey('questionnaire_response.questionnaire_response_id'), nullable=False)
  questionId = Column('question_id', Integer,
                      ForeignKey('questionnaire_question.questionnaire_question_id'),
                      nullable=False)
  # The time at which this answer was replaced by another answer. Not set if this answer is the
  # latest answer to the question.
  endTime = Column('end_time', UTCDateTime)
  valueSystem = Column('value_system', String(50))
  valueCodeId = Column('value_code_id', Integer, ForeignKey('code.code_id'))
  valueBoolean = Column('value_boolean', Boolean)
  valueDecimal = Column('value_decimal', Float)
  valueInteger = Column('value_integer', Integer)
  valueString = Column('value_string', Text)
  valueDate = Column('value_date', Date)
  valueDateTime = Column('value_datetime', UTCDateTime)
  valueUri = Column('value_uri', String(1024))
