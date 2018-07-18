from model.base import Base
from model.utils import UTCDateTime, Enum
from participant_enums import QuestionnaireDefinitionStatus
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, BLOB, String, ForeignKeyConstraint, Boolean
from sqlalchemy import UniqueConstraint, ForeignKey


class QuestionnaireBase(object):
  """Mixin containing columns for Questionnaire and QuestionnaireHistory"""
  questionnaireId = Column('questionnaire_id', Integer, primary_key=True)
  # Incrementing version, starts at 1 and is incremented on each update.
  version = Column('version', Integer, nullable=False)
  created = Column('created', UTCDateTime, nullable=False)
  lastModified = Column('last_modified', UTCDateTime, nullable=False)
  # The JSON representation of the questionnaire provided by the client.
  # Concepts and questions can be be parsed out of this for use in querying.
  resource = Column('resource', BLOB, nullable=False)
  status = Column('status', Enum(QuestionnaireDefinitionStatus),
                   default=QuestionnaireDefinitionStatus.VALID)

  def asdict_with_children(self):
    return self.asdict(follow={'concepts': {}, 'questions': {}})


class Questionnaire(QuestionnaireBase, Base):
  """A questionnaire containing questions to pose to participants."""
  __tablename__ = 'questionnaire'
  concepts = relationship('QuestionnaireConcept', cascade='expunge', cascade_backrefs=False,
                          primaryjoin='Questionnaire.questionnaireId==' + \
                            'foreign(QuestionnaireConcept.questionnaireId)')
  questions = relationship('QuestionnaireQuestion', cascade='expunge', cascade_backrefs=False,
                           primaryjoin='Questionnaire.questionnaireId==' + \
                            'foreign(QuestionnaireQuestion.questionnaireId)')


class QuestionnaireHistory(QuestionnaireBase, Base):
  __tablename__ = 'questionnaire_history'
  version = Column('version', Integer, primary_key=True)
  concepts = relationship('QuestionnaireConcept', cascade='all, delete-orphan')
  questions = relationship(
      'QuestionnaireQuestion', cascade='all, delete-orphan')


class QuestionnaireConcept(Base):
  """Concepts for the questionnaire as a whole.

  These should be copied whenever a new version of
  a questionnaire is created.
  """
  __tablename__ = 'questionnaire_concept'
  questionnaireConceptId = Column(
      'questionnaire_concept_id', Integer, primary_key=True)
  questionnaireId = Column('questionnaire_id', Integer, nullable=False)
  questionnaireVersion = Column(
      'questionnaire_version', Integer, nullable=False)
  codeId = Column(
      'code_id', Integer, ForeignKey('code.code_id'), nullable=False)
  __table_args__ = (ForeignKeyConstraint([
      'questionnaire_id', 'questionnaire_version'
  ], [
      'questionnaire_history.questionnaire_id', 'questionnaire_history.version'
  ]), UniqueConstraint('questionnaire_id', 'questionnaire_version', 'code_id'))


class QuestionnaireQuestion(Base):
  """A question in a questionnaire.

  These should be copied whenever a new version of a
  questionnaire is created.

  Each question has a concept system and code defining what the question is
  about. Questions on
  different questionnaires can share the same concept code, but concept code is
  unique within a
  given questionnaire.
  """
  __tablename__ = 'questionnaire_question'
  questionnaireQuestionId = Column(
      'questionnaire_question_id', Integer, primary_key=True)
  questionnaireId = Column('questionnaire_id', Integer)
  questionnaireVersion = Column('questionnaire_version', Integer)
  linkId = Column('link_id', String(20))
  codeId = Column(
      'code_id', Integer, ForeignKey('code.code_id'), nullable=False)
  repeats = Column('repeats', Boolean, nullable=False)
  __table_args__ = (ForeignKeyConstraint([
      'questionnaire_id', 'questionnaire_version'
  ], [
      'questionnaire_history.questionnaire_id', 'questionnaire_history.version'
  ]), UniqueConstraint('questionnaire_id', 'questionnaire_version', 'link_id'))
