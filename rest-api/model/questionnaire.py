import fhirclient.models.questionnaire
import json

from code_constants import PPI_EXTRA_SYSTEM
from model.code import CodeType
from model.base import Base
from model.utils import UTCDateTime
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, BLOB, String, ForeignKeyConstraint, Boolean
from sqlalchemy import UniqueConstraint, ForeignKey
from werkzeug.exceptions import BadRequest


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

  def asdict_with_children(self):
    return self.asdict(follow={'concepts': {}, 'questions': {}})

  def to_client_json(self):
    return json.loads(self.resource)


class Questionnaire(QuestionnaireBase, Base):
  """A questionnaire containing questions to pose to participants."""
  __tablename__ = 'questionnaire'
  concepts = relationship('QuestionnaireConcept', cascade='expunge', cascade_backrefs=False,
                          primaryjoin='Questionnaire.questionnaireId==' + \
                            'foreign(QuestionnaireConcept.questionnaireId)')
  questions = relationship('QuestionnaireQuestion', cascade='expunge', cascade_backrefs=False,
                           primaryjoin='Questionnaire.questionnaireId==' + \
                            'foreign(QuestionnaireQuestion.questionnaireId)')

  @staticmethod
  def from_client_json(resource_json,
                       id_=None,
                       expected_version=None,
                       client_id=None):
    #pylint: disable=unused-argument
    # Parse the questionnaire to make sure it's valid, but preserve the original JSON
    # when saving.
    fhir_q = fhirclient.models.questionnaire.Questionnaire(resource_json)
    if not fhir_q.group:
      raise BadRequest('No top-level group found in questionnaire')

    q = Questionnaire(
        resource=json.dumps(resource_json),
        questionnaireId=id_,
        version=expected_version)
    # Assemble a map of (system, value) -> (display, code_type, parent_id) for passing into CodeDao.
    # Also assemble a list of (system, code) for concepts and (system, code, linkId) for questions,
    # which we'll use later when assembling the child objects.
    code_map, concepts, questions = Questionnaire._extract_codes(fhir_q.group)

    from dao.code_dao import CodeDao
    # Get or insert codes, and retrieve their database IDs.
    code_id_map = CodeDao().get_or_add_codes(code_map)

    # Now add the child objects, using the IDs in code_id_map
    Questionnaire._add_concepts(q, code_id_map, concepts)
    Questionnaire._add_questions(q, code_id_map, questions)

    return q

  @staticmethod
  def _add_concepts(q, code_id_map, concepts):
    for system, code in concepts:
      q.concepts.append(
          QuestionnaireConcept(
              questionnaireId=q.questionnaireId,
              questionnaireVersion=q.version,
              codeId=code_id_map.get((system, code))))

  @staticmethod
  def _add_questions(q, code_id_map, questions):
    for system, code, linkId, repeats in questions:
      q.questions.append(
          QuestionnaireQuestion(
              questionnaireId=q.questionnaireId,
              questionnaireVersion=q.version,
              linkId=linkId,
              codeId=code_id_map.get((system, code)),
              repeats=repeats if repeats else False))

  @staticmethod
  def _extract_codes(group):
    code_map = {}
    concepts = []
    questions = []
    if group.concept:
      for concept in group.concept:
        if concept.system and concept.code and concept.system != PPI_EXTRA_SYSTEM:
          code_map[(concept.system, concept.code)] = (concept.display,
                                                      CodeType.MODULE, None)
          concepts.append((concept.system, concept.code))
    Questionnaire._populate_questions(group, code_map, questions)
    return (code_map, concepts, questions)

  @staticmethod
  def _populate_questions(group, code_map, questions):
    """Recursively populate questions under this group."""
    if group.question:
      for question in group.question:
        # Capture any questions that have a link ID and single concept with a system and code
        if question.linkId and question.concept and len(question.concept) == 1:
          concept = question.concept[0]
          if concept.system and concept.code and concept.system != PPI_EXTRA_SYSTEM:
            code_map[(concept.system, concept.code)] = (concept.display,
                                                        CodeType.QUESTION, None)
            questions.append((concept.system, concept.code, question.linkId,
                              question.repeats))
        if question.group:
          for sub_group in question.group:
            Questionnaire._populate_questions(sub_group, code_map, questions)
    if group.group:
      for sub_group in group.group:
        Questionnaire._populate_questions(sub_group, code_map, questions)


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
