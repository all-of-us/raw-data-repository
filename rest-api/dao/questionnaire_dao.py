import clock
import config
import json

import fhirclient.models.questionnaire
from sqlalchemy.orm import subqueryload
from werkzeug.exceptions import BadRequest

from dao.base_dao import BaseDao, UpdatableDao
from code_constants import PPI_EXTRA_SYSTEM
from model.code import CodeType
from model.questionnaire import Questionnaire, QuestionnaireHistory, QuestionnaireConcept
from model.questionnaire import QuestionnaireQuestion


class QuestionnaireDao(UpdatableDao):

  def __init__(self):
    super(QuestionnaireDao, self).__init__(Questionnaire)

  def get_id(self, obj):
    return obj.questionnaireId

  def get_with_children(self, questionnaireId):
    with self.session() as session:
      query = session.query(Questionnaire).options(subqueryload(Questionnaire.concepts),
                                                   subqueryload(Questionnaire.questions))
      return query.get(questionnaireId)

  def get_latest_questionnaire_with_concept(self, codeId):
    """Find the questionnaire most recently modified that has the specified concept code."""
    with self.session() as session:
      return (session.query(Questionnaire)
                .join(Questionnaire.concepts)
                .filter(QuestionnaireConcept.codeId == codeId)
                .order_by(Questionnaire.lastModified.desc())
                .options(subqueryload(Questionnaire.questions))
                .first())

  def _make_history(self, questionnaire, concepts, questions):
    history = QuestionnaireHistory()
    history.fromdict(questionnaire.asdict(), allow_pk=True)
    for concept in concepts:
      new_concept = QuestionnaireConcept()
      new_concept.fromdict(concept.asdict())
      new_concept.questionnaireId = questionnaire.questionnaireId
      new_concept.questionnaireVersion = questionnaire.version
      history.concepts.append(new_concept)
    for question in questions:
      new_question = QuestionnaireQuestion()
      new_question.fromdict(question.asdict())
      new_question.questionnaireId = questionnaire.questionnaireId
      new_question.questionnaireVersion = questionnaire.version
      history.questions.append(new_question)

    return history

  def insert_with_session(self, session, questionnaire):
    questionnaire.created = clock.CLOCK.now()
    questionnaire.lastModified = clock.CLOCK.now()
    questionnaire.version = 1
    # SQLAlchemy emits warnings unnecessarily when these collections aren't empty.
    # We don't want these to be cascaded now anyway, so point them at nothing, but save
    # the concepts and questions for use in history.
    concepts = list(questionnaire.concepts)
    questions = list(questionnaire.questions)
    questionnaire.concepts = []
    questionnaire.questions = []

    super(QuestionnaireDao, self).insert_with_session(session, questionnaire)
    # This is needed to assign an ID to the questionnaire, as the client doesn't need to provide
    # one.
    session.flush()

    # Set the ID in the resource JSON
    resource_json = json.loads(questionnaire.resource)
    resource_json['id'] = str(questionnaire.questionnaireId)
    resource_json['version'] = str(questionnaire.version)
    questionnaire.resource = json.dumps(resource_json)

    history = self._make_history(questionnaire, concepts, questions)
    history.questionnaireId = questionnaire.questionnaireId
    QuestionnaireHistoryDao().insert_with_session(session, history)
    return questionnaire

  def _do_update(self, session, obj, existing_obj):
    # If the provider link changes, update the HPO ID on the participant and its summary.
    obj.lastModified = clock.CLOCK.now()
    obj.version = existing_obj.version + 1
    obj.created = existing_obj.created
    resource_json = json.loads(obj.resource)
    resource_json['id'] = str(obj.questionnaireId)
    resource_json['version'] = str(obj.version)
    obj.resource = json.dumps(resource_json)
    super(QuestionnaireDao, self)._do_update(session, obj, existing_obj)

  def update_with_session(self, session, questionnaire):
    super(QuestionnaireDao, self).update_with_session(session, questionnaire)
    QuestionnaireHistoryDao().insert_with_session(session,
                                                  self._make_history(questionnaire,
                                                                     questionnaire.concepts,
                                                                     questionnaire.questions))

  @classmethod
  def from_client_json(cls,
                       resource_json,
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
    code_map, concepts, questions = cls._extract_codes(fhir_q.group)

    from dao.code_dao import CodeDao
    # Get or insert codes, and retrieve their database IDs.
    add_codes_if_missing = config.get_add_codes_if_missing()
    code_id_map = CodeDao().get_or_add_codes(code_map, add_codes_if_missing=add_codes_if_missing)

    # Now add the child objects, using the IDs in code_id_map
    cls._add_concepts(q, code_id_map, concepts)
    cls._add_questions(q, code_id_map, questions)

    return q

  @classmethod
  def _add_concepts(cls, q, code_id_map, concepts):
    for system, code in concepts:
      q.concepts.append(
          QuestionnaireConcept(
              questionnaireId=q.questionnaireId,
              questionnaireVersion=q.version,
              codeId=code_id_map.get((system, code))))

  @classmethod
  def _add_questions(cls, q, code_id_map, questions):
    for system, code, linkId, repeats in questions:
      q.questions.append(
          QuestionnaireQuestion(
              questionnaireId=q.questionnaireId,
              questionnaireVersion=q.version,
              linkId=linkId,
              codeId=code_id_map.get((system, code)),
              repeats=repeats if repeats else False))

  @classmethod
  def _extract_codes(cls, group):
    code_map = {}
    concepts = []
    questions = []
    if group.concept:
      for concept in group.concept:
        if concept.system and concept.code and concept.system != PPI_EXTRA_SYSTEM:
          code_map[(concept.system, concept.code)] = (concept.display,
                                                      CodeType.MODULE, None)
          concepts.append((concept.system, concept.code))
    cls._populate_questions(group, code_map, questions)
    return (code_map, concepts, questions)

  @classmethod
  def _populate_questions(cls, group, code_map, questions):
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
            cls._populate_questions(sub_group, code_map, questions)
    if group.group:
      for sub_group in group.group:
        cls._populate_questions(sub_group, code_map, questions)


class QuestionnaireHistoryDao(BaseDao):
  """Maintains version history for questionnaires.

  All previous versions of a questionnaire are maintained (with the same questionnaireId value and
  a new version value for each update.)

  Old versions of questionnaires and their questions can still be referenced by questionnaire
  responses, and are used when generating metrics / participant summaries, and in general
  determining what answers participants gave to questions.

  Concepts and questions live under a QuestionnaireHistory entry, such that when the questionnaire
  gets updated new concepts and questions are created and existing ones are left as they were.

  Do not use this DAO for write operations directly; instead use QuestionnaireDao.
  """
  def __init__(self):
    super(QuestionnaireHistoryDao, self).__init__(QuestionnaireHistory)

  def get_id(self, obj):
    return [obj.questionnaireId, obj.version]

  def get_with_children_with_session(self, session, questionnaireIdAndVersion):
    query = session.query(QuestionnaireHistory) \
        .options(subqueryload(QuestionnaireHistory.concepts),
                 subqueryload(QuestionnaireHistory.questions))
    return query.get(questionnaireIdAndVersion)

  def get_with_children(self, questionnaireIdAndVersion):
    with self.session() as session:
      return self.get_with_children_with_session(session, questionnaireIdAndVersion)


class QuestionnaireConceptDao(BaseDao):

  def __init__(self):
    super(QuestionnaireConceptDao, self).__init__(QuestionnaireConcept)

  def get_id(self, obj):
    return obj.questionnaireConceptId


class QuestionnaireQuestionDao(BaseDao):

  def __init__(self):
    super(QuestionnaireQuestionDao, self).__init__(QuestionnaireQuestion)

  def get_id(self, obj):
    return obj.questionnaireQuestionId

  def get_all_with_session(self, session, ids):
    if not ids:
      return []
    return (session.query(QuestionnaireQuestion)
            .filter(QuestionnaireQuestion.questionnaireQuestionId.in_(ids)).all())
