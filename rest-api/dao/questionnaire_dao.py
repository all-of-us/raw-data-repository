import clock

from dao.base_dao import BaseDao
from model.questionnaire import Questionnaire, QuestionnaireHistory, QuestionnaireConcept, QuestionnaireQuestion
from sqlalchemy.orm import subqueryload

class QuestionnaireDao(BaseDao):

  def __init__(self):
    super(QuestionnaireDao, self).__init__(Questionnaire)

  def get_id(self, obj):
    return obj.questionnaireId

  def get_with_children(self, questionnaireId):
    with self.session() as session:
      query = session.query(Questionnaire).options(subqueryload(Questionnaire.concepts),
                                                   subqueryload(Questionnaire.questions))
      return query.get(questionnaireId)

  def _make_history(self, questionnaire):
    history = QuestionnaireHistory()
    history.fromdict(questionnaire.asdict(), allow_pk=True)
    history.concepts.extend(questionnaire.concepts)
    history.questions.extend(questionnaire.questions)

    return history

  def insert_with_session(self, session, questionnaire):
    questionnaire.created = clock.CLOCK.now()
    questionnaire.lastModified = clock.CLOCK.now()
    questionnaire.version = 1
    super(QuestionnaireDao, self).insert_with_session(session, questionnaire)
    # This is needed to assign an ID to the questionnaire, as the client doesn't need to provide
    # one.
    session.flush()
    QuestionnaireHistoryDao().insert_with_session(session, self._make_history(questionnaire))

  def _do_update(self, session, obj, existing_obj):
    # If the provider link changes, update the HPO ID on the participant and its summary.
    obj.lastModified = clock.CLOCK.now()
    obj.version = existing_obj.version + 1
    super(QuestionnaireDao, self)._do_update(session, obj, existing_obj)

  def update_with_session(self, session, questionnaire, expected_version_id=None):
    super(QuestionnaireDao, self).update_with_session(session, questionnaire, expected_version_id)
    QuestionnaireHistoryDao().insert_with_session(session, self._make_history(questionnaire))

class QuestionnaireHistoryDao(BaseDao):

  def __init__(self):
    super(QuestionnaireHistoryDao, self).__init__(QuestionnaireHistory)

  def get_id(self, obj):
    return [obj.questionnaireId, obj.version]

  def get_with_children(self, questionnaireId):
    with self.session() as session:
      query = session.query(QuestionnaireHistory) \
          .options(subqueryload(QuestionnaireHistory.concepts),
                   subqueryload(QuestionnaireHistory.questions))
      return query.get(questionnaireId)

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
