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
    for concept in questionnaire.concepts:
      new_concept = QuestionnaireConcept()
      new_concept.fromdict(concept.asdict())
      new_concept.questionnaireId = questionnaire.questionnaireId
      new_concept.questionnaireVersion = questionnaire.version
      history.concepts.append(new_concept)
    for question in questionnaire.questions:
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
    history = self._make_history(questionnaire)
    # SQLAlchemy emits warnings unnecessarily when these collections aren't cleared.
    # We don't want these to be cascaded now anyway, so clear them.
    del questionnaire.concepts[:]
    del questionnaire.questions[:]
    
    super(QuestionnaireDao, self).insert_with_session(session, questionnaire)
    # This is needed to assign an ID to the questionnaire, as the client doesn't need to provide
    # one.
    session.flush()    
    history.questionnaireId = questionnaire.questionnaireId
    QuestionnaireHistoryDao().insert_with_session(session, history)

  def _do_update(self, session, obj, existing_obj):
    # If the provider link changes, update the HPO ID on the participant and its summary.
    obj.lastModified = clock.CLOCK.now()
    obj.version = existing_obj.version + 1
    obj.created = existing_obj.created
    super(QuestionnaireDao, self)._do_update(session, obj, existing_obj)

  def update_with_session(self, session, questionnaire, expected_version=None):
    super(QuestionnaireDao, self).update_with_session(session, questionnaire, expected_version)
    QuestionnaireHistoryDao().insert_with_session(session, self._make_history(questionnaire))
        
class QuestionnaireHistoryDao(BaseDao):
  '''Maintains version history for questionnaires.
  
  All previous versions of a questionnaire are maintained (with the same questionnaireId value and
  a new version value for each update.)
  
  Old versions of questionnaires and their questions can still be referenced by questionnaire 
  responses, and are used when generating metrics / participant summaries, and in general 
  determining what answers participants gave to questions.
  
  Concepts and questions live under a QuestionnaireHistory entry, such that when the questionnaire
  gets updated new concepts and questions are created and existing ones are left as they were.
  
  Do not use this DAO for write operations directly; instead use QuestionnaireDao.
  '''
  def __init__(self):
    super(QuestionnaireHistoryDao, self).__init__(QuestionnaireHistory)

  def get_id(self, obj):
    return [obj.questionnaireId, obj.version]

  def get_with_children(self, questionnaireIdAndVersion):
    with self.session() as session:
      query = session.query(QuestionnaireHistory) \
          .options(subqueryload(QuestionnaireHistory.concepts),
                   subqueryload(QuestionnaireHistory.questions))
      return query.get(questionnaireIdAndVersion)

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
