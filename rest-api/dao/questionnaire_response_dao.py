import clock

from dao.base_dao import BaseDao
from model.questionnaire import QuestionnaireQuestion
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from sqlalchemy.orm import subqueryload

class QuestionnaireResponseDao(BaseDao):

  def __init__(self):
    super(QuestionnaireResponseDao, self).__init__(QuestionnaireResponse)

  def get_id(self, obj):
    return obj.questionnaireResponseId

  def get_with_children(self, questionnaire_response_id):
    with self.session() as session:
      query = session.query(QuestionnaireResponse) \
          .options(subqueryload(QuestionnaireResponse.answers))
      return query.get(questionnaire_response_id)

  def insert_with_session(self, session, questionnaireResponse):
    questionnaireResponse.created = clock.CLOCK.now()
    question_ids = [answer.questionId for answer in questionnaireResponse.answers]
    current_answers = QuestionnaireResponseAnswerDao().\
        get_current_answers_for_concepts(session, questionnaireResponse.participantId,
                                         question_ids)
    super(QuestionnaireResponseDao, self).insert_with_session(session, questionnaireResponse)
    # Mark existing answers for the questions in this response given previously by this participant
    # as ended.
    for answer in current_answers:
      answer.endTime = questionnaireResponse.created
      session.merge(answer)

class QuestionnaireResponseAnswerDao(BaseDao):

  def __init__(self):
    super(QuestionnaireResponseAnswerDao, self).__init__(QuestionnaireResponseAnswer)

  def get_id(self, obj):
    return obj.questionnaireResponseAnswerId

  def get_current_answers_for_concepts(self, session, participant_id, question_ids):
    """Return any answers the participant has previously given to questions using the same
    concepts as the questions with the provided IDs."""
    if not question_ids:
      return []
    subquery = session.query(QuestionnaireQuestion) \
        .filter(QuestionnaireQuestion.questionnaireQuestionId.in_(question_ids)) \
        .subquery()

    return session.query(QuestionnaireResponseAnswer).join(QuestionnaireResponse) \
        .join(QuestionnaireQuestion) \
        .filter(QuestionnaireResponse.participantId == participant_id) \
        .filter(QuestionnaireResponseAnswer.endTime == None) \
        .filter(QuestionnaireQuestion.conceptSystem == subquery.c.concept_system) \
        .filter(QuestionnaireQuestion.conceptCode == subquery.c.concept_code) \
        .all()