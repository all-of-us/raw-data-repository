import clock

from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao
from dao.questionnaire_dao import QuestionnaireHistoryDao
from model.questionnaire import QuestionnaireQuestion
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from sqlalchemy.orm import subqueryload
from werkzeug.exceptions import BadRequest

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

  def _validate_model(self, session, obj):
    if not obj.participantId:
      raise BadRequest('QuestionnaireResponse.participantId is required.')
    if not obj.questionnaireId:
      raise BadRequest('QuestionnaireResponse.questionnaireId is required.')
    if not obj.questionnaireVersion:
      raise BadRequest('QuestionnaireResponse.questionnaireVersion is required.')
    if not ParticipantDao().get_with_session(session, obj.participantId):
      raise BadRequest('Participant with ID %s is not found.' % obj.participantId)
    qh = QuestionnaireHistoryDao().get_with_children_with_session(session,
                                                                  [obj.questionnaireId,
                                                                   obj.questionnaireVersion])
    if not qh:
      raise BadRequest('Questionnaire with ID %s, version %s is not found' %
                       (obj.questionnaireId, obj.questionnaireVersion))
    question_ids = set([question.questionnaireQuestionId for question in qh.questions])
    for answer in obj.answers:
      if answer.questionId not in question_ids:
        raise BadRequest('Questionnaire response contains question ID %s not in questionnaire.' %
                         answer.questionId)


  def insert_with_session(self, session, questionnaire_response):
    questionnaire_response.created = clock.CLOCK.now()
    question_ids = [answer.questionId for answer in questionnaire_response.answers]
    current_answers = (QuestionnaireResponseAnswerDao().
        get_current_answers_for_concepts(session, questionnaire_response.participantId,
                                         question_ids))
    super(QuestionnaireResponseDao, self).insert_with_session(session, questionnaire_response)
    # Mark existing answers for the questions in this response given previously by this participant
    # as ended.
    for answer in current_answers:
      answer.endTime = questionnaire_response.created
      session.merge(answer)
    return questionnaire_response

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
    subquery = (session.query(QuestionnaireQuestion)
        .filter(QuestionnaireQuestion.questionnaireQuestionId.in_(question_ids))
        .subquery())

    return (session.query(QuestionnaireResponseAnswer).join(QuestionnaireResponse)
        .join(QuestionnaireQuestion)
        .filter(QuestionnaireResponse.participantId == participant_id)
        .filter(QuestionnaireResponseAnswer.endTime == None)
        .filter(QuestionnaireQuestion.codeId == subquery.c.code_id)
        .all())