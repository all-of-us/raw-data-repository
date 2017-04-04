import clock
import config
import json

from code_constants import PPI_SYSTEM, RACE_QUESTION_CODE, CONSENT_FOR_STUDY_ENROLLMENT_MODULE
from dao.base_dao import BaseDao
from dao.code_dao import CodeDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.questionnaire_dao import QuestionnaireHistoryDao, QuestionnaireQuestionDao
from field_mappings import FieldType, QUESTION_CODE_TO_FIELD, QUESTIONNAIRE_MODULE_CODE_TO_FIELD
from model.questionnaire import QuestionnaireQuestion
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from participant_enums import QuestionnaireStatus, get_race
from sqlalchemy.orm import subqueryload
from werkzeug.exceptions import BadRequest

def count_completed_baseline_ppi_modules(participant_summary):
  baseline_ppi_module_fields = config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS, [])
  return sum(1 for field in baseline_ppi_module_fields
             if getattr(participant_summary, field) == QuestionnaireStatus.SUBMITTED)

def count_completed_ppi_modules(participant_summary):
  ppi_module_fields = config.getSettingList(config.PPI_QUESTIONNAIRE_FIELDS, [])
  return sum(1 for field in ppi_module_fields
             if getattr(participant_summary, field) == QuestionnaireStatus.SUBMITTED)

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
    if not obj.questionnaireId:
      raise BadRequest('QuestionnaireResponse.questionnaireId is required.')
    if not obj.questionnaireVersion:
      raise BadRequest('QuestionnaireResponse.questionnaireVersion is required.')

  def insert_with_session(self, session, questionnaire_response):
    qh = (QuestionnaireHistoryDao().
          get_with_children_with_session(session, [questionnaire_response.questionnaireId,
                                                   questionnaire_response.questionnaireVersion]))
    if not qh:
      raise BadRequest('Questionnaire with ID %s, version %s is not found' %
                       (questionnaire_response.questionnaireId,
                        questionnaire_response.questionnaireVersion))
    q_question_ids = set([question.questionnaireQuestionId for question in qh.questions])
    for answer in questionnaire_response.answers:
      if answer.questionId not in q_question_ids:
        raise BadRequest('Questionnaire response contains question ID %s not in questionnaire.' %
                         answer.questionId)

    questionnaire_response.created = clock.CLOCK.now()

    # Put the ID into the resource.
    resource_json = json.loads(questionnaire_response.resource)
    resource_json['id'] = str(questionnaire_response.questionnaireResponseId)
    questionnaire_response.resource = json.dumps(resource_json)

    question_ids = [answer.questionId for answer in questionnaire_response.answers]
    questions = QuestionnaireQuestionDao().get_all_with_session(session, question_ids)
    code_ids = [question.codeId for question in questions]
    current_answers = (QuestionnaireResponseAnswerDao().
        get_current_answers_for_concepts(session, questionnaire_response.participantId, code_ids))
    super(QuestionnaireResponseDao, self).insert_with_session(session, questionnaire_response)
    # Mark existing answers for the questions in this response given previously by this participant
    # as ended.
    for answer in current_answers:
      answer.endTime = questionnaire_response.created
      session.merge(answer)

    self._update_participant_summary(session, questionnaire_response, code_ids, questions, qh)
    return questionnaire_response

  def _get_field_value(self, field_type, answer):
    if field_type == FieldType.CODE:
      return answer.valueCodeId
    if field_type == FieldType.STRING:
      return answer.valueString
    if field_type == FieldType.DATE:
      return answer.valueDate
    raise BadRequest("Don't know how to map field of type %s" % field_type)

  def _update_field(self, participant_summary, field_name, field_type, answer):
    value = getattr(participant_summary, field_name)
    new_value = self._get_field_value(field_type, answer)
    if new_value is not None and value != new_value:
      setattr(participant_summary, field_name, new_value)
      return True
    return False

  def _update_participant_summary(self, session, questionnaire_response, code_ids, questions, qh):
    """Updates the participant summary based on questions answered and modules completed
    in the questionnaire response.

    If no participant summary exists already, only a response to the study enrollment consent
    questionnaire can be submitted, and it must include first and last name and e-mail address.
    """
    participant_summary = (ParticipantSummaryDao().
                           get_with_session(session, questionnaire_response.participantId))

    code_ids.extend([concept.codeId for concept in qh.concepts])

    code_dao = CodeDao()

    something_changed = False
    # If no participant summary exists, make sure this is the study enrollment consent.
    if not participant_summary:
      consent_code = code_dao.get_code(PPI_SYSTEM, CONSENT_FOR_STUDY_ENROLLMENT_MODULE)
      if not consent_code:
        raise BadRequest('No study enrollment consent code found; import codebook.')
      if not consent_code.codeId in code_ids:
        raise BadRequest("Can't submit order for participant %s without consent" %
                         questionnaire_response.participantId)
      participant = ParticipantDao().validate_participant_reference(session, questionnaire_response)
      participant_summary = ParticipantDao.create_summary_for_participant(participant)
      something_changed = True

    # Fetch the codes for all questions and concepts
    codes = code_dao.get_with_ids(code_ids)

    code_map = {code.codeId: code for code in codes if code.system == PPI_SYSTEM}
    question_map = {question.questionnaireQuestionId: question for question in questions}
    race_code_ids = []
    # Set summary fields for answers that have questions with codes found in QUESTION_CODE_TO_FIELD
    for answer in questionnaire_response.answers:
      question = question_map.get(answer.questionId)
      if question:
        code = code_map.get(question.codeId)
        if code:
          summary_field = QUESTION_CODE_TO_FIELD.get(code.value)
          if summary_field:
            something_changed = self._update_field(participant_summary, summary_field[0],
                                                   summary_field[1], answer)
          elif code.value == RACE_QUESTION_CODE:
            race_code_ids.append(answer.valueCodeId)

    # If race was provided in the response in one or more answers, set the new value.
    if race_code_ids:
      race_codes = [code_dao.get(code_id) for code_id in race_code_ids]
      race = get_race(race_codes)
      if race != participant_summary.race:
        participant_summary.race = race
        something_changed = True

    # Set summary fields to SUBMITTED for questionnaire concepts that are found in
    # QUESTIONNAIRE_MODULE_CODE_TO_FIELD
    module_changed = False
    for concept in qh.concepts:
      code = code_map.get(concept.codeId)
      if code:
        summary_field = QUESTIONNAIRE_MODULE_CODE_TO_FIELD.get(code.value)
        if summary_field:
          if getattr(participant_summary, summary_field) != QuestionnaireStatus.SUBMITTED:
            setattr(participant_summary, summary_field, QuestionnaireStatus.SUBMITTED)
            setattr(participant_summary, summary_field + 'Time', questionnaire_response.created)
            something_changed = True
            module_changed = True
    if module_changed:
      participant_summary.numCompletedBaselinePPIModules = \
          count_completed_baseline_ppi_modules(participant_summary)
      participant_summary.numCompletedPPIModules = \
          count_completed_ppi_modules(participant_summary)

    if something_changed:
      if (not participant_summary.firstName or not participant_summary.lastName
          or not participant_summary.email):
        raise BadRequest('First name, last name, and email address are required for consenting '
                         'participants')
      ParticipantSummaryDao().update_enrollment_status(participant_summary)
      session.merge(participant_summary)

  def insert(self, obj):
    if obj.questionnaireResponseId:
      return super(QuestionnaireResponseDao, self).insert(obj)
    return self._insert_with_random_id(obj, ['questionnaireResponseId'])

class QuestionnaireResponseAnswerDao(BaseDao):

  def __init__(self):
    super(QuestionnaireResponseAnswerDao, self).__init__(QuestionnaireResponseAnswer)

  def get_id(self, obj):
    return obj.questionnaireResponseAnswerId

  def get_current_answers_for_concepts(self, session, participant_id, code_ids):
    """Return any answers the participant has previously given to questions with the specified
    code IDs."""
    if not code_ids:
      return []
    return (session.query(QuestionnaireResponseAnswer).join(QuestionnaireResponse)
        .join(QuestionnaireQuestion)
        .filter(QuestionnaireResponse.participantId == participant_id)
        .filter(QuestionnaireResponseAnswer.endTime == None)
        .filter(QuestionnaireQuestion.codeId.in_(code_ids))
        .all())
