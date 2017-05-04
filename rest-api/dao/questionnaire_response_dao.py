import clock
import config
import json

import fhirclient.models.questionnaireresponse
from sqlalchemy.orm import subqueryload
from werkzeug.exceptions import BadRequest

from code_constants import PPI_SYSTEM, RACE_QUESTION_CODE, CONSENT_FOR_STUDY_ENROLLMENT_MODULE
from code_constants import EHR_CONSENT_QUESTION_CODE, CONSENT_PERMISSION_YES_CODE
from code_constants import CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE, PPI_EXTRA_SYSTEM
from config_api import is_config_admin
from dao.base_dao import BaseDao
from dao.code_dao import CodeDao
from dao.participant_dao import ParticipantDao, raise_if_withdrawn
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.questionnaire_dao import QuestionnaireHistoryDao, QuestionnaireQuestionDao
from field_mappings import FieldType, QUESTION_CODE_TO_FIELD, QUESTIONNAIRE_MODULE_CODE_TO_FIELD
from model.code import CodeType
from model.questionnaire import QuestionnaireQuestion
from model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from participant_enums import QuestionnaireStatus, get_race

_QUESTIONNAIRE_PREFIX = 'Questionnaire/'
_QUESTIONNAIRE_HISTORY_SEGMENT = '/_history/'
_QUESTIONNAIRE_REFERENCE_FORMAT = (_QUESTIONNAIRE_PREFIX + "%d" +
                                   _QUESTIONNAIRE_HISTORY_SEGMENT + "%d")

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

  def get_with_session(self, session, obj_id, **kwargs):
    result = super(QuestionnaireResponseDao, self).get_with_session(session, obj_id, **kwargs)
    if result:
      ParticipantDao().validate_participant_reference(session, result)
    return result

  def get_with_children(self, questionnaire_response_id):
    with self.session() as session:
      query = session.query(QuestionnaireResponse) \
          .options(subqueryload(QuestionnaireResponse.answers))
      result = query.get(questionnaire_response_id)
      if result:
        ParticipantDao().validate_participant_reference(session, result)
      return result

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
    # Block on other threads modifying the participant or participant summary.
    participant = ParticipantDao().get_for_update(session, questionnaire_response.participantId)
    participant_summary = participant.participantSummary

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
    else:
      raise_if_withdrawn(participant_summary)

    # Fetch the codes for all questions and concepts
    codes = code_dao.get_with_ids(code_ids)

    code_map = {code.codeId: code for code in codes if code.system == PPI_SYSTEM}
    question_map = {question.questionnaireQuestionId: question for question in questions}
    race_code_ids = []
    ehr_consent = False
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
          elif code.value == EHR_CONSENT_QUESTION_CODE:
            code = code_dao.get(answer.valueCodeId)
            if code and code.value == CONSENT_PERMISSION_YES_CODE:
              ehr_consent = True

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
          new_status = QuestionnaireStatus.SUBMITTED
          if code.value == CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE and not ehr_consent:
            new_status = QuestionnaireStatus.SUBMITTED_NO_CONSENT
          if getattr(participant_summary, summary_field) != new_status:
            setattr(participant_summary, summary_field, new_status)
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

  def to_client_json(self):
    return json.loads(self.resource)

  @classmethod
  def from_client_json(cls, resource_json, participant_id=None, client_id=None):
    #pylint: disable=unused-argument
    # Parse the questionnaire response, but preserve the original response when persisting
    fhir_qr = fhirclient.models.questionnaireresponse.QuestionnaireResponse(resource_json)
    patient_id = fhir_qr.subject.reference
    if patient_id != 'Patient/P{}'.format(participant_id):
      raise BadRequest("Questionnaire response subject reference does not match participant_id %d"
                       % participant_id)
    questionnaire = cls._get_questionnaire(fhir_qr.questionnaire, resource_json)
    qr = QuestionnaireResponse(questionnaireId=questionnaire.questionnaireId,
                               questionnaireVersion=questionnaire.version,
                               participantId=participant_id,
                               resource=json.dumps(resource_json))

    # Extract a code map and answers from the questionnaire response.
    code_map, answers = cls._extract_codes_and_answers(fhir_qr.group,
                                                                         questionnaire)
    # Get or insert codes, and retrieve their database IDs.
    code_id_map = CodeDao().get_or_add_codes(code_map,
                                             _add_codes_if_missing=_add_codes_if_missing(client_id))

     # Now add the child answers, using the IDs in code_id_map
    cls._add_answers(qr, code_id_map, answers)

    return qr

  @staticmethod
  def _get_questionnaire(questionnaire, resource_json):
    """Retrieves the questionnaire referenced by this response; mutates the resource JSON to include
    the version if it doesn't already."""
    if not questionnaire.reference.startswith(_QUESTIONNAIRE_PREFIX):
      raise BadRequest('Questionnaire reference %s is invalid' % questionnaire.reference)
    questionnaire_reference = questionnaire.reference[len(_QUESTIONNAIRE_PREFIX):]
    # If the questionnaire response specifies the version of the questionnaire it's for, use it.
    if _QUESTIONNAIRE_HISTORY_SEGMENT in questionnaire_reference:
      questionnaire_ref_parts = questionnaire_reference.split(_QUESTIONNAIRE_HISTORY_SEGMENT)
      if len(questionnaire_ref_parts) != 2:
        raise BadRequest('Questionnaire id %s is invalid' % questionnaire_reference)
      try:
        questionnaire_id = int(questionnaire_ref_parts[0])
        version = int(questionnaire_ref_parts[1])
        q = QuestionnaireHistoryDao().get_with_children((questionnaire_id, version))
        if not q:
          raise BadRequest('Questionnaire with id %d, version %d is not found' %
                           (questionnaire_id, version))
        return q
      except ValueError:
        raise BadRequest('Questionnaire id %s is invalid' % questionnaire_reference)
    else:
      try:
        questionnaire_id = int(questionnaire_reference)
        from dao.questionnaire_dao import QuestionnaireDao
        q = QuestionnaireDao().get_with_children(questionnaire_id)
        if not q:
          raise BadRequest('Questionnaire with id %d is not found' % questionnaire_id)
        # Mutate the questionnaire reference to include the version.
        questionnaire_reference = _QUESTIONNAIRE_REFERENCE_FORMAT % (questionnaire_id, q.version)
        resource_json["questionnaire"]["reference"] = questionnaire_reference
        return q
      except ValueError:
        raise BadRequest('Questionnaire id %s is invalid' % questionnaire_reference)

  @classmethod
  def _extract_codes_and_answers(cls, group, q):
    """Returns (system, code) -> (display, code type, question code id) code map
    and (QuestionnaireResponseAnswer, (system, code)) answer pairs."""
    code_map = {}
    answers = []
    link_id_to_question = {}
    if q.questions:
      link_id_to_question = {question.linkId: question for question in q.questions}
    cls._populate_codes_and_answers(group, code_map, answers, link_id_to_question,
                                                      q.questionnaireId)
    return (code_map, answers)

  @classmethod
  def _populate_codes_and_answers(cls, group, code_map, answers, link_id_to_question,
                                  questionnaire_id):
    """Populates code_map with (system, code) -> (display, code type, question code id)
    and answers with (QuestionnaireResponseAnswer, (system, code)) pairs."""
    if group.question:
      for question in group.question:
        if question.linkId and question.answer:
          qq = link_id_to_question.get(question.linkId)
          if qq:
            for answer in question.answer:
              qr_answer = QuestionnaireResponseAnswer(questionId=qq.questionnaireQuestionId)
              system_and_code = None
              ignore_answer = False
              if answer.valueCoding:
                if not answer.valueCoding.system:
                  raise BadRequest("No system provided for valueCoding: %s" % question.linkId)
                if not answer.valueCoding.code:
                  raise BadRequest("No code provided for valueCoding: %s" % question.linkId)
                if answer.valueCoding.system == PPI_EXTRA_SYSTEM:
                  # Ignore answers from the ppi-extra system, as they aren't used for analysis.
                  ignore_answer = True
                else:
                  system_and_code = (answer.valueCoding.system, answer.valueCoding.code)
                  if not system_and_code in code_map:
                    code_map[system_and_code] = (answer.valueCoding.display, CodeType.ANSWER,
                                                 qq.codeId)
              if not ignore_answer:
                if answer.valueDecimal:
                  qr_answer.valueDecimal = answer.valueDecimal
                if answer.valueInteger:
                  qr_answer.valueInteger = answer.valueInteger
                if answer.valueString:
                  qr_answer.valueString = answer.valueString
                if answer.valueDate:
                  qr_answer.valueDate = answer.valueDate.date
                if answer.valueDateTime:
                  qr_answer.valueDateTime = answer.valueDateTime.date
                if answer.valueBoolean:
                  qr_answer.valueBoolean = answer.valueBoolean
                answers.append((qr_answer, system_and_code))
              if answer.group:
                for sub_group in answer.group:
                  cls._populate_codes_and_answers(sub_group, code_map, answers,
                                                                    link_id_to_question,
                                                                    questionnaire_id)

    if group.group:
      for sub_group in group.group:
        cls._populate_codes_and_answers(sub_group, code_map, answers,
                                                          link_id_to_question, questionnaire_id)
  @staticmethod
  def _add_answers(qr, code_id_map, answers):
    for answer, system_and_code in answers:
      if system_and_code:
        answer.valueCodeId = code_id_map[system_and_code]
      qr.answers.append(answer)


def _add_codes_if_missing(client_id):
  # Don't add missing codes for questionnaire responses submitted by the config admin
  # (our command line tools.) Tests override this to return true.
  return not is_config_admin(client_id)


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
