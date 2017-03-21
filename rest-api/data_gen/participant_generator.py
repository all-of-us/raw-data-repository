'''Creates a participant, physical measurements, questionnaire responses, and biobank
orders.'''
import datetime
import logging
import random

from clock import FakeClock
from model.code import CodeType
from code_constants import PPI_SYSTEM
from dao.code_dao import CodeDao
from dao.hpo_dao import HPODao
from dao.questionnaire_dao import QuestionnaireConceptDao, QuestionnaireQuestionDao
from participant_enums import UNSET_HPO_ID
from werkzeug.exceptions import BadRequest

# 30%+ of participants have no primary provider link / HPO set
_NO_HPO_PERCENT = 0.3
# 20%+ of participants have no questionnaires submitted
_NO_QUESTIONNAIRES_SUBMITTED = 0.2
# Any given questionnaire has a 40% chance of not being submitted
_QUESTIONNAIRE_NOT_SUBMITTED = 0.4
# Any given question on a submitted questionnaire has a 10% chance of not being answered
_QUESTION_NOT_ANSWERED = 0.1
# Random amount of time between questionnaire submissions
_MAX_DAYS_BETWEEN_SUBMISSIONS = 30

# Start creating participants from 4 years ago
_MAX_DAYS_HISTORY = 4 * 365

# Maximum number of answers for multi-answer questions
_MAX_ANSWERS_PER_QUESTION = 3

_CODE_TO_ANSWERS_GENERATOR = {
  FIRST_NAME_QUESTION_CODE: _pick_first_name(),
  LAST_NAME_QUESTION_CODE: _pick_last_name(),
  MIDDLE_NAME_QUESTION_CODE: _pick_middle_name(),
  ZIPCODE_QUESTION_CODE: _pick_zipcode(),
  STATE_QUESTION_CODE: _pick_state(),
  DATE_OF_BIRTH_QUESTION_CODE: _pick_date_of_birth()
}




class ParticipantGenerator(object):

  def __init__(self, request_sender):
    self._request_sender = request_sender
    self._hpos = HPODao().get_all()
    self._now = datetime.datetime.now()
    self._setup_questionnaires()

  def _days_ago(self, num_days):
    return self._now - datetime.timedelta(days=num_days)

  def _setup_questionnaires(self):
    '''Locates or creates questionnaires for the question and module codes.'''
    modules_by_code = {concept.codeId: concept for concept in
                       QuestionnaireConceptDao.get_all()}
    questions_by_code = {question.codeId: question for question in
                         QuestionnaireQuestionDao.get_all()}
    code_dao = CodeDao()

    # Construct a map from module code value to missing question code values
    missing_question_codes_by_module = {}
    for question_code in QUESTION_CODE_TO_FIELD.keys():
      code = code_dao.get_code(PPI_SYSTEM, question_code)
      if not code:
        raise BadRequest('Missing question code %s, import codebook first' % question_code)
      if not questions_by_code.get(code.codeId):
        module_code = code_dao.find_ancestor_of_type(code, CodeType.MODULE)
        if not module_code:
          raise BadRequest('Code %s is missing module ancestor' % question_code)
        questions = missing_question_codes_by_module.get(module_code.value)
        if not questions:
          missing_question_codes_by_module[module_code.value] = [code]
        else:
          questions.append(code)

    created_questionnaire = False

    # Create questionnaires for missing modules
    for module_code in FIELD_TO_QUESTIONNAIRE_MODULE_CODE.values():
      code = code_dao.get_code(PPI_SYSTEM, module_code)
      if not code:
        raise BadRequest('Missing module code %s, import codebook first' % module_code)
      if not modules_by_code.get(code.codeId):
        missing_question_codes = missing_question_codes_by_module.get(module_code.value)
        self._create_questionnaire(code, missing_question_codes)
        created_questionnaire = True
        del missing_question_codes_by_module[module_code.value]

    # Create a questionnaire for any remaining question codes
    for module_code_value, missing_question_codes in missing_question_codes_by_module.iteritems():
      code = code_dao.get_code(PPI_SYSTEM, module_code_value)
      self._create_questionnaire(module_code, missing_question_codes)
      created_questionnaire = True

    # If we created any questionnaires, reload everything.
    if created_questionnaire:
      modules_by_code = {concept.codeId: concept for concept in
                         QuestionnaireConceptDao.get_all()}
      questions_by_code = {question.codeId: question for question in
                           QuestionnaireQuestionDao.get_all()}

    _populate_questionnaire_map(modules_by_code, questions_by_code)


  def _populate_questionnaire_map(self, modules_by_code, questions_by_code):
    # Construct a map from questionnaire ID and version to questions we care
    # about. Participants will randomly submit some of these questionnaires.
    self._questionnaire_map = {}
    for module_code in FIELD_TO_QUESTIONNAIRE_MODULE_CODE.values():
      code = code_dao.get_code(PPI_SYSTEM, module_code)
      module = modules_by_code.get(code.codeId)
      self._questionnaire_map[(module.questionnaireId, module.questionnaireVersion)] = []
    for question_code in QUESTION_CODE_TO_FIELD.keys():
      code = code_dao.get_code(PPI_SYSTEM, question_code)
      question = questions_by_code.get(code.codeId)
      id_and_version = (question.questionnaireId, question.questionnaireVersion)
      questions = self._questionnaire_map.get(id_and_version)
      if not questions:
        self._questionnaire_map[id_and_version] = [question]
      else:
        questions.append(question)

  def _create_questionnaire(self, module_code, question_codes):
    module_concept = {
      'system': PPI_SYSTEM,
      'code': module_code.value
    }
    questionnaire_json = { 'resourceType': 'Questionnaire',
                           'status':'published',
                           'date': self._now.isoformat(),
                           'publisher':'fake',
                           'group': { 'concept': [ module_concept ] } }

    if question_codes:
      questions = []
      for i in range(0, len(question_codes)):
        question_code = question_codes[i]
        questions.append({'linkId': '%d' % (i + 1),
                          'text': question_code.display,
                          'concept': [{'system': PPI_SYSTEM,
                                       'code': question_code.value,
                                       'display': question_code.display }]})
      questionnaire_json['group']['question'] = questions
    response = self.request_sender.send_request(self._now, 'POST', 'Questionnaire',
                                                questionnaire_json)

  def generate_participant(self):
    participant_id = self._create_participant()
    self._submit_questionnaire_responses(participant_id)

  def _create_participant(self):
    participant_json = {}
    if random.random() > _NO_HPO_PERCENT:
      hpo = random.choice(self._hpos)
      if hpo.hpoId != UNSET_HPO_ID:
        participant_json['providerLink'] = [_make_primary_provider_link(hpo)]
    creation_time = self._days_ago(random.randint(0, _MAX_DAYS_HISTORY))
    participant_response = self._request_sender.send_request(creation_time, 'POST', 'Participant',
                                                             participant_json)
    return (participant_response['participantId'], creation_time)

  def _make_answer_map(self):
    answer_map = {}
    answer_map[GENDER_IDENTITY_QUESTION_CODE] = choose_random(GENDER_IDENTITY_QUESTION_CODE)
    answer_map[RACE_QUESTION_CODE] = choose_random(GENDER_IDENTITY_QUESTION_CODE)


  def _submit_questionnaire_responses(self, participant_id, start_time):
    if random.random() <= _NO_QUESTIONNAIRES_SUBMITTED:
      return
    submission_time = start_time
    answer_map = _make_answer_map()
    for questionnaire_id_and_version, questions in self._questionnaire_map.iteritems():
      if random.random() > _QUESTIONNAIRE_NOT_SUBMITTED:
        delta = datetime.timedelta(days=random.randint(0, _MAX_DAYS_BETWEEN_SUBMISSIONS))
        submission_time = submission_time + delta
        self._submit_questionnaire_response(participant_id, questionnaire_id_and_version,
                                            questions, submission_time)

  def _create_code_answer(self, answer_code):
    return {"valueCoding": {"code": answer_code.value,
                            "system": answer_code.system}};

  def _create_question_answer(self, question, code, answers):
    return { "linkId": question.linkId,
             "text": code.display,
             "answer": answers }

  def _submit_questionnaire_response(self, participant_id, q_id_and_version, questions,
                                     submission_time, answer_map):
    code_dao = CodeDao()
    questions_with_answers = []
    for question in questions:
      code = code_dao.get(question.codeId)
      answers = answer_map.get(code.value)
      if answers:
        questions_with_answers.append(self._create_question_answer(question, code, answers))
    qr_json = self._create_questionnaire_response(participant_id, q_id_and_version,
                                                  questions_with_answers)
    self._request_sender.send_request(submission_time, 'POST', 'QuestionnaireResponse',
                                      qr_json)

  def _create_questionnaire_response(self, participant_id, q_id_and_version,
                                     questions_with_answers):
    qr_json =  {'resourceType': 'QuestionnaireResponse',
                'status': 'completed',
                'subject': {'reference': 'Patient/%s' % participant_id },
                'questionnaire': {'reference':
                                  'Questionnaire/%d/_history/%d' % (q_id_and_version[0],
                                                                    q_id_and_version[1]) },
                'group': {}}
    if questions_with_answers:
      qr_json['group']['question'] = questions_with_answers
    return qr_json

def _make_primary_provider_link(hpo):
   return {'primary': True,
           'organization': { 'reference': 'Organization/' + hpo.name }}
