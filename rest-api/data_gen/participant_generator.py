'''Creates a participant, physical measurements, questionnaire responses, and biobank
orders.'''
import csv
import datetime
import random

from code_constants import PPI_SYSTEM, CONSENT_FOR_STUDY_ENROLLMENT_MODULE
from code_constants import CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE, OVERALL_HEALTH_PPI_MODULE
from code_constants import LIFESTYLE_PPI_MODULE, THE_BASICS_PPI_MODULE
from code_constants import QUESTION_CODE_TO_FIELD, RACE_QUESTION_CODE, GENDER_IDENTITY_QUESTION_CODE
from code_constants import FIRST_NAME_QUESTION_CODE, LAST_NAME_QUESTION_CODE
from code_constants import MIDDLE_NAME_QUESTION_CODE, ZIPCODE_QUESTION_CODE
from code_constants import STATE_QUESTION_CODE, DATE_OF_BIRTH_QUESTION_CODE
from code_constants import PMI_PREFER_NOT_TO_ANSWER_CODE, PMI_OTHER_CODE

from dao.code_dao import CodeDao
from dao.hpo_dao import HPODao
from dao.questionnaire_dao import QuestionnaireDao
from model.code import CodeType
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

# Percentage of participants with multiple race answers
_MULTIPLE_RACE_ANSWERS = 0.2

# Maximum number of race answers
_MAX_RACE_ANSWERS = 3

# Maximum age of participants 
_MAX_PARTICIPANT_AGE = 102

# Minimum age of participants
_MIN_PARTICIPANT_AGE = 12

_QUESTIONNAIRE_CONCEPTS = [CONSENT_FOR_STUDY_ENROLLMENT_MODULE,
                          CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE,
                          OVERALL_HEALTH_PPI_MODULE,
                          LIFESTYLE_PPI_MODULE,
                          THE_BASICS_PPI_MODULE]

_QUESTION_CODES = QUESTION_CODE_TO_FIELD.keys() + [RACE_QUESTION_CODE]

_CONSTANT_CODES = [PMI_PREFER_NOT_TO_ANSWER_CODE, PMI_OTHER_CODE]

class ParticipantGenerator(object):

  def __init__(self, request_sender):
    self._request_sender = request_sender
    self._hpos = HPODao().get_all()
    self._now = datetime.datetime.now()
    self._setup_questionnaires()
    self._setup_data()
    self._min_birth_date = self._now - datetime.timedelta(days=_MAX_PARTICIPANT_AGE * 365)
    self._max_days_for_birth_date = 365 * (_MAX_PARTICIPANT_AGE - _MIN_PARTICIPANT_AGE) 

  def _days_ago(self, num_days):
    return self._now - datetime.timedelta(days=num_days)

  def _get_answer_codes(self, code):
    result = []
    for child in code.children:
      if child.codeType == CodeType.ANSWER:
        result.append(child.value)
        result.extend(self._get_answer_codes(child))
    return result

  def _setup_questionnaires(self):
    '''Locates questionnaires and verifies that they have the appropriate questions in them.'''    
    questionnaire_dao = QuestionnaireDao()
    code_dao = CodeDao()    
    question_code_to_questionnaire_id = {}
    self._questionnaire_to_questions = {}
    self._question_code_to_answer_codes = {}
    # Populate maps of questionnaire ID/version to [(question_code, link ID)] and 
    # question code to answer codes.
    for concept in _QUESTIONNAIRE_CONCEPTS:
      code = code_dao.get_code(PPI_SYSTEM, concept)
      if code is None:
        raise BadRequest("Code missing: %s; import codebook" % concept)
      questionnaire = questionnaire_dao.get_latest_questionnaire_with_concept(code.codeId)
      if questionnaire is None:
        raise BadRequest("Questionnaire for code %s missing; import questionnaires" % concept)
      for question in questionnaire.questions:
        question_code = code_dao.get(question.codeId)
        if question_code.value in _QUESTION_CODES:
          question_code_to_questionnaire_id[question_code.value] = questionnaire.questionnaireId
          questionnaire_id_and_version = (questionnaire.questionnaireId, questionnaire.version)
          code_and_link_id = (question_code.value, question.linkId)
          questions = self._questionnaire_to_questions.get(questionnaire_id_and_version)
          if not questions:
            self._questionnaire_to_questions[questionnaire_id_and_version] = [code_and_link_id]
          else:
            questions.append(code_and_link_id)
          answer_codes = self._get_answer_codes(question_code)
          if answer_codes:
            self._question_code_to_answer_codes[question_code.value] = (answer_codes + 
                                                                        _CONSTANT_CODES)
    # Make sure that all the questions are in the questionnaires.
    for code_value in _QUESTION_CODES:
      questionnaire_id = question_code_to_questionnaire_id.get(code_value)
      if not questionnaire_id:
        raise BadRequest("Question for code %s missing; import questionnaires" % code_value)
  
  def _read_all_lines(self, filename):
    with open('app_data/%s' % filename) as f:
      reader = csv.reader(f)
      return [line[0].strip() for line in reader]
      
  def _setup_data(self):
    self._zip_code_to_state = {}    
    with open('app_data/zipcodes.txt') as zipcodes:
      reader = csv.reader(zipcodes)
      for zipcode, state in reader:
        self._zip_code_to_state[zipcode] = state
    self._first_names = self._read_all_lines('first_names.txt')
    self._middle_names = self._read_all_lines('middle_names.txt')
    self._last_names = self._read_all_lines('last_names.txt')
  
  def generate_participant(self):
    participant_id, creation_time = self._create_participant()
    self._submit_questionnaire_responses(participant_id, creation_time)

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

  def _random_code_answer(self, question_code):
    code = random.choice(self._question_code_to_answer_codes[question_code])
    return [{"valueCoding": {"system": PPI_SYSTEM, "code": code}}]
    
  def _choose_answer_code(self, question_code):
    if random.random() <= _QUESTION_NOT_ANSWERED:
      return None                   
    return self._random_code_answer(question_code)
  
  def _choose_answer_codes(self, question_code, percent_with_multiple, max_answers):
    if random.random() <= _QUESTION_NOT_ANSWERED:
      return None
    if random.random() > percent_with_multiple:
      return self._random_code_answer(question_code)                   
    num_answers = random.randint(2, max_answers)
    codes = random.sample(self._question_code_to_answer_codes[question_code], num_answers)
    return [{"valueCoding": {"system": PPI_SYSTEM, "code": code}} for code in codes] 
      
  def _choose_state_and_zip(self, answer_map):  
    if random.random() <= _QUESTION_NOT_ANSWERED:
      return
    zip_code = random.choice(self._zip_code_to_state.keys())
    state = self._zip_code_to_state.get(zip)
    answer_map[ZIPCODE_QUESTION_CODE] = _string_answer(zip_code)
    answer_map[STATE_QUESTION_CODE] = _string_answer(state)
  
  def _choose_name(self, answer_map):
    if random.random() <= _QUESTION_NOT_ANSWERED:
      return
    answer_map[FIRST_NAME_QUESTION_CODE] = _string_answer(random.choice(self._first_names))
    answer_map[MIDDLE_NAME_QUESTION_CODE] = _string_answer(random.choice(self._middle_names))
    answer_map[LAST_NAME_QUESTION_CODE] = _string_answer(random.choice(self._last_names))
  
  def _choose_date_of_birth(self, answer_map):      
    if random.random() <= _QUESTION_NOT_ANSWERED:
      return
    delta = datetime.timedelta(days=random.randint(0, self._max_days_for_birth_date))
    date_of_birth = (self._min_birth_date + delta).date()
    answer_map[DATE_OF_BIRTH_QUESTION_CODE] = [{"valueDate": date_of_birth.isoformat()}]
        
  def _make_answer_map(self):
    answer_map = {}
    gender_identity_answers = self._choose_answer_code(GENDER_IDENTITY_QUESTION_CODE)
    answer_map[GENDER_IDENTITY_QUESTION_CODE] = gender_identity_answers
    answer_map[RACE_QUESTION_CODE] = self._choose_answer_codes(RACE_QUESTION_CODE, 
                                                               _MULTIPLE_RACE_ANSWERS,
                                                               _MAX_RACE_ANSWERS)
    self._choose_state_and_zip(answer_map)
    self._choose_name(answer_map)
    self._choose_date_of_birth(answer_map)
    return answer_map

  def _submit_questionnaire_responses(self, participant_id, start_time):
    if random.random() <= _NO_QUESTIONNAIRES_SUBMITTED:
      return
    submission_time = start_time
    answer_map = self._make_answer_map()
    for questionnaire_id_and_version, questions in self._questionnaire_to_questions.iteritems():
      if random.random() > _QUESTIONNAIRE_NOT_SUBMITTED:
        delta = datetime.timedelta(days=random.randint(0, _MAX_DAYS_BETWEEN_SUBMISSIONS))
        submission_time = submission_time + delta
        self._submit_questionnaire_response(participant_id, questionnaire_id_and_version,
                                            questions, submission_time, answer_map)

  def _create_code_answer(self, answer_code):
    return {"valueCoding": {"code": answer_code.value,
                            "system": answer_code.system}};

  def _create_question_answer(self, link_id, answers):
    return {"linkId": link_id, "answer": answers}

  def _submit_questionnaire_response(self, participant_id, q_id_and_version, questions,
                                     submission_time, answer_map):
    code_dao = CodeDao()
    questions_with_answers = []
    for question_code, link_id in questions:
      answer = answer_map.get(question_code)
      if answer:
        questions_with_answers.append(self._create_question_answer(link_id, answer))
    qr_json = self._create_questionnaire_response(participant_id, q_id_and_version,
                                                  questions_with_answers)
    self._request_sender.send_request(submission_time, 'POST', 
                                      _questionnaire_response_url(participant_id), qr_json)

  def _create_questionnaire_response(self, participant_id, q_id_and_version,
                                     questions_with_answers):
    qr_json = {'resourceType': 'QuestionnaireResponse',
               'status': 'completed',
               'subject': {'reference': 'Patient/%s' % participant_id },
               'questionnaire': {'reference':
                                 'Questionnaire/%d/_history/%d' % (q_id_and_version[0],
                                                                    q_id_and_version[1])},
               'group': {}}
    if questions_with_answers:
      qr_json['group']['question'] = questions_with_answers
    return qr_json

def _questionnaire_response_url(participant_id):
  return 'Participant/%s/QuestionnaireResponse' % participant_id
  
def _string_answer(value):
  return [{"valueString": value}]
    
def _make_primary_provider_link(hpo):
   return {'primary': True,
           'organization': { 'reference': 'Organization/' + hpo.name}}
