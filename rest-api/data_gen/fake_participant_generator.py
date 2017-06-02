"""Creates a participant, physical measurements, questionnaire responses, and biobank orders."""
import clock
import collections
import csv
import datetime
import logging
import random

from code_constants import PPI_SYSTEM, CONSENT_FOR_STUDY_ENROLLMENT_MODULE
from code_constants import CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE, OVERALL_HEALTH_PPI_MODULE
from code_constants import LIFESTYLE_PPI_MODULE, THE_BASICS_PPI_MODULE
from code_constants import RACE_QUESTION_CODE, GENDER_IDENTITY_QUESTION_CODE
from code_constants import FIRST_NAME_QUESTION_CODE, LAST_NAME_QUESTION_CODE
from code_constants import MIDDLE_NAME_QUESTION_CODE, ZIPCODE_QUESTION_CODE
from code_constants import STATE_QUESTION_CODE, DATE_OF_BIRTH_QUESTION_CODE, EMAIL_QUESTION_CODE
from code_constants import STREET_ADDRESS_QUESTION_CODE, CITY_QUESTION_CODE
from code_constants import PHONE_NUMBER_QUESTION_CODE, RECONTACT_METHOD_QUESTION_CODE
from code_constants import LANGUAGE_QUESTION_CODE, SEX_QUESTION_CODE
from code_constants import SEXUAL_ORIENTATION_QUESTION_CODE, EDUCATION_QUESTION_CODE
from code_constants import INCOME_QUESTION_CODE, CABOR_SIGNATURE_QUESTION_CODE
from code_constants import PMI_PREFER_NOT_TO_ANSWER_CODE, PMI_OTHER_CODE, BIOBANK_TESTS
from field_mappings import QUESTION_CODE_TO_FIELD

from dao.code_dao import CodeDao
from dao.hpo_dao import HPODao
from dao.questionnaire_dao import QuestionnaireDao
from model.code import CodeType
from participant_enums import UNSET_HPO_ID
from werkzeug.exceptions import BadRequest

_TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
# 30%+ of participants have no primary provider link / HPO set
_NO_HPO_PERCENT = 0.3
# 20%+ of participants have no questionnaires submitted (including consent)
_NO_QUESTIONNAIRES_SUBMITTED = 0.2
# 20% of consented participants that submit the basics questionnaire have no biobank orders
_NO_BIOBANK_ORDERS = 0.2
# 20% of consented participants that submit the basics questionnaire have no physical measurements
_NO_PHYSICAL_MEASUREMENTS = 0.2
# 80% of consented participants have no changes to their HPO
_NO_HPO_CHANGE = 0.8
# 5% of participants withdraw from the study
_WITHDRAWN_PERCENT = 0.05
# 5% of participants suspend their account
_SUSPENDED_PERCENT = 0.05
# 5% of participants with biobank orders have multiple
_MULTIPLE_BIOBANK_ORDERS = 0.05
# 20% of participants with biobank orders have no biobank samples
_NO_BIOBANK_SAMPLES = 0.2
# Any other questionnaire has a 40% chance of not being submitted
_QUESTIONNAIRE_NOT_SUBMITTED = 0.4
# Any given question on a submitted questionnaire has a 10% chance of not being answered
_QUESTION_NOT_ANSWERED = 0.1
# Maximum number of days between a participant consenting and submitting physical measurements
_MAX_DAYS_BEFORE_PHYSICAL_MEASUREMENTS = 60
# Maximum number of days between a participant consenting and submitting a biobank order.
_MAX_DAYS_BEFORE_BIOBANK_ORDER = 60
# Maximum number of days between a participant consenting and changing their HPO
_MAX_DAYS_BEFORE_HPO_CHANGE = 60
# Maximum number of days between the last request and the participant withdrawing from the study
_MAX_DAYS_BEFORE_WITHDRAWAL = 30
# Maximum number of days between the last request and the participant suspending their account
_MAX_DAYS_BEFORE_SUSPENSION = 30
# Max amount of time between created biobank orders and collected time for a sample.
_MAX_MINUTES_BETWEEN_ORDER_CREATED_AND_SAMPLE_COLLECTED = 72 * 60
# Max amount of time between collected and processed biobank order samples.
_MAX_MINUTES_BETWEEN_SAMPLE_COLLECTED_AND_PROCESSED = 72 * 60
# Max amount of time between processed and finalized biobank order samples.
_MAX_MINUTES_BETWEEN_SAMPLE_PROCESSED_AND_FINALIZED = 72 * 60
# Max amount of time between processed and finalized biobank orders.
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
_CALIFORNIA_HPOS = ['CAL_PMC', 'SAN_YSIDRO']

_QUESTION_CODES = QUESTION_CODE_TO_FIELD.keys() + [RACE_QUESTION_CODE,
                                                   CABOR_SIGNATURE_QUESTION_CODE]

_CONSTANT_CODES = [PMI_PREFER_NOT_TO_ANSWER_CODE, PMI_OTHER_CODE]


class FakeParticipantGenerator(object):

  def __init__(self, client):
    self._client = client
    self._hpos = HPODao().get_all()
    self._now = clock.CLOCK.now()
    self._consent_questionnaire_id_and_version = None
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
    """Locates questionnaires and verifies that they have the appropriate questions in them."""
    questionnaire_dao = QuestionnaireDao()
    code_dao = CodeDao()
    question_code_to_questionnaire_id = {}
    self._questionnaire_to_questions = collections.defaultdict(list)
    self._question_code_to_answer_codes = {}
    # Populate maps of questionnaire ID/version to [(question_code, link ID)] and
    # question code to answer codes.
    for concept in _QUESTIONNAIRE_CONCEPTS:
      code = code_dao.get_code(PPI_SYSTEM, concept)
      if code is None:
        raise BadRequest('Code missing: %s; import data and clear cache.' % concept)
      questionnaire = questionnaire_dao.get_latest_questionnaire_with_concept(code.codeId)
      if questionnaire is None:
        raise BadRequest('Questionnaire for code %s missing; import data.' % concept)
      questionnaire_id_and_version = (questionnaire.questionnaireId, questionnaire.version)
      if concept == CONSENT_FOR_STUDY_ENROLLMENT_MODULE:
        self._consent_questionnaire_id_and_version = questionnaire_id_and_version
      elif concept == THE_BASICS_PPI_MODULE:
        self._the_basics_questionnaire_id_and_version = questionnaire_id_and_version
      questions = self._questionnaire_to_questions[questionnaire_id_and_version]
      if questions:
        # We already handled this questionnaire.
        continue

      for question in questionnaire.questions:
        question_code = code_dao.get(question.codeId)
        if question_code.value in _QUESTION_CODES:
          question_code_to_questionnaire_id[question_code.value] = questionnaire.questionnaireId
          questions.append((question_code.value, question.linkId))
          answer_codes = self._get_answer_codes(question_code)
          all_codes = (answer_codes + _CONSTANT_CODES) if answer_codes else _CONSTANT_CODES
          self._question_code_to_answer_codes[question_code.value] = all_codes
    # Log warnings for any question codes not found in the questionnaires.
    for code_value in _QUESTION_CODES:
      questionnaire_id = question_code_to_questionnaire_id.get(code_value)
      if not questionnaire_id:
        logging.warning('Question for code %s missing; import questionnaires', code_value)

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
    self._city_names = self._read_all_lines('city_names.txt')
    self._street_names = self._read_all_lines('street_names.txt')

  def _make_physical_measurements(self, participant_id, measurements_time):
    time_str = measurements_time.isoformat()
    blood_pressure = random.randint(50, 200)
    entry_1 = {
      "fullUrl": "urn:example:report",
      "resource":
        {"author": [{"display": "N/A"}],
         "date": time_str,
         "resourceType": "Composition",
         "section": [{"entry": [{"reference": "urn:example:blood-pressure-1"}]}],
         "status": "final",
         "subject": {
          "reference": "Patient/%s" % participant_id
         },
         "title": "PMI Intake Evaluation",
         "type": {"coding": [{"code": "intake-exam-v0.0.1",
                              "display": "PMI Intake Evaluation v0.0.1",
                              "system": "http://terminology.pmi-ops.org/CodeSystem/document-type"
                            }],
                  "text": "PMI Intake Evaluation v0.0.1"
                }
        }
    }
    entry_2 = {
      "fullUrl": "urn:example:blood-pressure-1",
      "resource": {
        "bodySite": {
          "coding": [
            {
              "code": "368209003",
              "display": "Right arm",
              "system": "http://snomed.info/sct"
            }
          ],
          "text": "Right arm"
        },
        "code": {
          "coding": [
            {
              "code": "55284-4",
              "display": "Blood pressure systolic and diastolic",
              "system": "http://loinc.org"
            }
          ],
          "text": "Blood pressure systolic and diastolic"
        },
        "component": [
          {
            "code": {
              "coding": [
                {
                  "code": "8480-6",
                  "display": "Systolic blood pressure",
                  "system": "http://loinc.org"
                }
              ],
              "text": "Systolic blood pressure"
            },
            "valueQuantity": {
              "code": "mm[Hg]",
              "system": "http://unitsofmeasure.org",
              "unit": "mmHg",
              "value": blood_pressure
            }
          }
        ],
        "effectiveDateTime": time_str,
        "resourceType": "Observation",
        "status": "final",
        "subject": {
          "reference": "Patient/%s" % participant_id
        }
      }
    },
    return {"entry": [entry_1, entry_2]}

  def _submit_physical_measurements(self, participant_id, consent_time):
    if random.random() <= _NO_PHYSICAL_MEASUREMENTS:
      return consent_time
    days_delta = random.randint(0, _MAX_DAYS_BEFORE_BIOBANK_ORDER)
    measurements_time = consent_time + datetime.timedelta(days=days_delta)
    request_json = self._make_physical_measurements(participant_id, measurements_time)
    self._client.request_json(
        _physical_measurements_url(participant_id), method='POST', body=request_json,
        pretend_date=measurements_time)
    return measurements_time

  def _make_biobank_order_request(self, participant_id, sample_tests, created_time):
    samples = []
    order_id_suffix = '%s-%d' % (participant_id, random.randint(0, 100000000))
    request = {"subject": "Patient/%s" % participant_id,
               "identifier": [{"system": "http://health-pro.org",
                                "value": "healthpro-order-id-123%s" % order_id_suffix},
                              {"system": "https://orders.mayomedicallaboratories.com",
                                "value": "WEB1YLHV%s" % order_id_suffix}],
               # TODO: randomize this?
               "sourceSite": {"system": "http://health-pro.org",
                              "value": "789012"},
               "created": created_time.strftime(_TIME_FORMAT),
               "samples": samples,
               "notes": {
                 "collected": "Collected notes",
                 "processed": "Processed notes",
                 "finalized": "Finalized notes"
               }
              }
    for sample_test in sample_tests:
      minutes_delta = random.randint(0, _MAX_MINUTES_BETWEEN_ORDER_CREATED_AND_SAMPLE_COLLECTED)
      collected_time = created_time + datetime.timedelta(minutes=minutes_delta)
      minutes_delta = random.randint(0, _MAX_MINUTES_BETWEEN_SAMPLE_COLLECTED_AND_PROCESSED)
      processed_time = collected_time + datetime.timedelta(minutes=minutes_delta)
      minutes_delta = random.randint(0, _MAX_MINUTES_BETWEEN_SAMPLE_PROCESSED_AND_FINALIZED)
      finalized_time = processed_time + datetime.timedelta(minutes=minutes_delta)
      processing_required = True if random.random() <= 0.5 else False
      samples.append({"test": sample_test,
                      "description": "Description for %s" % sample_test,
                      "collected": collected_time.strftime(_TIME_FORMAT),
                      "processed": processed_time.strftime(_TIME_FORMAT),
                      "finalized": finalized_time.strftime(_TIME_FORMAT),
                      "processingRequired": processing_required})
    return request

  def _submit_biobank_order(self, participant_id, start_time):
    num_samples = random.randint(1, len(BIOBANK_TESTS))
    order_tests = random.sample(BIOBANK_TESTS, num_samples)
    days_delta = random.randint(0, _MAX_DAYS_BEFORE_BIOBANK_ORDER)
    created_time = start_time + datetime.timedelta(days=days_delta)
    order_json = self._make_biobank_order_request(participant_id, order_tests, created_time)
    self._client.request_json(
        _biobank_order_url(participant_id),
        method='POST',
        body=order_json,
        pretend_date=created_time)
    return created_time

  def _submit_biobank_data(self, participant_id, consent_time):
    if random.random() <= _NO_BIOBANK_ORDERS:
      return consent_time
    last_request_time = self._submit_biobank_order(participant_id, consent_time)
    if random.random() <= _MULTIPLE_BIOBANK_ORDERS:
      last_request_time = self._submit_biobank_order(participant_id, last_request_time)
    return last_request_time

  def _update_participant(self, change_time, participant_response, participant_id):
    return self._client.request_json(
        _participant_url(participant_id),
        method='PUT',
        body=participant_response,
        headers={'If-Match':
                 participant_response['meta']['versionId']},
        pretend_date=change_time)

  def _submit_hpo_changes(self, participant_response, participant_id, consent_time):
    if random.random() <= _NO_HPO_CHANGE:
      return consent_time, participant_response
    hpo = random.choice(self._hpos)
    participant_response['providerLink'] = [_make_primary_provider_link(hpo)]
    days_delta = random.randint(0, _MAX_DAYS_BEFORE_HPO_CHANGE)
    change_time = consent_time + datetime.timedelta(days=days_delta)
    result = self._update_participant(change_time, participant_response, participant_id)
    return change_time, result

  def _submit_status_changes(self, participant_response, participant_id, last_request_time):
    if random.random() <= _SUSPENDED_PERCENT:
      participant_response['suspensionStatus'] = 'NO_CONTACT'
      days_delta = random.randint(0, _MAX_DAYS_BEFORE_SUSPENSION)
      change_time = last_request_time + datetime.timedelta(days=days_delta)
      participant_response = self._update_participant(change_time, participant_response,
                                                      participant_id)
      last_request_time = change_time
    if random.random() <= _WITHDRAWN_PERCENT:
      participant_response['withdrawalStatus'] = 'NO_USE'
      days_delta = random.randint(0, _MAX_DAYS_BEFORE_WITHDRAWAL)
      change_time = last_request_time + datetime.timedelta(days=days_delta)
      self._update_participant(change_time, participant_response, participant_id)

  def generate_participant(self, include_physical_measurements, include_biobank_orders):
    participant_response, creation_time, hpo = self._create_participant()
    participant_id = participant_response['participantId']
    california_hpo = hpo is not None and hpo.name in _CALIFORNIA_HPOS
    consent_time, last_qr_time, the_basics_submission_time = (self._submit_questionnaire_responses(
      participant_id, california_hpo, creation_time))
    if consent_time:
      last_request_time = last_qr_time
      # Potentially include physical measurements and biobank orders if the client requested it
      # and the participant has submitted the basics questionnaire.
      if include_physical_measurements and the_basics_submission_time:
        last_measurement_time = self._submit_physical_measurements(participant_id,
                                                                   the_basics_submission_time)
        last_request_time = max(last_request_time, last_measurement_time)
      if include_biobank_orders and the_basics_submission_time:
        last_biobank_time = self._submit_biobank_data(participant_id,
                                                      the_basics_submission_time)
        last_request_time = max(last_request_time, last_biobank_time)
      last_hpo_change_time, participant_response = self._submit_hpo_changes(participant_response,
                                                                            participant_id,
                                                                            consent_time)
      last_request_time = max(last_request_time, last_hpo_change_time)
      self._submit_status_changes(participant_response, participant_id, last_request_time)

  def _create_participant(self):
    participant_json = {}
    hpo = None
    if random.random() > _NO_HPO_PERCENT:
      hpo = random.choice(self._hpos)
      if hpo.hpoId != UNSET_HPO_ID:
        participant_json['providerLink'] = [_make_primary_provider_link(hpo)]
    creation_time = self._days_ago(random.randint(0, _MAX_DAYS_HISTORY))
    participant_response = self._client.request_json(
        'Participant', method='POST', body=participant_json, pretend_date=creation_time)
    return (participant_response, creation_time, hpo)

  def _random_code_answer(self, question_code):
    code = random.choice(self._question_code_to_answer_codes[question_code])
    return [_code_answer(code)]

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
    return [_code_answer(code) for code in codes]

  def _choose_street_address(self):
    if random.random() <= _QUESTION_NOT_ANSWERED:
      return None
    return '%d %s' % (random.randint(100, 9999), random.choice(self._street_names))

  def _choose_city(self):
    if random.random() <= _QUESTION_NOT_ANSWERED:
      return None
    return random.choice(self._city_names)

  def _choose_phone_number(self):
    if random.random() <= _QUESTION_NOT_ANSWERED:
      return None
    return '(%d) %d-%d' % (random.randint(200, 999), random.randint(200, 999),
                           random.randint(0, 9999))

  def _choose_state_and_zip(self, answer_map):
    if random.random() <= _QUESTION_NOT_ANSWERED:
      return
    zip_code = random.choice(self._zip_code_to_state.keys())
    state = self._zip_code_to_state.get(zip_code)
    answer_map[ZIPCODE_QUESTION_CODE] = _string_answer(zip_code)
    answer_map[STATE_QUESTION_CODE] = [_code_answer('PIIState_%s' % state)]

  def _choose_name(self, answer_map):
    first_name = random.choice(self._first_names)
    middle_name = random.choice(self._middle_names)
    last_name = random.choice(self._last_names)
    email = first_name + last_name + '@example.com'
    answer_map[FIRST_NAME_QUESTION_CODE] = _string_answer(first_name)
    answer_map[MIDDLE_NAME_QUESTION_CODE] = _string_answer(middle_name)
    answer_map[LAST_NAME_QUESTION_CODE] = _string_answer(last_name)
    answer_map[EMAIL_QUESTION_CODE] = _string_answer(email)

  def _choose_date_of_birth(self, answer_map):
    if random.random() <= _QUESTION_NOT_ANSWERED:
      return
    delta = datetime.timedelta(days=random.randint(0, self._max_days_for_birth_date))
    date_of_birth = (self._min_birth_date + delta).date()
    answer_map[DATE_OF_BIRTH_QUESTION_CODE] = [{"valueDate": date_of_birth.isoformat()}]

  def _make_answer_map(self, california_hpo):
    answer_map = {}
    answer_map[RACE_QUESTION_CODE] = self._choose_answer_codes(RACE_QUESTION_CODE,
                                                               _MULTIPLE_RACE_ANSWERS,
                                                               _MAX_RACE_ANSWERS)
    answer_map[STREET_ADDRESS_QUESTION_CODE] = _string_answer(self._choose_street_address())
    answer_map[CITY_QUESTION_CODE] = _string_answer(self._choose_city())
    answer_map[PHONE_NUMBER_QUESTION_CODE] = _string_answer(self._choose_phone_number())
    for question_code in [GENDER_IDENTITY_QUESTION_CODE, RECONTACT_METHOD_QUESTION_CODE,
                          LANGUAGE_QUESTION_CODE, SEX_QUESTION_CODE,
                          SEXUAL_ORIENTATION_QUESTION_CODE, EDUCATION_QUESTION_CODE,
                          INCOME_QUESTION_CODE]:
      answer_map[question_code] = self._choose_answer_code(question_code)

    self._choose_state_and_zip(answer_map)
    self._choose_name(answer_map)
    if california_hpo:
      answer_map[CABOR_SIGNATURE_QUESTION_CODE] = _string_answer('signature')
    self._choose_date_of_birth(answer_map)
    return answer_map

  def _submit_questionnaire_responses(self, participant_id, california_hpo, start_time):
    if random.random() <= _NO_QUESTIONNAIRES_SUBMITTED:
      return None, None, None
    submission_time = start_time
    answer_map = self._make_answer_map(california_hpo)

    delta = datetime.timedelta(days=random.randint(0, _MAX_DAYS_BETWEEN_SUBMISSIONS))
    submission_time = submission_time + delta
    consent_time = submission_time
    # Submit the consent questionnaire always and other questionnaires at random.
    questions = self._questionnaire_to_questions[self._consent_questionnaire_id_and_version]
    self._submit_questionnaire_response(participant_id, self._consent_questionnaire_id_and_version,
                                        questions, submission_time, answer_map)

    the_basics_submission_time = None
    for questionnaire_id_and_version, questions in self._questionnaire_to_questions.iteritems():
      if (questionnaire_id_and_version != self._consent_questionnaire_id_and_version and
          random.random() > _QUESTIONNAIRE_NOT_SUBMITTED):
        delta = datetime.timedelta(days=random.randint(0, _MAX_DAYS_BETWEEN_SUBMISSIONS))
        submission_time = submission_time + delta
        self._submit_questionnaire_response(participant_id, questionnaire_id_and_version,
                                            questions, submission_time, answer_map)
        if questionnaire_id_and_version == self._the_basics_questionnaire_id_and_version:
          the_basics_submission_time = submission_time
    return consent_time, submission_time, the_basics_submission_time

  def _create_question_answer(self, link_id, answers):
    return {"linkId": link_id, "answer": answers}

  def _submit_questionnaire_response(self, participant_id, q_id_and_version, questions,
                                     submission_time, answer_map):
    questions_with_answers = []
    for question_code, link_id in questions:
      answer = answer_map.get(question_code)
      if answer:
        questions_with_answers.append(self._create_question_answer(link_id, answer))
    qr_json = self._create_questionnaire_response(participant_id, q_id_and_version,
                                                  questions_with_answers)
    self._client.request_json(
        _questionnaire_response_url(participant_id),
        method='POST',
        body=qr_json,
        pretend_date=submission_time)

  def _create_questionnaire_response(self, participant_id, q_id_and_version,
                                     questions_with_answers):
    qr_json = {'resourceType': 'QuestionnaireResponse',
               'status': 'completed',
               'subject': {'reference': 'Patient/%s' % participant_id},
               'questionnaire': {'reference':
                                 'Questionnaire/%d/_history/%d' % (q_id_and_version[0],
                                                                    q_id_and_version[1])},
               'group': {}}
    if questions_with_answers:
      qr_json['group']['question'] = questions_with_answers
    return qr_json

def _questionnaire_response_url(participant_id):
  return 'Participant/%s/QuestionnaireResponse' % participant_id

def _biobank_order_url(participant_id):
  return 'Participant/%s/BiobankOrder' % participant_id

def _physical_measurements_url(participant_id):
  return 'Participant/%s/PhysicalMeasurements' % participant_id

def _participant_url(participant_id):
  return 'Participant/%s' % participant_id

def _string_answer(value):
  return [{"valueString": value}]

def _code_answer(code):
  return {"valueCoding": {"system": PPI_SYSTEM, "code": code}}

def _make_primary_provider_link(hpo):
  return {'primary': True, 'organization': {'reference': 'Organization/' + hpo.name}}
