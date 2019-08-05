import json

from flask import request

from rdr_service.api_util import PTC_AND_HEALTHPRO
from rdr_service.app_util import auth_required
from rdr_service.code_constants import EMAIL_QUESTION_CODE, PPI_SYSTEM
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseAnswerDao
from rdr_service.model.code import CodeType
from rdr_service.model.participant_summary import ParticipantSummary


@auth_required(PTC_AND_HEALTHPRO)
def check_ppi_data():
  """Validates the questions/responses for test participants.

  Typically called from rdr_client/check_ppi_data.py.

  The request contains a ppi_data dict which maps test participant e-mail addresses/phone number
  to their responses. All code and answer values are unparsed strings. Values may be empty,
  and multiple values for one answer may be separated by | characters.
  {
    'ppi_data': {
      'email@address.com|5555555555': {
        'PIIName_First': 'Alex',
        'Insurance_HealthInsurance': 'HealthInsurance_Yes',
        'EmplymentWorkAddress_AddressLineOne': '',
        'Race_WhatRaceEthnicity': 'WhatRaceEthnicity_Hispanic|WhatRaceEthnicity_Black',
        ...
      },
      'email2@address.com': { ... }
    }
  }

  The response contains a ppi_results dict which maps test participant e-mail addresses to their
  individual results. Each participant's results has test/error counts and a list of human-readable
  error messages.
  {
    'ppi_results': {
      'email@address.com|5555555555': {
        'tests_count': number,
        'errors_count': number,
        'error_messages' : [
          'formatted error message detailing an error',
        ]
      },
      'email@b.com': { ... }
    }
  }
  """
  _sanity_check_codebook()
  ppi_results = {}
  ppi_data = request.get_json(force=True)['ppi_data']
  for key, codes_to_answers in ppi_data.iteritems():
    ppi_results[key] = _get_validation_result(key, codes_to_answers).to_json()
  return json.dumps({'ppi_results': ppi_results})


def _sanity_check_codebook():
  if not CodeDao().get_code(PPI_SYSTEM, EMAIL_QUESTION_CODE):
    raise RuntimeError('No question code found for %s; import codebook.' % EMAIL_QUESTION_CODE)


class _ValidationResult(object):
  """Container for an individual's PPI validation result."""
  def __init__(self):
    self.tests_count = 0
    self.errors_count = 0
    self.messages = []

  def add_error(self, message):
    self.errors_count += 1
    self.messages.append(message)

  def to_json(self):
    return {
        'tests_count': self.tests_count,
        'errors_count': self.errors_count,
        'error_messages': self.messages,
    }


def _get_validation_result(key, codes_to_answers):
  result = _ValidationResult()
  with ParticipantSummaryDao().session() as session:
    # Get summary by email or phone
    if '@' not in key:
      summaries = session.query(ParticipantSummary).\
                      filter(ParticipantSummary.loginPhoneNumber == key).all()
    else:
      summaries = session.query(ParticipantSummary).\
                      filter(ParticipantSummary.email == key).all()

  if not summaries:
    result.add_error('No ParticipantSummary found for %r.' % key)
    return result
  if len(summaries) > 1:
    result.add_error('%d ParticipantSummary values found for %r.' % (len(summaries), key))
    return result
  participant_id = summaries[0].participantId

  code_dao = CodeDao()
  qra_dao = QuestionnaireResponseAnswerDao()
  with qra_dao.session() as session:
    for code_string, answer_string in codes_to_answers.iteritems():
      result.tests_count += 1

      question_code = code_dao.get_code(PPI_SYSTEM, code_string)
      if not question_code:
        result.add_error(
            'Could not find question code %r, skipping answer %r.' % (code_string, answer_string))
        continue
      if question_code.codeType != CodeType.QUESTION:
        result.add_error(
            'Code %r type is %s, not QUESTION; skipping.' % (code_string, question_code.codeType))
        continue

      qras = qra_dao.get_current_answers_for_concepts(
          session, participant_id, [question_code.codeId])
      qra_values = set()
      for qra in qras:
        try:
          qra_values.add(_get_value_for_qra(qra, question_code, code_dao, session))
        except ValueError as e:
          result.add_error(e.message)
          continue

      if answer_string:
        expected_values = set(_boolean_to_lower(v.strip()) for v in answer_string.split('|'))
      else:
        expected_values = set()
      if expected_values != qra_values:
        result.add_error(
            '%s: Expected %s, found %s.'
            % (question_code.value, _format_values(expected_values), _format_values(qra_values)))
  return result


def _format_values(values):
  if not values:
    return 'no answer'
  return repr(sorted(list(values)))


def _get_value_for_qra(qra, question_code, code_dao, session):
  if qra.valueString:
    return qra.valueString
  if qra.valueInteger is not None:
    return str(qra.valueInteger)
  if qra.valueDecimal is not None:
    return str(qra.valueDecimal)
  if qra.valueBoolean is not None:
    return str(qra.valueBoolean).lower()
  if qra.valueDate is not None:
    return qra.valueDate.isoformat()
  if qra.valueDateTime is not None:
    return qra.valueDateTime.isoformat()
  if qra.valueCodeId is not None:
    code = code_dao.get_with_session(session, qra.valueCodeId)
    if code.system != PPI_SYSTEM:
      raise ValueError(
          'Unexpected value %r with non-PPI system %r for question %s.'
          % (code.value, code.system, question_code))
    return code.value
  raise ValueError('Answer for question %s has no value set.' % question_code)


def _boolean_to_lower(value):
  if value.lower() == 'true' or value.lower() == 'false':
    return value.lower()
  return value
