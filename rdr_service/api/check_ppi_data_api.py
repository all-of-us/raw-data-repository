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
    ppi_data = request.get_json(force=True)["ppi_data"]
    for key, codes_to_answers in list(ppi_data.items()):
        ppi_results[key] = _get_validation_result(key, codes_to_answers).to_json()
    return json.dumps({"ppi_results": ppi_results})


def _sanity_check_codebook():
    if not CodeDao().get_code(PPI_SYSTEM, EMAIL_QUESTION_CODE):
        raise RuntimeError(f"No question code found for {EMAIL_QUESTION_CODE}; import codebook.")


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
        return {"tests_count": self.tests_count, "errors_count": self.errors_count, "error_messages": self.messages}


def _get_validation_result(key, codes_to_answers):
    result = _ValidationResult()
    with ParticipantSummaryDao().session() as session:
        # Get summary by email or phone
        if "@" not in key:
            summaries = session.query(ParticipantSummary).filter(ParticipantSummary.loginPhoneNumber == key).all()
        else:
            summaries = session.query(ParticipantSummary).filter(ParticipantSummary.email == key).all()

    if not summaries:
        result.add_error(f"No ParticipantSummary found for {key}.")
        return result
    if len(summaries) > 1:
        result.add_error(f"{len(summaries)} ParticipantSummary values found for {key}.")
        return result
    participant_id = summaries[0].participantId

    code_dao = CodeDao(use_cache=False)
    qra_dao = QuestionnaireResponseAnswerDao()
    with qra_dao.session() as session:
        for code_string, answer_string in list(codes_to_answers.items()):
            result.tests_count += 1

            question_code = code_dao.get_code(PPI_SYSTEM, code_string)
            if not question_code:
                result.add_error(f"Could not find question code {code_string}, skipping answer {answer_string}.")
                continue
            if question_code.codeType != CodeType.QUESTION and question_code.codeType != CodeType.TOPIC:
                result.add_error(f"Code {code_string} type is {question_code.codeType}, not QUESTION; skipping.")
                continue

            qras = qra_dao.get_current_answers_for_concepts(session, participant_id, [question_code.codeId])
            qra_values = set()
            for qra in qras:
                try:
                    qra_values.add(_get_value_for_qra(qra, question_code, code_dao, session))
                except ValueError as e:
                    result.add_error(e.message)
                    continue

            if answer_string:
                expected_values = set(_boolean_to_lower(v.strip()) for v in answer_string.split("|"))
            else:
                expected_values = set()
            if expected_values != qra_values:
                result.add_error(
                    f"{question_code.value}: Expected {_format_values(expected_values)}, \
                    found {_format_values(qra_values)}."
                )
    return result


def _format_values(values):
    if not values:
        return "no answer"
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
                f"Unexpected value {code.value} with non-PPI system {code.system} for question {question_code}."
            )
        return code.value
    raise ValueError(f"Answer for question {question_code} has no value set.")


def _boolean_to_lower(value):
    if value.lower() == "true" or value.lower() == "false":
        return value.lower()
    return value
