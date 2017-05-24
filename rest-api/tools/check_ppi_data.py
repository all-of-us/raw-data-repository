"""Asserts that questionnaire response answers in the database match values specified in a
CSV input file.
"""
import csv
import logging
import os
import sys

import dao.database_factory
from code_constants import PPI_SYSTEM, EMAIL_QUESTION_CODE
from dao.code_dao import CodeDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.questionnaire_response_dao import QuestionnaireResponseAnswerDao
from model.code import CodeType
from tools.main_util import get_parser, configure_logging

class PPIChecker(object):

  def __init__(self):
    self.num_errors = 0

  def log_error(self, message, *args):
    logging.error(message, *args)
    self.num_errors += 1

  def get_person_dicts(self, input_file):
    """Constructs dicts for each person found in the spreadsheet.

    The first column contains question codes. Each other column is for a participant, with the
    cell values containing values for answers to the questions indicated in the first column
    (which could be code values, strings, dates, or numbers.)
    """
    person_dicts = []
    question_code_ids = set()
    code_dao = CodeDao()
    with open(input_file) as fp:
      csv_file = csv.reader(fp)
      row_number = 0
      for row in csv_file:
        row_number += 1
        if row_number == 1:
          if len(row) == 0 or row[0] != EMAIL_QUESTION_CODE:
            self.log_error('First row should have question code %s; exiting.', EMAIL_QUESTION_CODE)
            sys.exit(-1)
          # Allocate person_dicts.
          for i in range(1, len(row)):
            person_dicts.append({})
        elif len(row) == 0:
          continue
        question_code_value = row[0].strip()
        if not question_code_value:
          self.log_error('No question code found for row %d; skipping.' % row_number)
          continue
        question_code = code_dao.get_code(PPI_SYSTEM, question_code_value)
        if not question_code:
          if row_number == 1:
            self.log_error('No question code found for ConsentPII_EmailAddress; import codebook.')
            sys.exit(-1)
          self.log_error('Could not find question code %s on row %d; skipping.', 
                         question_code_value, row_number)
          continue
        if question_code.codeType != CodeType.QUESTION:
          self.log_error('Code %s on row %d is of type %s, not QUESTION; skipping.', 
                         question_code_value, row_number, question_code.codeType)
          continue
        if row_number != 1:
          # Add all the non-email question codes to question_code_ids
          question_code_ids.add(question_code.codeId)
        for i in range(1, len(row)):
          value = row[i].strip()
          if value:
            # TODO: validate values based on answer type here
            person_dicts[i - 1][question_code.codeId] = value
          elif row_number == 1:
            self.log_error('No email address found for column %d! Aborting.', (i + 1))
            sys.exit(-1)
      return person_dicts, question_code_ids

  def check_ppi(self, person_dicts, question_code_ids):
    code_dao = CodeDao()
    participant_summary_dao = ParticipantSummaryDao()
    email_code_id = code_dao.get_code(PPI_SYSTEM, EMAIL_QUESTION_CODE).codeId
    for person_dict in person_dicts:
      email = person_dict[email_code_id]
      summaries = participant_summary_dao.get_by_email(email)
      if not summaries:
        self.log_error('No participant found with email %s.', email)
      elif len(summaries) > 1:
        self.log_error('Multiple participants found with email %s', email)
      else:
        self.check_person_dict(email, summaries[0].participantId, person_dict,
                               question_code_ids)

  def get_value_for_qra(self, qra, email, question_code, code_dao):
    if qra.valueString:
      return qra.valueString
    if qra.valueInteger:
      return str(qra.valueInteger)
    if qra.valueDecimal:
      return str(qra.valueDecimal)
    if qra.valueBoolean:
      return str(qra.valueBoolean)
    if qra.valueDate:
      return qra.valueDate.isoformat()
    if qra.valueDateTime:
      return qra.valueDateTime.isoformat()
    if qra.valueCodeId:
      code = code_dao.get(qra.valueCodeId)
      if code.system != PPI_SYSTEM:
        self.log_error('Unexpected value %s with non-PPI system %s for question %s for '
                       + 'participant %s', code.value, code.system, question_code, email)
        return None
      return code.value
    self.log_error('Answer for question %s for participant %s has no value set', question_code,
                   email)
    return None

  def check_person_dict(self, email, participant_id, person_dict, question_code_ids):
    code_dao = CodeDao()
    qra_dao = QuestionnaireResponseAnswerDao()
    with qra_dao.session() as session:
      for question_code_id in question_code_ids:
        question_code = code_dao.get(question_code_id)
        qras = qra_dao.get_current_answers_for_concepts(session, participant_id, [question_code_id])
        answer_string = person_dict.get(question_code_id)
        if qras:
          qra_values = set([self.get_value_for_qra(qra, participant_id, question_code.value,
                                                   code_dao) for qra in qras])
          if answer_string:
            values = set(value.strip() for value in answer_string.split('|'))
            if values != qra_values:
              self.log_error('Expected answers %s for question %s for participant %s, found: %s',
                            values, question_code.value, email, qra_values)
          else:
            self.log_error('Expected no answer for question %s for participant %s, found: %s',
                          question_code.value, email, qra_values)
        else:
          if answer_string:
            values = set(answer_string.split('|'))
            self.log_error('Expected answers %s for question %s for participant %s, found none',
                      values, question_code.value, email)

  def run(self, input_file):
    person_dicts, question_code_ids = self.get_person_dicts(input_file)
    self.check_ppi(person_dicts, question_code_ids)
    logging.info('Finished with %d errors.' % self.num_errors)

def main(args):
  dao.database_factory.DB_CONNECTION_STRING = os.environ['DB_CONNECTION_STRING']
  ppi_checker = PPIChecker()
  ppi_checker.run(args.file)

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--file', help='File name containing the input CSV',
                      required=True)
  main(parser.parse_args())
