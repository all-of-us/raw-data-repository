"""Imports fake participants into the database

Reads fake CSV data specifying attributes of the participants. This is used by HealthPro to insert
some participants in the database in non-prod environments that are expected to be there for testing
and training purposes.

This may create duplicate participants (it does not check for existing ones before importing).

Usage:
  tools/import_participants.sh --account $USER@pmi-ops.org --project all-of-us-rdr-stable \
      --file /tmp/testparticipants.csv
"""

# fhirclient makes sys.path edits on import which mask our client module, so make sure to import
# our client before importing fhirclient.
from client import Client, client_log

import csv
import logging
import fhirclient.models.questionnaire

from code_constants import LAST_NAME_QUESTION_CODE, FIRST_NAME_QUESTION_CODE, EMAIL_QUESTION_CODE
from code_constants import ZIPCODE_QUESTION_CODE, DATE_OF_BIRTH_QUESTION_CODE
from code_constants import GENDER_IDENTITY_QUESTION_CODE, CONSENT_FOR_STUDY_ENROLLMENT_MODULE
from code_constants import CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE
from code_constants import OVERALL_HEALTH_PPI_MODULE, LIFESTYLE_PPI_MODULE, THE_BASICS_PPI_MODULE
from code_constants import PPI_SYSTEM
from main_util import get_parser, configure_logging

ALL_MODULE_CODES = [CONSENT_FOR_STUDY_ENROLLMENT_MODULE,
                    CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE,
                    OVERALL_HEALTH_PPI_MODULE,
                    LIFESTYLE_PPI_MODULE,
                    THE_BASICS_PPI_MODULE]

ALL_QUESTION_CODES = [LAST_NAME_QUESTION_CODE, FIRST_NAME_QUESTION_CODE, EMAIL_QUESTION_CODE,
                      ZIPCODE_QUESTION_CODE, DATE_OF_BIRTH_QUESTION_CODE,
                      GENDER_IDENTITY_QUESTION_CODE]


def _get_questions(group):
  """Recursively find questions in ALL_QUESTION_CODES and populate a dict of question code to
  linkId."""
  result = {}
  if group.question:
    for question in group.question:
      if question.linkId and question.concept and len(question.concept) == 1:
        concept = question.concept[0]
        if concept.system == PPI_SYSTEM and concept.code in ALL_QUESTION_CODES:
          result[concept.code] = question.linkId
      if question.group:
        for sub_group in question.group:
          result.update(_get_questions(sub_group))
  return result


def _setup_questionnaires(client):
  """Verify that questionnaires exist for all the modules in ALL_MODULE_CODES, and construct
  a map from questionnaire ID and version to { question_code: linkId } for all question codes
  found in the questionnaire in ALL_QUESTION_CODES.
  """
  logging.info('Verifying that questionnaires exist.')
  questionnaire_to_questions = {}
  consent_questionnaire_id_and_version = None
  for module_code in ALL_MODULE_CODES:
    questionnaire = client.request_json('Questionnaire?concept=%s' % module_code, 'GET')
    questionnaire_id = questionnaire['id']
    version = questionnaire['version']
    if module_code == CONSENT_FOR_STUDY_ENROLLMENT_MODULE:
      consent_questionnaire_id_and_version = (questionnaire_id, version)
    if questionnaire_to_questions.get((questionnaire_id, version)):
      continue
    fhir_q = fhirclient.models.questionnaire.Questionnaire(questionnaire)
    questionnaire_to_questions[(questionnaire_id, version)] = _get_questions(fhir_q.group)
  return questionnaire_to_questions, consent_questionnaire_id_and_version


def _create_question_answer(link_id, answers):
  return {'linkId': link_id, 'answer': answers}


def _create_questionnaire_response(participant_id, q_id_and_version,
                                   questions_with_answers):
  qr_json = {'resourceType': 'QuestionnaireResponse',
             'status': 'completed',
             'subject': {'reference': 'Patient/%s' % participant_id},
             'questionnaire': {'reference':
                               'Questionnaire/%s/_history/%s' % (q_id_and_version[0],
                                                                 q_id_and_version[1])},
             'group': {}}
  if questions_with_answers:
    qr_json['group']['question'] = questions_with_answers
  return qr_json


def _string_answer(value):
  if not value:
    return None
  return [{"valueString": value}]


def _date_answer(value):
  if not value:
    return None
  return [{"valueDate": value}]


def _code_answer(code):
  if not code:
    return None
  return [{"valueCoding": {"system": PPI_SYSTEM, "code": code}}]


def _submit_questionnaire_response(client, participant_id, questionnaire_id_and_version,
                                   questions, answer_map):
  questions_with_answers = []
  for question_code, link_id in questions.iteritems():
    answer = answer_map.get(question_code)
    if answer:
      questions_with_answers.append(_create_question_answer(link_id, answer))
  if not questions_with_answers:
    # Don't submit a questionnaire with no questions answered.
    return
  qr_json = _create_questionnaire_response(participant_id, questionnaire_id_and_version,
                                           questions_with_answers)
  client.request_json('Participant/%s/QuestionnaireResponse' % participant_id, 'POST', qr_json)


def main(args):
  client = Client('rdr/v1', False, args.creds_file, args.instance)
  client_log.setLevel(logging.WARN)
  num_participants = 0
  questionnaire_to_questions, consent_questionnaire_id_and_version = _setup_questionnaires(client)
  consent_questions = questionnaire_to_questions[consent_questionnaire_id_and_version]
  logging.info('Importing participants from %r.', args.file)
  with open(args.file, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      import_participant(row, client, consent_questionnaire_id_and_version,
                         questionnaire_to_questions, consent_questions, reader)
      num_participants += 1
  logging.info('%d participants imported.' % num_participants)


def import_participant(row, client, consent_questionnaire_id_and_version,
                       questionnaire_to_questions, consent_questions,
                       reader=None):

  answer_map = {}
  answer_map[LAST_NAME_QUESTION_CODE] = _string_answer(row['last_name'])
  answer_map[FIRST_NAME_QUESTION_CODE] = _string_answer(row['first_name'])
  email = row.get('email')
  if not email:
    email = 'participant%s_%s@example.com' % (row['first_name'][-1], row['last_name'])
  hpo_id = row.get('hpo_siteid')
  participant_resource = {}
  if hpo_id:
    participant_resource['providerLink'] = [{
      "primary": True,
      "organization": {
        "display": None,
        "reference": "Organization/%s" % hpo_id
      }
    }]
  answer_map[EMAIL_QUESTION_CODE] = _string_answer(email)
  answer_map[ZIPCODE_QUESTION_CODE] = _string_answer(row['zip_code'])
  answer_map[DATE_OF_BIRTH_QUESTION_CODE] = _date_answer(row['date_of_birth'])
  answer_map[GENDER_IDENTITY_QUESTION_CODE] = _code_answer(row['gender_identity'])
  participant_response = client.request_json('Participant', 'POST', participant_resource)
  participant_id = participant_response['participantId']
  if not reader:
    client.request_json('Participant/%s' % participant_id, 'PUT', row,
                        headers={'If-Match': 'W/"1"'})

  _submit_questionnaire_response(client, participant_id, consent_questionnaire_id_and_version,
                                 consent_questions, answer_map)
  for questionnaire_id_and_version, questions in questionnaire_to_questions.iteritems():
    if questionnaire_id_and_version != consent_questionnaire_id_and_version:
      _submit_questionnaire_response(client, participant_id, questionnaire_id_and_version,
                                     questions, answer_map)
  if reader:
    logging.info(
      '%s created: (%r %r).',
      participant_id, row['first_name'], row['last_name'])
  else:
    logging.info(
      '%s created from (%r %r).',
      participant_id, row['first_name'], row['last_name'])


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--file', help='Path to the CSV file containing the participant data.',
                      required=True)
  parser.add_argument('--instance',
                      type=str,
                      help='The instance to hit, defaults to http://localhost:8080',
                      default='http://localhost:8080')
  parser.add_argument('--creds_file',
                      type=str,
                      help='Path to credentials JSON file.')
  main(parser.parse_args())
