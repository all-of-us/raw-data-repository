"""Imports questionnaires into the database, using JSON found in specified files
in a specified directory.
"""
import config
import json
import logging
import os

import dao.database_factory
from dao.questionnaire_dao import QuestionnaireDao
from tools.main_util import get_parser, configure_logging


def main(args):
  # We should never add codes when importing questionnaires.
  config.get_add_codes_if_missing = lambda: False
  dao.database_factory.DB_CONNECTION_STRING = os.environ['DB_CONNECTION_STRING']
  files = args.files.split(',')
  questionnaire_dao = QuestionnaireDao()
  for filename in files:
    with open(args.dir + filename) as f:
      questionnaire_json = json.load(f)
      questionnaire = QuestionnaireDao.from_client_json(questionnaire_json)
      questionnaire_dao.insert(questionnaire)
  logging.info("%d questionnaires imported." % len(files))

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--dir', help='Directory name containing questionnaires to import',
                      required=True)
  parser.add_argument('--files', help='File names of questionnaires to import',
                      required=True)
  main(parser.parse_args())
