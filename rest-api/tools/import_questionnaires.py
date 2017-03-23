"""Imports a codebook into the database, using JSON found at a specified path.
"""

import argparse
import dao.database_factory
import json
import logging
import os
import sys

from dao.questionnaire_dao import QuestionnaireDao
from model.questionnaire import Questionnaire

def main(args):
  logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s: %(message)s')
  dao.database_factory.DB_CONNECTION_STRING = os.environ['DB_CONNECTION_STRING']
  files = args.files.split(',')
  questionnaire_dao = QuestionnaireDao()
  for file in files:
    with open(args.dir + file) as f:
      questionnaire_json = json.load(f)
      questionnaire = Questionnaire.from_client_json(questionnaire_json)
      questionnaire_dao.insert(questionnaire)
  logging.info("%d questionnaires imported." % len(files))

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('--dir', help='Directory name containing questionnaires to import',
                      required=True)
  parser.add_argument('--files', help='File names of questionnaires to import',
                      required=True)
  main(parser.parse_args())
