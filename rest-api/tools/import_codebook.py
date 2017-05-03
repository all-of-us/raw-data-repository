"""Imports a codebook into the database, using JSON found at a specified path.
"""

import argparse
import dao.database_factory
import json
import logging
import os
import sys

from dao.code_dao import CodeBookDao
from main_util import get_parser, configure_logging

def main(args):
  logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s: %(message)s')
  dao.database_factory.DB_CONNECTION_STRING = os.environ['DB_CONNECTION_STRING']
  with open(args.file) as f:
    codebook_json = json.load(f)
    CodeBookDao().import_codebook(codebook_json)

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--file', help='Path to the JSON representation of the codebook.',
                      required=True)

  main(parser.parse_args())
