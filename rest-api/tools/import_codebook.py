"""Imports a codebook into the database, using JSON found at a specified path.
"""

import argparse
import dao.database_factory
import json
import os

from dao.code_dao import CodeBookDao

def main(args):
  dao.database_factory.DB_CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING",
                                                        "mysql+mysqldb://root:root@localhost/rdr")
  with open(args.file) as f:
    codebook_json = json.load(f)
    CodeBookDao().import_codebook(codebook_json)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('--file', help='Path to the JSON representation of the codebook.',
                      required=True)

main(parser.parse_args())