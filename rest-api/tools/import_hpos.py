"""Imports HPOs into the database, using CSV as input.
"""

import argparse
import csv
import logging
import sys

from dao.hpo_dao import HPODao
from main_util import get_parser, configure_logging
from model.hpo import HPO
from participant_enums import OrganizationType

def main(args):
  with open(args.file, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    hpo_dao = HPODao()
    existing_hpo_map = {hpo.name: hpo for hpo in hpo_dao.get_all()}
    hpo_id = len(existing_hpo_map)
    with hpo_dao.session() as session:
      for row in reader:
        name = row['Organization ID']
        display_name = row['Name']
        organization_type = OrganizationType(row['Type'])
        existing_hpo = existing_hpo_map.get(name)
        if existing_hpo:
          existing_type = existing_hpo.organizationType or OrganizationType.UNSET
          if (existing_hpo.displayName != display_name or
              existing_type != organization_type):
            existing_hpo_dict = existing_hpo.asdict()
            existing_hpo.displayName = display_name
            existing_hpo.organizationType = organization_type
            hpo_dao.update_with_session(session, existing_hpo)
            logging.info('Updating HPO: old = %s, new = %s', existing_hpo_dict,
                         existing_hpo.asdict())
        else:
          hpo = HPO(hpoId=hpo_id, name=name, displayName=display_name,
                    organizationType=organization_type)
          hpo_dao.insert_with_session(session, hpo)
          logging.info('Inserting HPO: %s', hpo.asdict())
          hpo_id += 1
  logging.info('Done.')

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--file', help='Filename containing CSV to import',
                      required=True)
  main(parser.parse_args())
