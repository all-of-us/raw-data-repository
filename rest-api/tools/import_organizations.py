"""Imports awardees / HPOs, organizations, and sites into the database.

Usage:
  tools/import_organizations.sh [--account <USER>@pmi-ops.org --project <PROJECT>] \
      --awardee_file <AWARDEE CSV> --organization_file <ORGANIZATION CSV> \
      --site_file <SITE CSV> [--dry_run]

  The CSV files originate from the Google Sheets found here:

  https://docs.google.com/spreadsheets/d/1CcIGRV0Bd6BIz7PeuvrV6QDGDRQkp83CJUkWAl-fG58/edit
"""

import logging

from dateutil.parser import parse
from tools.csv_importer import CsvImporter
from dao.hpo_dao import HPODao
from dao.organization_dao import OrganizationDao
from dao.site_dao import SiteDao
from model.hpo import HPO
from model.organization import Organization
from participant_enums import OrganizationType
from main_util import get_parser, configure_logging

class HPOImporter(CsvImporter):

  def __init__(self):
    super(HPOImporter, self).__init__('awardee', HPODao(), 'hpoId', 'name',
                                      ['Awardee ID', 'Name', 'Type'])
    self.new_count = 0

  def _entity_from_row(self, row):


    return HPO(name=row['Awardee ID'],
               displayName=row['Name'],
               organizationType=OrganizationType(row['Type']))

  def _insert_entity(self, entity, existing_map, session, dry_run):
    # HPO IDs are not autoincremented by the database; manually set it here.
    entity.hpoId = len(existing_map) + self.new_count
    self.new_count += 1
    super(HPOImporter, self)._insert_entity(entity, existing_map, session, dry_run)


class OrganizationImporter(CsvImporter):

  def __init__(self):
    super(OrganizationImporter, self).__init__('organization', OrganizationDao(),
                                               'organizationId', 'externalId',
                                               ['Awardee ID', 'Organization ID', 'Name'])
    self.hpo_dao = HPODao()

  def _entity_from_row(self, row):
    hpo = self.hpo_dao.get_by_name(row['Awardee ID'])
    if hpo is None:
      logging.info('Invalid awardee ID %s importing organization %s', row['Awardee ID'],
                   row['Organization ID'])
      return None
    return Organization(externalId=row['Organization ID'],
                        displayName=row['Name'],
                        hpoId=hpo.hpoId)

class SiteImporter(CsvImporter):

  def __init__(self):
    super(SiteImporter, self).__init__('site', SiteDao(), 'siteId', 'googleGroup',
                                       ['Organization ID', 'Site ID / Google Group'])

    self.organization_dao = OrganizationDao()

  def _entity_from_row(self, row):
    organization = self.organization_dao.get_by_external_id(row['Organization ID'])
    if organization is None:
      logging.info('Invalid organization ID %s importing site %s', row['Organization ID'],
                   row['Site ID / Google Group'])
      return None
    parse(row['Anticipated launch date'])
    return None

def main(args):
  #pylint: disable=unused-argument
  pass

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--awardee_file', help='Filename containing awardee CSV to import',
                      required=True)
  parser.add_argument('--organization_file', help='Filename containing organization CSV to import',
                      required=True)
  parser.add_argument('--site_file', help='Filename containing site CSV to import',
                      required=True)
  parser.add_argument('--dry_run', help='Read CSV and check for diffs against database.',
                      action='store_true')
  main(parser.parse_args())
