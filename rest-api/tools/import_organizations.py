"""Imports awardees / HPOs, organizations, and sites into the database.

Usage:
  tools/import_organizations.sh [--account <USER>@pmi-ops.org --project <PROJECT>] \
      --awardee_file <AWARDEE CSV> --organization_file <ORGANIZATION CSV> \
      --site_file <SITE CSV> [--dry_run]

  The CSV files originate from the Google Sheets found here:

  https://docs.google.com/spreadsheets/d/1CcIGRV0Bd6BIz7PeuvrV6QDGDRQkp83CJUkWAl-fG58/edit
"""


import csv
import logging

from dao.hpo_dao import HPODao
from dao.organization_dao import OrganizationDao
from dao.site_dao import SiteDao
from model.hpo import HPO
from model.organization import Organization
from model.site import Site
from participant_enums import OrganizationType
from main_util import get_parser, configure_logging

def import_awardees(awardees_file):
  skip_count = 0
  new_or_updated_count = 0
  matched_count = 0
  logging.info('Importing awardees from %r.', awardees_file)
  with open(awardees_file, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    hpo_dao = HPODao()
    existing_hpo_map = {hpo.name: hpo for hpo in hpo_dao.get_all()}
    hpo_id = len(existing_hpo_map)
    with hpo_dao.session() as session:
      for row in reader:
        hpo = _hpo_from_row(row)
        if awardee is None:
          skip_count += 1
          continue
        changed = _upsert_hpo(hpo, existing_hpo_map.get(hpo.name),
                              hpo_dao, session, args.dry_run)
        if changed:
          new_or_updated_count += 1
        else:
          matched_count += 1
    logging.info('Done importing awardees %s: %d skipped, %d new/updated, %d not changed',
                 ' (dry run)' if args.dry_run else '', skip_count, new_or_updated_count, matched_count)
        if existing_hpo:
          existing_type = existing_hpo.organizationType or OrganizationType.UNSET
          if (existing_hpo.displayName != display_name or
              existing_type != organization_type):
            existing_hpo_dict = existing_hpo.asdict()
            existing_hpo.displayName = display_name
            existing_hpo.organizationType = organization_type
            hpo_dao.update_with_session(session, existing_hpo)
            logging.info('Updating awardee: old = %s, new = %s', existing_hpo_dict,
                         existing_hpo.asdict())
        else:
          hpo = HPO(hpoId=hpo_id, name=name, displayName=display_name,
                    organizationType=organization_type)
          hpo_dao.insert_with_session(session, hpo)
          logging.info('Inserting awardee: %s', hpo.asdict())
          hpo_id += 1
  logging.info('Done.')

def _hpo_from_row(row):
  return HPO(name=row['Awardee ID'],
             displayName=row['Name'],
             organizationType=OrganizationType(row['Type']))


def import_sites(sites_file):
  _GOOGLE_GROUP_SUFFIX = '@prod.pmi-ops.org'

def main(args):
  skip_count = 0
  new_or_updated_count = 0
  matched_count = 0
  logging.info('Importing from %r.', args.file)
  with open(args.file, 'r') as csv_file:
    sites_reader = csv.DictReader(csv_file)
    hpo_dao = HPODao()
    site_dao = SiteDao()
    existing_site_map = {site.googleGroup: site for site in site_dao.get_all()}

    with site_dao.session() as session:
      for row in sites_reader:
        site = _site_from_row(row, hpo_dao)
        if site is None:
          skip_count += 1
          continue
        changed = _upsert_site(
            site, existing_site_map.get(site.googleGroup), site_dao, session, args.dry_run)
        if changed:
          new_or_updated_count += 1
        else:
          matched_count += 1

  logging.info(
      'Done%s. %d skipped, %d sites new/updated, %d sites not changed.',
      ' (dry run)' if args.dry_run else '', skip_count, new_or_updated_count, matched_count)


def _site_from_row(row, hpo_dao):
  hpo_name = row['HPO Site ID']
  hpo = hpo_dao.get_by_name(hpo_name)
  if not hpo:
    logging.error('Invalid HPO %r; skipping %s.', hpo_name, row)
    return None
  mayolink_client_num_str = row['MayoLINK Client #']
  google_group = row['Google Group Email Address']
  if not google_group.endswith(_GOOGLE_GROUP_SUFFIX):
    logging.error(
        'Invalid google group: %r does not end with %r; skipping %s.',
        google_group, _GOOGLE_GROUP_SUFFIX, row)
    return None
  google_group_prefix = google_group[:-len(_GOOGLE_GROUP_SUFFIX)].lower()

  return Site(siteName=row['Site'],
              mayolinkClientNumber=(int(mayolink_client_num_str) if mayolink_client_num_str
                                    else None),
              googleGroup=google_group_prefix,
              hpoId=hpo.hpoId)


def _upsert_site(site, existing_site, site_dao, session, dry_run):
  site_dict = site.asdict()

  if existing_site:
    existing_site_dict = existing_site.asdict()
    existing_site_dict['siteId'] = None
    if existing_site_dict == site_dict:
      logging.info('Not updating %s.', site_dict['siteName'])
      return False
    else:
      existing_site.siteName = site.siteName
      existing_site.mayolinkClientNumber = site.mayolinkClientNumber
      existing_site.hpoId = site.hpoId
      if not dry_run:
        site_dao.update_with_session(session, existing_site)
      logging.info(
          'Updating site: old = %s, new = %s', existing_site_dict, existing_site.asdict())
      return True
  else:
    logging.info('Inserting site: %s', site_dict)
    if not dry_run:
      site_dao.insert_with_session(session, site)
    return True


def main(args):

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
