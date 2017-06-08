import csv
import logging

from dao.hpo_dao import HPODao
from dao.site_dao import SiteDao
from model.site import Site
from tools.main_util import get_parser, configure_logging

_GOOGLE_GROUP_SUFFIX = '@prod.pmi-ops.org'


def main(args):
  error_count = 0
  new_or_updated_count = 0
  matched_count = 0
  with open(args.file, 'r') as csv_file:
    sites_reader = csv.DictReader(csv_file)
    hpo_dao = HPODao()
    site_dao = SiteDao()
    existing_site_map = {site.googleGroup: site for site in site_dao.get_all()}

    with site_dao.session() as session:
      for row in sites_reader:
        site = _site_from_row(row, hpo_dao)
        if site is None:
          error_count += 1
          continue
        changed = _upsert_site(
            site, existing_site_map.get(site.googleGroup), site_dao, session, args.dry_run)
        if changed:
          new_or_updated_count += 1
        else:
          matched_count += 1

  logging.info(
      'Done%s. %d errors, %d sites new/updated, %d sites not changed.',
      ' (dry run)' if args.dry_run else '', error_count, new_or_updated_count, matched_count)


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
  google_group_prefix = google_group[0:len(google_group) - len(_GOOGLE_GROUP_SUFFIX)].lower()

  return Site(consortiumName=row['Group (Consortium)'],
              siteName=row['Site'],
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
      logging.info('Not updating existing matching site %s.', site_dict['siteName'])
      return False
    else:
      existing_site.consortiumName = site.consortiumName
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


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--file', help='Filename containing CSV to import',
                      required=True)
  parser.add_argument('--dry_run', help='Read CSV and check for diffs against database.',
                      action='store_true')
  main(parser.parse_args())
