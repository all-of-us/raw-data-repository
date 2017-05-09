"""Imports HPOs into the database, using CSV as input.
"""

import csv
import logging

from dao.hpo_dao import HPODao
from dao.site_dao import SiteDao
from model.site import Site
from tools.main_util import get_parser, configure_logging

def main(args):
  with open(args.file, 'r') as csv_file:
    reader = csv.DictReader(csv_file)
    hpo_dao = HPODao()
    site_dao = SiteDao()
    existing_site_map = {site.googleGroup: site for site in site_dao.get_all()}
    with site_dao.session() as session:
      for row in reader:
        hpo_name = row['HPO Site ID']
        hpo = hpo_dao.get_by_name(hpo_name)
        if not hpo:
          logging.error("Invalid HPO: %s; skipping.", hpo_name)
          continue;
        mayolink_client_num_str = row['MayoLINK Client #']
        site = Site(consortiumName=row['Group (Consortium)'],
                    siteName=row['Site'],
                    mayolinkClientNumber=(int(mayolink_client_num_str) if mayolink_client_num_str
                                          else None),
                    googleGroup=row['Google Group Email Address'],
                    hpoId=hpo.hpoId)
        site_dict = site.asdict()
        existing_site = existing_site_map.get(site.googleGroup)
        if existing_site:
          existing_site_dict = existing_site.asdict()
          existing_site_dict['siteId'] = None
          if existing_site_dict != site_dict:
            existing_site.consortiumName = site.consortiumName
            existing_site.siteName = site.siteName
            existing_site.mayolinkClientNumber = site.mayolinkClientNumber
            existing_site.hpoId = site.hpoId
            site_dao.update_with_session(session, existing_site)
            logging.info('Updating HPO: old = %s, new = %s', existing_site_dict,
                         existing_site.asdict())
        else:
          site_dao.insert_with_session(session, site)
          logging.info('Inserting HPO: %s', site_dict)
  logging.info('Done.')

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--file', help='Filename containing CSV to import',
                      required=True)
  main(parser.parse_args())
