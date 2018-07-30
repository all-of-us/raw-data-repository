"""Get site/participant info for a given awardee to fetch consent forms from a bucket
and upload to awardee bucket."""
from dao import database_factory
from main_util import get_parser, configure_logging
from sqlalchemy import text


def main(args):
  with database_factory.get_database().session() as session:
    if args.sites:
      get_sites_for_awardee(session, args.awardee)
    if args.participants:
      get_participants_under_sites(session, args.awardee)


def get_sites_for_awardee(session, awardee):
  sql = """ select site_name from site
      where  hpo_id in (select hpo_id from hpo where name = '{}')
      order by site_name;
      """.format(awardee)
  cursor = session.execute(text(sql))

  try:
    results = cursor.fetchall()
    results = results[:3]  # @TODO: For testing purposes
    results = [r[0] for r in results]
    results = [str(i).replace(" ", "_") for i in results]
    print results
  finally:
    cursor.close()


def get_participants_under_sites(session, awardee):
  sql = """ select p.participant_id, s.site_name from participant p, site s
      where  p.site_id in (
        select site_id from site where hpo_id in (select hpo_id from hpo where name = '{}'))
      order by s.site_name;
      """.format(awardee)
  cursor = session.execute(text(sql))
  try:
    results = cursor.fetchall()
    results = results[:3]  # @TODO: For testing purposes
    map(int, [r[0] for r in results])

    print results
  finally:
    cursor.close()


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--awardee', help='The awardee to find participants and sites for',
                      required=True)
  parser.add_argument('--sites', help='command line flag to get sites')
  parser.add_argument('--participants', help='command line flag to get participants')

  main(parser.parse_args())
