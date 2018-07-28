"""Get site/participant info for a given awardee to fetch consent forms from a bucket
and upload to awardee bucket."""
from dao import database_factory
from main_util import get_parser, configure_logging
from sqlalchemy import text


def main(args):
  sql ="""
      select p.participant_id, s.site_name from participant p, site s
      where  p.site_id in (
        select site_id from site where hpo_id in (select hpo_id from hpo where name = 'AZ_TUCSON'))
      order by s.site_name;
      """

  with database_factory.get_database().session() as session:
    cursor = session.execute(text(sql))
    try:
      results = cursor.fetchall()
      print results
      # for result in results:
      #   writer.writerow(result)
    finally:
      cursor.close()


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--awardee', help='The awardee to find participants and sites for',
                      required=True)

  main(parser.parse_args())
