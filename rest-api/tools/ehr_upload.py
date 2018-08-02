"""Get site/participant info for a given awardee to fetch consent forms from a bucket
and upload to a new awardee bucket by the same name.
eg: tools/ehr_upload.sh --awardee AZ_TUCSON --bucket ptc-uploads-pmi-drc-api-prod
    --account me@pmi-ops.org --project all-of-us-rdr-prod
"""
import subprocess
from dao import database_factory
from main_util import get_parser, configure_logging
from sqlalchemy import text


def main(args):
  with database_factory.get_database().session() as session:
    get_participants_under_sites(session, args.awardee, args.source_bucket, args.destination_bucket)


def get_participants_under_sites(session, awardee, source_bucket, destination_bucket):
  sql = """ select p.participant_id, s.google_group from participant p, site s
      where  p.site_id in (
        select site_id from site where hpo_id in (select hpo_id from hpo where name = '{}'))
      order by s.site_name;
      """.format(awardee)
  cursor = session.execute(text(sql))
  try:
    results = cursor.fetchall()
    results = results[:3]  # @TODO: For testing purposes
    results = [(int(i), str(k)) for i, k in results]
    if results:
      for participant, google_group in results:
        gsutil = "gsutil -m cp -r gs://" + source_bucket + "/Participant/P" + str(
          participant) + "/* " + "gs://" + destination_bucket + "/Participant/" + \
          google_group + "/P" + str(participant)

        system_call = subprocess.call(gsutil, shell=True)
        if system_call == 0:
          print "Successfully moved folder " + google_group + '/' + str(participant)
        elif system_call == 1:
          print "There was an error moving folder " + google_group + '/' + str(participant)
  finally:
    cursor.close()


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--awardee', help='The awardee to find participants and sites for',
                      required=True)
  parser.add_argument('--source_bucket', help='The bucket to read from in one env.'
                      ' i.e. ptc-uploads-pmi-drc-api-prod', required=True)
  parser.add_argument('--destination_bucket', help='The bucket to write to in one env.'
                      'i.e. ptc-uploads-pmi-drc-api-prod', required=True)

  main(parser.parse_args())
