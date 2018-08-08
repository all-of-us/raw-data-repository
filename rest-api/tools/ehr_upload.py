"""Get site/participant info for a given organization to fetch consent forms from a bucket
and upload to a new organization bucket by the same name.
eg: tools/ehr_upload.sh --organization AZ_TUCSON --bucket ptc-uploads-pmi-drc-api-prod
"""
import subprocess
from dao import database_factory
from main_util import get_parser, configure_logging
from sqlalchemy import text


def main(args):
  with database_factory.get_database().session() as session:
    get_participants_under_sites(
      session, args.organization, args.source_bucket, args.destination_bucket)

    get_participants_without_site_pairing(
        session, args.organization, args.source_bucket, args.destination_bucket)

def get_participants_under_sites(session, organization, source_bucket, destination_bucket):
  sql = """
  select p.participant_id, google_group from participant p left join site s on
  p.site_id = s.site_id
  left join participant_summary summary on p.participant_id = summary.participant_id
  where p.organization_id in (select organization_id from organization where external_id = '{}')
  and summary.consent_for_electronic_health_records = 1
  and summary.consent_for_study_enrollment = 1;
   """.format(organization)
  cursor = session.execute(text(sql))
  try:
    results = cursor.fetchall()
    results = [(int(i), str(k)) for i, k in results]
    if results:
      for participant, google_group in results:
        gsutil = "gsutil -m cp gs://" + source_bucket + "/Participant/P" + str(
          participant) + "/* " + "gs://" + destination_bucket + "/Participant/" + \
          google_group + "/P" + str(participant) + "/"

        system_call = subprocess.call(gsutil, shell=True)
        if system_call == 0:
          print "Successfully moved folder " + google_group + '/' + str(participant)
        elif system_call == 1:
          print "There was an error moving folder " + google_group + '/' + str(participant)
        else:
          print "System error message: " + system_call
  finally:
    cursor.close()


def get_participants_without_site_pairing(session, organization, source_bucket, destination_bucket):
  sql = """
    select participant_id from participant p
    right join organization o on o.organization_id = p.organization_id
    where p.site_id is NULL and o.external_id = '{}';
   """.format(organization)
  cursor = session.execute(text(sql))
  try:
    results = cursor.fetchall()
    if results:
      results = [int(i) for i, in results]
      for participant in results:
        gsutil = "gsutil -m cp gs://" + source_bucket + "/Participant/P" + str(
          participant) + "/* " + "gs://" + destination_bucket + "/Participant/" + \
                 "no_site_pairing" + "/P" + str(participant) + "/"

        system_call = subprocess.call(gsutil, shell=True)
        if system_call == 0:
          print "Successfully moved folder " + "no_site_pairing" + '/P' + str(participant)
        elif system_call == 1:
          print "There was an error moving folder " + "no_site_pairing" + '/P' + str(participant)
        else:
          print "System error message: " + system_call
  finally:
    cursor.close()


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--organization', help='The organization to find participants and sites for',
                      required=True)
  parser.add_argument('--source_bucket', help='The bucket to read from in one env.'
                      ' i.e. ptc-uploads-pmi-drc-api-prod', required=True)
  parser.add_argument('--destination_bucket', help='The bucket to write to in one env.'
                      'i.e. ptc-uploads-pmi-drc-api-prod', required=True)

  main(parser.parse_args())
