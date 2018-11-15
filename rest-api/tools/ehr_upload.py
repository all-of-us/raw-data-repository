# """Get site/participant info for a given organization to fetch consent forms from a bucket
# and upload to a new organization bucket by the same name.
# eg: tools/ehr_upload.sh --organization AZ_TUCSON --bucket ptc-uploads-pmi-drc-api-prod
# """
# import subprocess
# import shlex
# from dao import database_factory
# from main_util import get_parser, configure_logging
# from sqlalchemy import text
#
#
# def main(args):
#   with database_factory.get_database().session() as session:
#     get_participants_under_sites(
#       session, args.organization, args.source_bucket, args.destination_bucket)
#
#     get_participants_without_site_pairing(
#         session, args.organization, args.source_bucket, args.destination_bucket)
#
# def get_participants_under_sites(session, organization, source_bucket, destination_bucket):
#   sql = """
#   select p.participant_id, google_group from participant p left join site s on
#   p.site_id = s.site_id
#   left join participant_summary summary on p.participant_id = summary.participant_id
#   where p.organization_id in (select organization_id from organization where external_id = '{}')
#   and summary.consent_for_electronic_health_records = 1
#   and summary.consent_for_study_enrollment = 1;
#    """.format(organization)
#   cursor = session.execute(text(sql))
#   try:
#     results = cursor.fetchall()
#     if results:
#       results = [(int(i), str(k)) for i, k in results]
#       for participant, google_group in results:
#         gsutil = "gsutil -m cp gs://" + source_bucket + "/Participant/P" + str(
#           participant) + "/* " + "gs://" + destination_bucket + "/Participant/" + \
#                  google_group + "/P" + str(participant) + "/"
#         gsutil = shlex.split(str(gsutil))
#         system_call = subprocess.Popen(gsutil)
#         system_call.communicate()[0]
#         if system_call.returncode == 0:
#           print "Successfully moved folder " + google_group + '/P' + str(participant)
#         else:
#           print "There was an error moving folder " + google_group + '/P' + str(participant)
#           print "return code is : " + str(system_call.returncode)
#     else:
#       print "No participants paired with sites found for organization: " + organization
#   finally:
#     cursor.close()
#
#
# def get_participants_without_site_pairing(session, organization, source_bucket, destination_bucket):
#   sql = """
#     select p.participant_id from participant p
#     left join participant_summary summary on p.participant_id = summary.participant_id
#     where p.site_id is NULL and p.organization_id in (
#     select organization_id from organization where external_id = '{}')
#     and summary.consent_for_electronic_health_records = 1
#     and summary.consent_for_study_enrollment = 1;
#    """.format(organization)
#   cursor = session.execute(text(sql))
#   try:
#     results = cursor.fetchall()
#     if results:
#       results = [int(i) for i, in results]
#       for participant in results:
#         gsutil = "gsutil -m cp gs://" + source_bucket + "/Participant/P" + str(
#           participant) + "/* " + "gs://" + destination_bucket + "/Participant/" + \
#                  "no_site_pairing" + "/P" + str(participant) + "/"
#
#         gsutil = shlex.split(str(gsutil))
#         system_call = subprocess.Popen(gsutil)
#         system_call.communicate()[0]
#         if system_call.returncode == 0:
#           print "Successfully moved folder " + "no_site_pairing" + '/P' + str(participant)
#         else:
#           print "There was an error moving folder " + "no_site_pairing" + '/P' + str(participant)
#           print "return code is : " + str(system_call.returncode)
#     else:
#       print "No participants found that are not paired with sites and paired with organization: " \
#             + organization
#   finally:
#     cursor.close()

"""Get site/participant info for a given organization to fetch consent forms from a bucket
and upload to a new organization bucket by the same name.
eg: tools/ehr_upload.sh --organization AZ_TUCSON --bucket ptc-uploads-pmi-drc-api-prod
"""
import csv
import logging
import subprocess
import shlex
import requests
from sqlalchemy import text
from dao import database_factory
from main_util import get_parser, configure_logging




test_url = 'https://docs.google.com/spreadsheets/d/1Fm0RsnMCvR6RTxEOkRKC2IrSy4dcQPbETdDeCSKG4O8'
# SOURCE_BUCKET = 'ptc-uploads-all-of-us-rdr-prod'
SOURCE_BUCKET = 'ptc-uploads-pmi-drc-api-sandbox'
BUCKET_NAME = 'Bucket Name'
AGGREGATING_ORG_ID = 'Aggregating Org ID'
ORG_ID = 'Org ID'
ORG_STATUS = 'Org Status'


def get_sql(organization):
  site_pairing_sql = """
    select p.participant_id, google_group from participant p left join site s on
    p.site_id = s.site_id
    left join participant_summary summary on p.participant_id = summary.participant_id
    where p.organization_id in (select organization_id from organization where external_id = '{}')
    and summary.consent_for_electronic_health_records = 1
    and summary.consent_for_study_enrollment = 1;
     """.format(organization)

  no_site_pairing_sql = """
      select p.participant_id from participant p
      left join participant_summary summary on p.participant_id = summary.participant_id
      where p.site_id is NULL and p.organization_id in (
      select organization_id from organization where external_id = '{}')
      and summary.consent_for_electronic_health_records = 1
      and summary.consent_for_study_enrollment = 1;
     """.format(organization)
  return site_pairing_sql, no_site_pairing_sql


def _fetch_csv_data(url):
  response = requests.get(url)
  reader = csv.DictReader(response.iter_lines())
  return reader


def _ensure_buckets(hpo_data):
  """Some organizations aggregating org ID is responsible for consent verification. This is a
  parent/child relationship(not seen elsewhere in the RDR). If bucket name is blank it is safe to
  assume the parent org (aggregate org id) bucket is to be used."""
  for org_id, _dict in hpo_data.items():
    if _dict['bucket'] == '':
      parent = _dict['aggregate_id']
      _dict['bucket'] = hpo_data[parent]['bucket']


def read_hpo_report(csv_reader):
  hpo_data = {}

  for row in csv_reader:
    bucket_name = row.get(BUCKET_NAME)
    aggregate_id = row.get(AGGREGATING_ORG_ID)
    org_id = row.get(ORG_ID)
    org_status = row.get(ORG_STATUS)

    if org_status == 'Active':
      hpo_data[org_id] = {'aggregate_id': aggregate_id, 'bucket': bucket_name}

  _ensure_buckets(hpo_data)
  return hpo_data


def parse_url(file_url):
  logging.info('The file url => =>: {}'.format(file_url))
  return file_url


def sync_ehr_consents(file_url):
  file_url = parse_url(file_url)
  csv_reader = _fetch_csv_data(file_url)
  csv_reader.next()
  hpo_data = read_hpo_report(csv_reader)
  for org, data in hpo_data.items():
    site_paired_sql, no_paired_sql = get_sql(org)
    # run_sql(data['bucket'], site_paired_sql, no_paired_sql)
    run_sql('fake-ehr-test-uploads/test_consent_upload', site_paired_sql, no_paired_sql)
  logging.info('finished importing data')
  logging.info('beginning gsutil sync')
  logging.info('File url is : {}'.format(file_url))
  logging.info('File url is : {}'.format(test_url))


def run_gsutil(gsutil):
  gsutil = shlex.split(str(gsutil))
  system_call = subprocess.Popen(gsutil)
  system_call.communicate()[0]


def run_sql(destination_bucket, site_pairing_sql, no_site_pairing_sql):
  with database_factory.make_server_cursor_database().session() as session:
    cursor = session.execute(text(site_pairing_sql))
    results = cursor.fetchall()
    results = [(int(i), str(k)) for i, k in results]
    for participant, google_group in results:
      gsutil = "gsutil -m rsync gs://" + SOURCE_BUCKET + "/Participant/P" + str(
        participant) + "/* " + "gs://" + destination_bucket + "/Participant/" + \
               google_group + "/P" + str(participant) + "/"

      run_gsutil(gsutil)
    cursor.close()

    cursor = session.execute(text(no_site_pairing_sql))
    results = cursor.fetchall()
    results = [int(i) for i, in results]
    for participant in results:
      gsutil = "gsutil -m rsync gs://" + SOURCE_BUCKET + "/Participant/P" + str(
        participant) + "/* " + "gs://" + destination_bucket + "/Participant/" + \
               "no_site_pairing" + "/P" + str(participant) + "/"

      run_gsutil(gsutil)
    cursor.close()


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--file_url', help='The url to google sheet (All HPOs report)', required=True)
  file_url = parser.parse_args()
  sync_ehr_consents(str(file_url))
  # sync_ehr_consents(test_url)
