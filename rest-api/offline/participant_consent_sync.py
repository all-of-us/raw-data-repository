"""Get site/participant info for a given organization to fetch consent forms from a bucket
and upload to a new organization bucket by the same name.
eg: tools/ehr_upload.sh --organization AZ_TUCSON --bucket ptc-uploads-pmi-drc-api-prod
"""
import csv
import io
import requests
import urllib2
import httplib
import subprocess
import shlex
from dao import database_factory
from sqlalchemy import text
from unicode_csv import UnicodeDictReader


SOURCE_BUCKET = 'ptc-uploads-all-of-us-rdr-prod'
BUCKET_NAME = 'Bucket Name'
AGGREGATING_ORG_ID = 'Aggregating Org ID'
ORG_ID = 'Org ID'
ORG_STATUS = 'Org Status'
FILE_URL = 'https://docs.google.com/spreadsheets/d/1Fm0RsnMCvR6RTxEOkRKC2IrSy4dcQPbETdDeCSKG4O8/edit' \
       '#gid=0?output=csv'

spreadshhet_id = '1Fm0RsnMCvR6RTxEOkRKC2IrSy4dcQPbETdDeCSKG4O8'


# read all hpo's report (which needs to be protected)
# get Org ID and Bucket Name. If no Bucket Name, get it from Aggregating Org ID (sort of parent)
def _fetch_csv_data(spreadsheet_id, spreadsheet_gid):
  url = 'https://docs.google.com/spreadsheets/d/%(id)s/export?format=csv&id=%(id)s&gid=%(gid)s' % {
    'id': spreadsheet_id,
    'gid': spreadsheet_gid,
    }
  # response = urllib2.urlopen(url)
  response = requests.get(url)
  if response.code != httplib.OK:  # urllib2 already raises urllib2.HTTPError for some of these.
    raise RuntimeError('Error fetching %r: response %s.' % (url, response.status))
  return response.read()



def read_hpo_report(file_url):
  hpo_data = {}

  with open(file_url, 'r') as f:
    reader = UnicodeDictReader(f)
    reader.next()
    for row in reader:
      bucket_name = row.get(BUCKET_NAME)
      aggregate_id = row.get(AGGREGATING_ORG_ID)
      org_id = row.get(ORG_ID)
      org_status = row.get(ORG_STATUS)

      if not bucket_name:
        pass
      hpo_data[org_id] = {'aggregate_id': aggregate_id, 'org_status': org_status,
                          'bucket': bucket_name}

  return hpo_data


def sync_ehr_consents():
  # read csv (link)
  x = _fetch_csv_data(spreadshhet_id, 0)
  # print x
  hpo_data = read_hpo_report(x)
  print x
  # with database_factory.get_database().session() as session:
  #   get_participants_site_pairing(
  #     session, org_status, SOURCE_BUCKET, bucket_name)
  #
  #   get_participants_without_site_pairing(
  #     session, org_status, SOURCE_BUCKET, bucket_name)


def get_participants_site_pairing(session, organization, source_bucket, destination_bucket):
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
    if results:
      results = [(int(i), str(k)) for i, k in results]
      for participant, google_group in results:
        gsutil = "gsutil -m rsync gs://" + source_bucket + "/Participant/P" + str(
          participant) + "/* " + "gs://" + destination_bucket + "/Participant/" + \
                 google_group + "/P" + str(participant) + "/"

        gsutil = shlex.split(str(gsutil))
        system_call = subprocess.Popen(gsutil)
        system_call.communicate()[0]
        if system_call.returncode == 0:
          print "Successfully moved folder " + google_group + '/P' + str(participant)
        else:
          print "There was an error moving folder " + google_group + '/P' + str(participant)
          print "return code is : " + str(system_call.returncode)
    else:
      print "No participants paired with sites found for organization: " + organization
  finally:
    cursor.close()


def get_participants_without_site_pairing(session, organization, source_bucket, destination_bucket):
  sql = """
    select p.participant_id from participant p
    left join participant_summary summary on p.participant_id = summary.participant_id
    where p.site_id is NULL and p.organization_id in (
    select organization_id from organization where external_id = '{}')
    and summary.consent_for_electronic_health_records = 1
    and summary.consent_for_study_enrollment = 1;
   """.format(organization)
  cursor = session.execute(text(sql))
  try:
    results = cursor.fetchall()
    if results:
      results = [int(i) for i, in results]
      for participant in results:
        gsutil = "gsutil -m rsync gs://" + source_bucket + "/Participant/P" + str(
          participant) + "/* " + "gs://" + destination_bucket + "/Participant/" + \
                 "no_site_pairing" + "/P" + str(participant) + "/"

        gsutil = shlex.split(str(gsutil))
        system_call = subprocess.Popen(gsutil)
        system_call.communicate()[0]
        if system_call.returncode == 0:
          print "Successfully moved folder " + "no_site_pairing" + '/P' + str(participant)
        else:
          print "There was an error moving folder " + "no_site_pairing" + '/P' + str(participant)
          print "return code is : " + str(system_call.returncode)
    else:
      print "No participants found that are not paired with sites and paired with organization: " \
            + organization
  finally:
    cursor.close()
