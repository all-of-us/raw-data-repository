"""Get site/participant info for a given organization to fetch consent forms from
ptc-uploads-all-of-us-rdr-prod (the default location for new participant uploads from PTC.
Syncs consent files to a new organization bucket by the same name in the Awardees consent bucket
i.e.aou179/hpo-site-xxx/Participant/<consent files>
To run: tools/ehr_upload.sh --account <pmi ops account> --project all-of-us-rdr-prod
"""
import StringIO
import csv
import logging
import shlex
import subprocess
import urllib2

from dao import database_factory
from main_util import configure_logging, get_parser
from sqlalchemy import text


SOURCE_BUCKET = 'ptc-uploads-all-of-us-rdr-prod'
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
    and s.google_group is not null
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


def _fetch_csv_data(file_url):
  response = urllib2.urlopen(file_url)
  return csv.DictReader(StringIO.StringIO(response.read()))


def _ensure_buckets(hpo_data):
  """Some organizations aggregating org ID is responsible for consent verification. This is a
  parent/child relationship(not seen elsewhere in the RDR). If bucket name is blank it is safe to
  assume the parent org (aggregate org id) bucket is to be used."""
  for _, _dict in hpo_data.items():
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


def sync_ehr_consents(spreadsheet_id):
  file_url = 'https://docs.google.com/spreadsheets/d/%(id)s/export?format=csv&id=%(id)s&gid=%(' \
             'gid)s' % {
               'id': spreadsheet_id,
               'gid': "0",
               }

  csv_reader = _fetch_csv_data(file_url)
  csv_reader.next()
  hpo_data = read_hpo_report(csv_reader)
  logging.info('Reading data complete, beginning sync...')
  for org, data in hpo_data.items()[:2]:
    site_paired_sql, no_paired_sql = get_sql(org)
    logging.info('syncing participants for {}'.format(org))
    run_sql(data['bucket'], site_paired_sql, no_paired_sql)


def run_gsutil(gsutil):
  system_call = subprocess.Popen(shlex.split(gsutil))
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
  parser.add_argument('--spreadsheet_id', help='The id of the Google Spreadsheet to use. i.e. '
                                               'All-hpos-report', required=True)
  args = parser.parse_args()
  sync_ehr_consents(args.spreadsheet_id)
