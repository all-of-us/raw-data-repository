"""Get site/participant info for a given organization to fetch consent forms from a bucket
and upload to a new organization bucket by the same name.
eg: tools/ehr_upload.sh --organization AZ_TUCSON --bucket ptc-uploads-pmi-drc-api-prod
"""
import csv
import logging
import subprocess
import shlex
import requests
import os
from sqlalchemy import text
from dao import database_factory
from main_util import configure_logging


FILE_URL = os.getenv('GAE_HPO_REPORT_URL')
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
  logging.info('url is => {}'.format(url))
  response = requests.get(url)
  logging.info('response is => {}'.format(response.iter_lines()))
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
    print row
    bucket_name = row.get(BUCKET_NAME)
    aggregate_id = row.get(AGGREGATING_ORG_ID)
    org_id = row.get(ORG_ID)
    org_status = row.get(ORG_STATUS)

    if org_status == 'Active':
      hpo_data[org_id] = {'aggregate_id': aggregate_id, 'bucket': bucket_name}

  logging.info('hpo_data is => {}'.format(hpo_data))
  _ensure_buckets(hpo_data)
  return hpo_data


def sync_ehr_consents():
  csv_reader = _fetch_csv_data(FILE_URL)
  csv_reader.next()
  hpo_data = read_hpo_report(csv_reader)
  logging.info('Reading data complete, beginning sync...')
  logging.info(hpo_data.items())
  for org, data in hpo_data.items():
    site_paired_sql, no_paired_sql = get_sql(org)
    logging.info('syncing participants for {}'.format(org))
    run_sql(data['bucket'], site_paired_sql, no_paired_sql)


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
  sync_ehr_consents()
