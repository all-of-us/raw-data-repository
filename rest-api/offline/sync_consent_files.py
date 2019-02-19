"""
Sync Consent Files

Organize all consent files from PTSC source bucket into proper awardee buckets.
"""
import collections
import csv
import json
import StringIO

import sqlalchemy

from google.appengine.ext import deferred
import cloudstorage
import requests

from dao import database_factory


HPO_REPORT_CONFIG_GCS_PATH = '/all-of-us-rdr-sequestered-config-test/hpo-report-config-mixin.json'

SOURCE_BUCKET = 'ptc-uploads-all-of-us-rdr-prod'

COLUMN_BUCKET_NAME = 'Bucket Name'
COLUMN_AGGREGATING_ORG_ID = 'Aggregating Org ID'
COLUMN_ORG_ID = 'Org ID'
COLUMN_ORG_STATUS = 'Org Status'

ORG_STATUS_ACTIVE = 'Active'
DEFAULT_GOOGLE_GROUP = 'no_site_pairing'


OrgData = collections.namedtuple('OrgData', (
  'org_id',
  'aggregate_id',
  'bucket_name',
))


ParticipantData = collections.namedtuple('ParticipantData', (
  'participant_id',
  'google_group',
  'org_id',
))


def do_sync_consent_files():
  """
  entrypoint
  """
  sheet_id = _get_sheet_id()
  org_data_map = _load_org_data_map(sheet_id)
  for participant_data in _iter_participants_data():
    kwargs = {
      "source_bucket": SOURCE_BUCKET,
      "destination_bucket": org_data_map[participant_data.org_id].bucket_name,
      "participant_id": participant_data.participant_id,
      "google_group": participant_data.google_group or DEFAULT_GOOGLE_GROUP
    }
    deferred.defer(
      cloudstorage_copy_objects,
      '/{source_bucket}/Participant/P{participant_id}/'.format(**kwargs),
      '/{destination_bucket}/Participant/{google_group}/P{participant_id}/'.format(**kwargs)
    )


def _get_sheet_id():
  with cloudstorage.open(HPO_REPORT_CONFIG_GCS_PATH, 'r') as handle:
    hpo_config = json.load(handle)
  sheet_id = hpo_config.get('hpo_report_google_sheet_id')
  if sheet_id is None:
    raise ValueError("Missing config value: hpo_report_google_sheet_id")
  return sheet_id


class GoogleSheetCSVReader(csv.DictReader):

  def __init__(self, sheet_id, gid=0, *args, **kwds):
    self._sheet_id = sheet_id
    self._gid = gid
    response = requests.get(self._get_sheet_url(self._sheet_id, self._gid))
    csv.DictReader.__init__(self, StringIO.StringIO(response.text), *args, **kwds)

  @staticmethod
  def _get_sheet_url(sheet_id, gid):
    return (
      "https://docs.google.com/spreadsheets/d/{id}/export"
      "?format=csv"
      "&id={id}"
      "&gid={gid}"
    ).format(
      id=sheet_id,
      gid=gid
    )


def _load_org_data_map(sheet_id):
  # parse initial mapping
  org_data_map = {
    row.get(COLUMN_ORG_ID): OrgData(
      row.get(COLUMN_ORG_ID),
      row.get(COLUMN_AGGREGATING_ORG_ID),
      row.get(COLUMN_BUCKET_NAME)
    )
    for row in GoogleSheetCSVReader(sheet_id)
    if row.get(COLUMN_ORG_STATUS) == ORG_STATUS_ACTIVE
  }
  # apply transformations that require full map
  return {
    org_data.org_id: _org_data_with_bucket_inheritance_from_org_data(org_data_map, org_data)
    for org_data in org_data_map.values()
  }


def _org_data_with_bucket_inheritance_from_org_data(org_data_map, org_data):
  return OrgData(
    org_data.org_id,
    org_data.aggregate_id,
    (
      org_data.bucket_name
      if org_data.bucket_name
      else org_data_map[org_data.aggregate_id].bucket_name
    )
  )


PARTICIPANT_DATA_SQL = sqlalchemy.text("""
select
  participant.participant_id,
  site.google_group,
  organization.external_id
from participant
left join organization
  on participant.organization_id = organization.organization_id
left join site
  on participant.site_id = site.site_id
left join participant_summary summary
  on participant.participant_id = summary.participant_id
where participant.is_ghost_id is not true
  and summary.consent_for_electronic_health_records = 1
  and summary.consent_for_study_enrollment = 1
  and (
    summary.email is null
    or summary.email not like '%@example.com'
  )
""")


def _iter_participants_data():
  with database_factory.make_server_cursor_database().session() as session:
    for row in session.execute(PARTICIPANT_DATA_SQL):
      yield ParticipantData(*row)


def cloudstorage_copy_objects(source, destination):
  """Copies all objects matching the source to the destination

  Both source and destination use the following format: /bucket/prefix/
  """
  for source_file_stat in cloudstorage.listbucket(source):
    destination_filename = destination + source_file_stat.filename[len(source):]
    if _should_copy_object(source_file_stat, destination_filename):
      cloudstorage.copy2(source_file_stat.filename, destination_filename)


def _should_copy_object(source_file_stat, destination):
  try:
    dest_file_stat = cloudstorage.stat(destination)
  except cloudstorage.NotFoundError:
    return True
  return source_file_stat.etag != dest_file_stat.etag
