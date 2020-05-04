"""
Sync Consent Files

Organize all consent files from PTSC source bucket into proper awardee buckets.
"""
import collections
import os
import sqlalchemy

from rdr_service import config
from rdr_service.api_util import list_blobs, copy_cloud_file, get_blob
from rdr_service.cloud_utils.google_sheets import GoogleSheetCSVReader
from rdr_service.dao import database_factory
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask

HPO_REPORT_CONFIG_GCS_PATH = "/all-of-us-rdr-sequestered-config-test/hpo-report-config-mixin.json"

SOURCE_BUCKET = "ptc-uploads-all-of-us-rdr-prod"

COLUMN_BUCKET_NAME = "Bucket Name"
COLUMN_AGGREGATING_ORG_ID = "Aggregating Org ID"
COLUMN_ORG_ID = "Org ID"
COLUMN_ORG_STATUS = "Org Status"

ORG_STATUS_ACTIVE = "Active"
DEFAULT_GOOGLE_GROUP = "no_site_pairing"


OrgData = collections.namedtuple("OrgData", ("org_id", "aggregate_id", "bucket_name"))


ParticipantData = collections.namedtuple("ParticipantData", ("participant_id", "google_group", "org_id"))


def do_sync_consent_files():
    """
  entrypoint
  """
    org_data_map = config.getSetting(config.CONSENT_SYNC_ORGANIZATIONS)
    org_ids = [org_id for org_id, org_data in org_data_map.items()]
    for participant_data in _iter_participants_data(org_ids):
        kwargs = {
            "source_bucket": SOURCE_BUCKET,
            "destination_bucket": org_data_map[participant_data.org_id]['bucket_name'],
            "participant_id": participant_data.participant_id,
            "google_group": participant_data.google_group or DEFAULT_GOOGLE_GROUP,
        }
        source = "/{source_bucket}/Participant/P{participant_id}/".format(**kwargs)
        destination = "/{destination_bucket}/Participant/{google_group}/P{participant_id}/".format(**kwargs)

        if config.GAE_PROJECT == 'localhost':
            cloudstorage_copy_objects_task(source, destination)
        else:
            params = {'source': source, 'destination': destination}
            task = GCPCloudTask('copy_cloudstorage_object_task', payload=params)
            task.execute()


def _load_org_data_map(sheet_id):
    # parse initial mapping
    org_data_map = {
        row.get(COLUMN_ORG_ID): OrgData(
            row.get(COLUMN_ORG_ID), row.get(COLUMN_AGGREGATING_ORG_ID), row.get(COLUMN_BUCKET_NAME)
        )
        for row in GoogleSheetCSVReader(sheet_id)
        if row.get(COLUMN_ORG_STATUS) == ORG_STATUS_ACTIVE
    }
    # apply transformations that require full map
    return {
        org_data.org_id: _org_data_with_bucket_inheritance_from_org_data(org_data_map, org_data)
        for org_data in list(org_data_map.values())
    }


def _org_data_with_bucket_inheritance_from_org_data(org_data_map, org_data):
    return OrgData(
        org_data.org_id,
        org_data.aggregate_id,
        (org_data.bucket_name if org_data.bucket_name else org_data_map[org_data.aggregate_id].bucket_name),
    )


PARTICIPANT_DATA_SQL = sqlalchemy.text(
    """
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
  and organization.external_id in :org_ids
"""
)


def _iter_participants_data(org_ids):
    with database_factory.make_server_cursor_database().session() as session:
        for row in session.execute(PARTICIPANT_DATA_SQL, {'org_ids': org_ids}):
            yield ParticipantData(*row)


def cloudstorage_copy_objects_task(source, destination):
    """
    Cloud Task: Copies all objects matching the source to the destination.
    Both source and destination use the following format: /bucket/prefix/
    """
    path = source if source[0:1] != '/' else source[1:]
    bucket_name, _, prefix = path.partition('/')
    prefix = None if prefix == '' else '/' + prefix
    for source_blob in list_blobs(bucket_name, prefix):
        source_file_path = os.path.normpath('/' + bucket_name + '/' + source_blob.name)
        destination_file_path = destination + source_file_path[len(source):]
        if _should_copy_object(source_file_path, destination_file_path):
            copy_cloud_file(source_file_path, destination_file_path)


def _should_copy_object(source_file_path, destination_file_path):
    destination_file_path = destination_file_path if destination_file_path[0:1] != '/' else destination_file_path[1:]
    destination_bucket_name, _, destination_blob_name = destination_file_path.partition('/')
    destination_blob = get_blob(destination_bucket_name, destination_blob_name)
    if destination_blob is None:
        return True

    source_file_path = source_file_path if source_file_path[0:1] != '/' else source_file_path[1:]
    source_bucket_name, _, source_blob_name = source_file_path.partition('/')
    source_blob = get_blob(source_bucket_name, source_blob_name)
    return source_blob.etag != destination_blob.etag
