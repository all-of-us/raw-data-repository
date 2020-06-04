"""
Sync Consent Files

Organize all consent files from PTSC source bucket into proper awardee buckets.
"""
import collections
from datetime import datetime, timedelta
import pytz
import os

from rdr_service import config
from rdr_service.api_util import list_blobs, copy_cloud_file, get_blob
from rdr_service.dao import database_factory
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask

HPO_REPORT_CONFIG_GCS_PATH = "/all-of-us-rdr-sequestered-config-test/hpo-report-config-mixin.json"

SOURCE_BUCKET = {
    "vibrent": "ptc-uploads-all-of-us-rdr-prod",
    "careevolution": "ce-uploads-all-of-us-rdr-prod"
}

COLUMN_BUCKET_NAME = "Bucket Name"
COLUMN_AGGREGATING_ORG_ID = "Aggregating Org ID"
COLUMN_ORG_ID = "Org ID"
COLUMN_ORG_STATUS = "Org Status"

ORG_STATUS_ACTIVE = "Active"
DEFAULT_GOOGLE_GROUP = "no-site-assigned"


OrgData = collections.namedtuple("OrgData", ("org_id", "aggregate_id", "bucket_name"))


ParticipantData = collections.namedtuple("ParticipantData", ("participant_id", "origin_id", "google_group", "org_id"))


def do_sync_recent_consent_files():
    # Sync everything from the start of the previous month
    start_date = datetime.now().replace(day=1) - timedelta(days=10)
    do_sync_consent_files(start_date=start_date.strftime('%Y-%m-01'))


def do_sync_consent_files(**kwargs):
    """
  entrypoint
  """
    org_buckets = get_org_data_map()
    org_ids = [org_id for org_id, org_data in org_buckets.items()]
    start_date = kwargs.get('start_date')
    file_filter = kwargs.get('file_filter', 'pdf')
    for participant_data in _iter_participants_data(org_ids, **kwargs):
        kwargs = {
            "source_bucket": SOURCE_BUCKET.get(participant_data.origin_id, SOURCE_BUCKET[next(iter(SOURCE_BUCKET))]),
            "destination_bucket": org_buckets[participant_data.org_id],
            "participant_id": participant_data.participant_id,
            "google_group": participant_data.google_group or DEFAULT_GOOGLE_GROUP,
        }
        source = "/{source_bucket}/Participant/P{participant_id}/".format(**kwargs)
        destination = "/{destination_bucket}/Participant/{google_group}/P{participant_id}/".format(**kwargs)

        if config.GAE_PROJECT == 'localhost':
            cloudstorage_copy_objects_task(source, destination, date_limit=start_date, file_filter=file_filter)
        else:
            params = {'source': source, 'destination': destination, 'date_limit': start_date,
                      'file_filter': file_filter}
            task = GCPCloudTask('copy_cloudstorage_object_task', payload=params)
            task.execute()


def get_org_data_map():
    return config.getSettingJson(config.CONSENT_SYNC_ORGANIZATIONS)


PARTICIPANT_DATA_SQL = """
select
  participant.participant_id,
  participant.participant_origin,
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
  and summary.consent_for_study_enrollment = 1
  and (
    summary.email is null
    or summary.email not like '%@example.com'
  )
"""

participant_filters_sql = {
    'start_date': """
        and (
            summary.consent_for_study_enrollment_time > :start_date
            or
            summary.consent_for_electronic_health_records_time > :start_date
            )
        """,
    'end_date': """
        and (
            summary.consent_for_study_enrollment_time < :end_date
            or
            summary.consent_for_electronic_health_records_time < :end_date
            )
        """,
    'org_ids': """
        and organization.external_id in :org_ids
    """,
    'all_va': """
        and organization.external_id like 'VA_%'
    """
}


def build_participant_query(org_ids, **kwargs):
    participant_sql = PARTICIPANT_DATA_SQL
    parameters = {}

    for filter_field in ['start_date', 'end_date']:
        if filter_field in kwargs:
            participant_sql += participant_filters_sql[filter_field]
            parameters[filter_field] = kwargs[filter_field]

    if kwargs.get('all_va'):
        participant_sql += participant_filters_sql['all_va']
    else:
        participant_sql += participant_filters_sql['org_ids']
        parameters['org_ids'] = org_ids

    return participant_sql, parameters


def _iter_participants_data(org_ids, **kwargs):
    participant_sql, parameters = build_participant_query(org_ids, **kwargs)

    with database_factory.make_server_cursor_database().session() as session:
        for row in session.execute(participant_sql, parameters):
            yield ParticipantData(*row)


def cloudstorage_copy_objects_task(source, destination, date_limit=None, file_filter=None):
    """
    Cloud Task: Copies all objects matching the source to the destination.
    Both source and destination use the following format: /bucket/prefix/
    """
    if date_limit:
        timezone = pytz.timezone('Etc/Greenwich')
        date_limit = timezone.localize(datetime.strptime(date_limit, '%Y-%m-%d'))
    path = source if source[0:1] != '/' else source[1:]
    bucket_name, _, prefix = path.partition('/')
    prefix = None if prefix == '' else prefix
    for source_blob in list_blobs(bucket_name, prefix):
        if not source_blob.name.endswith('/'):  # Exclude folders
            source_file_path = os.path.normpath('/' + bucket_name + '/' + source_blob.name)
            destination_file_path = destination + source_file_path[len(source):]
            if _not_previously_copied(source_file_path, destination_file_path) and \
                    _after_date_limit(source_blob, date_limit) and \
                    _matches_file_filter(source_blob.name, file_filter):
                copy_cloud_file(source_file_path, destination_file_path)


def _not_previously_copied(source_file_path, destination_file_path):
    destination_file_path = destination_file_path if destination_file_path[0:1] != '/' else destination_file_path[1:]
    destination_bucket_name, _, destination_blob_name = destination_file_path.partition('/')
    destination_blob = get_blob(destination_bucket_name, destination_blob_name)
    if destination_blob is None:
        return True

    source_file_path = source_file_path if source_file_path[0:1] != '/' else source_file_path[1:]
    source_bucket_name, _, source_blob_name = source_file_path.partition('/')
    source_blob = get_blob(source_bucket_name, source_blob_name)
    return source_blob.etag != destination_blob.etag


def _after_date_limit(source_blob, date_limit):
    return date_limit is None or source_blob.updated > date_limit


def _matches_file_filter(source_blob_name, file_filter):
    return file_filter is None or source_blob_name.endswith(file_filter)
