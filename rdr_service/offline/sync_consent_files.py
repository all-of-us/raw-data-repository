"""
Sync Consent Files

Organize all consent files from PTSC source bucket into proper awardee buckets.
"""
import collections
from datetime import datetime, timedelta
import logging
import pytz
import os
import shutil
from zipfile import ZipFile

from rdr_service import config
from rdr_service.api_util import copy_cloud_file, download_cloud_file, get_blob, list_blobs
from rdr_service.dao import database_factory
from rdr_service.services.gcp_utils import gcp_cp

SOURCE_BUCKET = {
    "vibrent": "ptc-uploads-all-of-us-rdr-prod",
    "careevolution": "ce-uploads-all-of-us-rdr-prod"
}
DEFAULT_GOOGLE_GROUP = "no-site-assigned"
TEMP_CONSENTS_PATH = "./temp_consents"

ParticipantData = collections.namedtuple("ParticipantData", ("participant_id", "origin_id", "google_group", "org_id"))


def get_consent_destination(zipping=False, **kwargs):
    destination_pattern = \
        TEMP_CONSENTS_PATH + '/{bucket_name}/{org_external_id}/{site_name}/P{p_id}/' \
        if zipping else \
        "gs://{bucket_name}/Participant/{org_external_id}/{site_name}/P{p_id}/"

    return destination_pattern.format(**kwargs)


def _directories_in(directory_path):
    with os.scandir(directory_path) as objects:
        return [directory_object for directory_object in objects if directory_object.is_dir()]


def _add_path_to_zip(zip_file, directory_path):
    for current_path, _, files in os.walk(directory_path):
        # os.walk will recurse into sub_directories, so we only need to handle the files in the current directory
        for file in files:
            file_path = os.path.join(current_path, file)
            archive_name = file_path[len(directory_path):]
            zip_file.write(file_path, arcname=archive_name)


def _format_debug_out(p_id, src, dest):
    logging.debug(" Participant: {0}".format(p_id))
    logging.debug("    src: {0}".format(src))
    logging.debug("   dest: {0}".format(dest))


def copy_file(source, destination, participant_id, dry_run=True, zip_files=False):
    if not dry_run:
        if zip_files:
            # gcp_cp doesn't create local directories when they don't exist
            os.makedirs(destination, exist_ok=True)

        copy_args = {'flags': '-m'}
        if not zip_files:
            copy_args['args'] = '-r'
        gcp_cp(source, destination, **copy_args)

    _format_debug_out(participant_id, source, destination)


def archive_and_upload_consents(dry_run=True):
    logging.info("zipping and uploading consent files...")
    for bucket_dir in _directories_in(TEMP_CONSENTS_PATH):
        for org_dir in _directories_in(bucket_dir):
            bucket = bucket_dir.name
            for site_dir in _directories_in(org_dir):
                zip_file_name = os.path.join(org_dir.path, site_dir.name + '.zip')
                with ZipFile(zip_file_name, 'w') as zip_file:
                    _add_path_to_zip(zip_file, site_dir.path)

                destination = "gs://{bucket_name}/Participant/{org_external_id}/".format(
                    bucket_name=bucket,
                    org_external_id=org_dir.name
                )
                if not dry_run:
                    logging.debug("Uploading file '{zip_file}' to '{destination}'".format(
                        zip_file=zip_file_name,
                        destination=destination
                    ))
                    gcp_cp(zip_file_name, destination, flags="-m", )

    shutil.rmtree(TEMP_CONSENTS_PATH)


def do_sync_recent_consent_files():
    # Sync everything from the start of the previous month
    start_date = datetime.now().replace(day=1) - timedelta(days=10)
    do_sync_consent_files(start_date=start_date.strftime('%Y-%m-01'))


def do_sync_consent_files(zip_files=False, **kwargs):
    """
  entrypoint
  """
    org_buckets = get_org_data_map()
    org_ids = [org_id for org_id, org_data in org_buckets.items()]
    start_date = kwargs.get('start_date')
    file_filter = kwargs.get('file_filter', 'pdf')
    for participant_data in _iter_participants_data(org_ids, **kwargs):
        source_bucket = SOURCE_BUCKET.get(participant_data.origin_id, SOURCE_BUCKET[next(iter(SOURCE_BUCKET))])
        source = "/{source_bucket}/Participant/P{participant_id}/"\
            .format(source_bucket=source_bucket,
                    participant_id=participant_data.participant_id)
        destination = get_consent_destination(zip_files,
                                              bucket_name=org_buckets[participant_data.org_id],
                                              org_external_id=participant_data.org_id,
                                              site_name=participant_data.google_group or DEFAULT_GOOGLE_GROUP,
                                              p_id=participant_data.participant_id)

        cloudstorage_copy_objects_task(source, destination, date_limit=start_date, file_filter=file_filter)

    if zip_files:
        archive_and_upload_consents(dry_run=False)


def get_org_data_map():
    return config.getSettingJson(config.CONSENT_SYNC_BUCKETS)


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
                move_file_function = download_cloud_file if destination.startswith('.') else copy_cloud_file
                move_file_function(source_file_path, destination_file_path)


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
