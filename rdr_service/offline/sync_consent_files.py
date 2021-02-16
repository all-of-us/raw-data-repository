"""
Sync Consent Files

Organize all consent files from PTSC source bucket into proper awardee buckets.
"""
from datetime import datetime, timedelta
import logging
import pytz
import os
import shutil
from sqlalchemy import or_
import tempfile
from zipfile import ZipFile

from rdr_service import config
from rdr_service.api_util import copy_cloud_file, download_cloud_file, get_blob, list_blobs, parse_date
from rdr_service.dao import database_factory
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.site import Site
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.services.gcp_utils import gcp_cp
from rdr_service.storage import GoogleCloudStorageProvider

SOURCE_BUCKET = {
    "vibrent": "ptc-uploads-all-of-us-rdr-prod",
    "careevolution": "ce-uploads-all-of-us-rdr-prod"
}
DEFAULT_GOOGLE_GROUP = "no-site-assigned"
TEMP_CONSENTS_PATH = os.path.join(tempfile.gettempdir(), "temp_consents")


def get_consent_destination(zipping=False, add_protocol=False, **kwargs):
    if zipping:
        destination_pattern = TEMP_CONSENTS_PATH + '/{bucket_name}/{org_external_id}/{site_name}/P{p_id}/'
    else:
        prefix = 'gs://' if add_protocol else ''
        destination_pattern = prefix + '{bucket_name}/Participant/{org_external_id}/{site_name}/P{p_id}/'

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
    storage_provider = GoogleCloudStorageProvider()
    for bucket_dir in _directories_in(TEMP_CONSENTS_PATH):
        for org_dir in _directories_in(bucket_dir):
            bucket = bucket_dir.name
            for site_dir in _directories_in(org_dir):
                zip_file_name = os.path.join(org_dir.path, site_dir.name + '.zip')
                with ZipFile(zip_file_name, 'w') as zip_file:
                    _add_path_to_zip(zip_file, site_dir.path)

                _, file_name = os.path.split(zip_file_name)
                destination = "{bucket_name}/Participant/{org_external_id}/{file_name}".format(
                    bucket_name=bucket,
                    org_external_id=org_dir.name,
                    file_name=file_name
                )
                if not dry_run:
                    logging.debug("Uploading file '{zip_file}' to '{destination}'".format(
                        zip_file=zip_file_name,
                        destination=destination
                    ))
                    storage_provider.upload_from_file(zip_file_name, destination)

    shutil.rmtree(TEMP_CONSENTS_PATH)


def do_sync_recent_consent_files(all_va=False, zip_files=False):
    # Sync everything from the start of the previous month
    start_date = datetime.now().replace(day=1) - timedelta(days=10)
    do_sync_consent_files(start_date=start_date.strftime('%Y-%m-01'), zip_files=zip_files, all_va=all_va)


def do_sync_consent_files(zip_files=False, **kwargs):
    """
    entrypoint
    """
    logging.info('Syncing consent files.')
    org_buckets = get_org_data_map()
    org_ids = [org_id for org_id, org_data in org_buckets.items()]
    start_date = kwargs.get('start_date')
    end_date = kwargs.get('end_date')
    all_va = kwargs.get('all_va')
    if start_date or end_date:
        logging.info(f'syncing consents from {start_date} to {end_date}')
    if zip_files:
        logging.info('zipping consent files')
    if all_va:
        logging.info('only interacting with VA files')
    file_filter = kwargs.get('file_filter', 'pdf')
    for participant_id, origin, site_google_group, org_external_id in _iter_participants_data(org_ids, **kwargs):
        logging.info(f'Syncing files for {participant_id}')
        source_bucket = SOURCE_BUCKET.get(origin, SOURCE_BUCKET[next(iter(SOURCE_BUCKET))])
        source = "/{source_bucket}/Participant/P{participant_id}/"\
            .format(source_bucket=source_bucket,
                    participant_id=participant_id)
        if all_va:
            destination_bucket = 'aou179'
        else:
            destination_bucket = org_buckets[org_external_id]
        destination = get_consent_destination(zip_files,
                                              bucket_name=destination_bucket,
                                              org_external_id=org_external_id,
                                              site_name=site_google_group or DEFAULT_GOOGLE_GROUP,
                                              p_id=participant_id)

        cloudstorage_copy_objects_task(source, destination, start_date=start_date,
                                       file_filter=file_filter, zip_files=zip_files)

    if zip_files:
        archive_and_upload_consents(dry_run=False)


def get_org_data_map():
    return config.getSettingJson(config.CONSENT_SYNC_BUCKETS)


def build_participant_query(session, org_ids, start_date=None, end_date=None, all_va=False, ids: list=None):
    participant_query = session.query(
        Participant.participantId,
        Participant.participantOrigin,
        Site.googleGroup,
        Organization.externalId
    ).outerjoin(
        Organization, Organization.organizationId == Participant.organizationId
    ).outerjoin(
        Site, Site.siteId == Participant.siteId
    ).outerjoin(
        ParticipantSummary, Participant.participantSummary
    ).filter(
        Participant.isGhostId.isnot(True),
        Participant.isTestParticipant.isnot(True),
        ParticipantSummary.consentForStudyEnrollment == int(QuestionnaireStatus.SUBMITTED),
        or_(
            ParticipantSummary.email.is_(None),
            ParticipantSummary.email.notlike('%@example.com')
        )
    )

    if start_date and end_date:
        participant_query = participant_query.filter(
            or_(
                ParticipantSummary.consentForStudyEnrollmentTime.between(start_date, end_date),
                ParticipantSummary.consentForElectronicHealthRecordsTime.between(start_date, end_date)
            )
        )
    elif start_date:
        participant_query = participant_query.filter(
            or_(
                ParticipantSummary.consentForStudyEnrollmentTime > start_date,
                ParticipantSummary.consentForElectronicHealthRecordsTime > start_date
            )
        )
    elif end_date:
        participant_query = participant_query.filter(
            or_(
                ParticipantSummary.consentForStudyEnrollmentTime < end_date,
                ParticipantSummary.consentForElectronicHealthRecordsTime < end_date
            )
        )

    if all_va:
        participant_query = participant_query.filter(Organization.externalId.like('VA_%'))
    else:
        participant_query = participant_query.filter(Organization.externalId.in_(org_ids))

    if ids:
        participant_query = participant_query.filter(Participant.participantId.in_(ids))

    return participant_query


def _iter_participants_data(org_ids, **kwargs):
    with database_factory.make_server_cursor_database().session() as session:
        participant_query = build_participant_query(session, org_ids, **kwargs)
        for participant_data in participant_query:
            yield participant_data


def _download_file(source, destination):
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    download_cloud_file(source, destination)


def cloudstorage_copy_objects_task(source, destination, start_date: str = None,
                                   file_filter=None, zip_files=False):
    """
    Cloud Task: Copies all objects matching the source to the destination.
    Both source and destination use the following format: /bucket/prefix/
    """
    if start_date:
        timezone = pytz.timezone('Etc/Greenwich')
        start_date = timezone.localize(parse_date(start_date))

    path = source if source[0:1] != '/' else source[1:]
    bucket_name, _, prefix = path.partition('/')
    prefix = None if prefix == '' else prefix
    files_found = False
    for source_blob in list_blobs(bucket_name, prefix):
        if not source_blob.name.endswith('/'):  # Exclude folders
            source_file_path = os.path.normpath('/' + bucket_name + '/' + source_blob.name)
            destination_file_path = destination + source_file_path[len(source):]
            if (zip_files or _not_previously_copied(source_file_path, destination_file_path)) and\
                    _meets_date_requirements(source_blob, start_date) and\
                    _matches_file_filter(source_blob.name, file_filter):
                files_found = True
                move_file_function = _download_file if zip_files else copy_cloud_file
                move_file_function(source_file_path, destination_file_path)
    if not files_found:
        logging.warning(f'No files copied from {source}')


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


def _meets_date_requirements(source_blob, start_date):
    # Not filtering files by end_date in case we need to transfer files that have been re-uploaded
    return start_date is None or source_blob.updated >= start_date


def _matches_file_filter(source_blob_name, file_filter):
    return file_filter is None or source_blob_name.endswith(file_filter)
