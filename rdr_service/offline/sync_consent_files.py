"""
Sync Consent Files

Organize all consent files from PTSC source bucket into proper awardee buckets.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
import os
import pytz
import shutil
import tempfile
from typing import Collection, Dict, List
from zipfile import ZipFile

from sqlalchemy import or_
from sqlalchemy.orm import Session

from rdr_service import config
from rdr_service.api_util import copy_cloud_file, download_cloud_file, get_blob, list_blobs, parse_date
from rdr_service.dao import database_factory
from rdr_service.dao.participant_dao import ParticipantDao, ParticipantHistoryDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.site import Site
from rdr_service.participant_enums import QuestionnaireStatus

from rdr_service.resource.tasks import dispatch_rebuild_consent_metrics_tasks
from rdr_service.services.gcp_utils import gcp_cp
from rdr_service.storage import GoogleCloudStorageProvider


SOURCE_BUCKET = {
    "vibrent": "ptc-uploads-all-of-us-rdr-prod",
    "careevolution": "ce-uploads-all-of-us-rdr-prod"
}
DEFAULT_ORG_NAME = 'no-org-assigned'
DEFAULT_GOOGLE_GROUP = "no-site-assigned"
TEMP_CONSENTS_PATH = os.path.join(tempfile.gettempdir(), "temp_consents")


@dataclass
class ParticipantPairingInfo:
    hpo_name: str
    org_name: str
    site_name: str


@dataclass
class PairingHistoryRecord:
    org_name: str
    start_date: datetime


class FileSyncHandler:
    """Responsible for syncing a specific group of consent files"""
    def __init__(self, zip_files: bool, dest_bucket: str, storage_provider: GoogleCloudStorageProvider,
                 root_destination_folder: str, participant_pairing_info: Dict[int, ParticipantPairingInfo]):
        self.files_to_sync: List[ConsentFile] = []
        self.zip_files = zip_files
        self.dest_bucket = dest_bucket
        self.storage_provider = storage_provider
        self.root_destination_folder = root_destination_folder
        self.participant_pairing_info_map = participant_pairing_info

    def sync_files(self) -> Collection[ConsentFile]:
        for file in self.files_to_sync:
            pairing_info = self.participant_pairing_info_map[file.participant_id]
            if self.zip_files:
                file_sync_function = self._download_file_for_zip
            else:
                file_sync_function = self._copy_file_in_cloud
            file_sync_function(
                file=file,
                org_name=pairing_info.org_name or DEFAULT_ORG_NAME,
                site_name=pairing_info.site_name or DEFAULT_GOOGLE_GROUP
            )

            file.sync_time = datetime.utcnow()
            file.sync_status = ConsentSyncStatus.SYNC_COMPLETE

        if self.zip_files:
            self._zip_and_upload()

        return self.files_to_sync

    def _download_file_for_zip(self, file: ConsentFile, org_name, site_name):
        if config.GAE_PROJECT == 'localhost' and not os.environ.get('UNITTEST_FLAG', None):
            raise Exception(
                'Can not download consent files to machines outside the cloud, '
                'please sync consent files using the cloud environment'
            )

        file_name = os.path.basename(file.file_path)
        temp_file_destination = (
            TEMP_CONSENTS_PATH + f'/{self.dest_bucket}/{org_name}/{site_name}/P{file.participant_id}/{file_name}'
        )
        os.makedirs(os.path.dirname(temp_file_destination), exist_ok=True)

        self.storage_provider.download_blob(
            source_path=file.file_path,
            destination_path=temp_file_destination
        )

    def _copy_file_in_cloud(self, file: ConsentFile, org_name, site_name):
        destination_path = self._build_cloud_destination_path(
            org_name=org_name,
            site_name=site_name,
            participant_id=file.participant_id,
            file_name=os.path.basename(file.file_path)
        )
        self.storage_provider.copy_blob(source_path=file.file_path, destination_path=destination_path)

    def _build_cloud_destination_path(self, org_name, site_name, participant_id, file_name):
        return f'{self.dest_bucket}/{self.root_destination_folder}/{org_name}/{site_name}/P{participant_id}/{file_name}'

    def _zip_and_upload(self):
        if not os.path.isdir(TEMP_CONSENTS_PATH):
            # The directory wouldn't exist if there were no files downloaded that need to be zipped
            return

        logging.info("zipping and uploading consent files...")
        for bucket_dir in _directories_in(TEMP_CONSENTS_PATH):
            for org_dir in _directories_in(bucket_dir):
                for site_dir in _directories_in(org_dir):
                    zip_file_path = os.path.join(org_dir.path, site_dir.name + '.zip')
                    with ZipFile(zip_file_path, 'w') as zip_file:
                        self._zip_files_in_directory(zip_file, site_dir.path)
                    self._upload_zip_file(
                        zip_file_path=zip_file_path,
                        bucket_name=bucket_dir.name,
                        org_name=org_dir.name
                    )

        shutil.rmtree(TEMP_CONSENTS_PATH)

    @classmethod
    def _zip_files_in_directory(cls, zip_file: ZipFile, directory_path):
        for current_path, _, files in os.walk(directory_path):
            # os.walk will recurse into sub_directories, so we only need to handle the files in the current directory
            for file in files:
                file_path = os.path.join(current_path, file)
                file_path_in_zip = file_path[len(directory_path):]
                zip_file.write(file_path, arcname=file_path_in_zip)

    def _upload_zip_file(self, zip_file_path, bucket_name, org_name):
        file_name = os.path.basename(zip_file_path)
        self.storage_provider.upload_from_file(
            source_file=zip_file_path,
            path=f'{bucket_name}/{self.root_destination_folder}/{org_name}/{file_name}'
        )


class ConsentSyncGuesser:
    _SYNC_OVERLAP_DAYS_DELTA = timedelta(days=10)

    def __init__(self, session: Session, participant_history_dao: ParticipantHistoryDao):
        self._session = session
        self._participant_history_dao = participant_history_dao

    def check_consents(self, files: Collection[ConsentFile], session):
        # Get the latest pairing information for the participants
        participant_ids = [file.participant_id for file in files]
        raw_pairing_data = self._participant_history_dao.get_pairing_history(
            session=self._session,
            participant_ids=participant_ids
        )
        latest_participant_pairings = {}
        for history_record in raw_pairing_data:
            participant_id = history_record.participantId
            possible_latest_record = PairingHistoryRecord(
                org_name=history_record.externalId,
                start_date=history_record.lastModified
            )
            if participant_id not in latest_participant_pairings:
                latest_participant_pairings[participant_id] = possible_latest_record
            else:
                currently_stored_pairing = latest_participant_pairings[participant_id]
                if currently_stored_pairing.start_date <= possible_latest_record.start_date \
                        and currently_stored_pairing.org_name != possible_latest_record.org_name:
                    latest_participant_pairings[participant_id] = possible_latest_record

        participant_summaries = ParticipantSummaryDao.get_by_ids_with_session(self._session, participant_ids)
        for summary in participant_summaries:
            pairing_info = latest_participant_pairings[summary.participantId]
            latest_participant_pairings[summary.participantId] = (pairing_info, summary)

        modified_files_count = 0
        for file in files:
            pairing_info, summary = latest_participant_pairings[file.participant_id]
            sync_date = self.get_sync_date(file=file, summary=summary, latest_pairing_info=pairing_info)
            if sync_date is not None:
                file.sync_time = sync_date
                file.sync_status = ConsentSyncStatus.SYNC_COMPLETE

                modified_files_count += 1
                if modified_files_count % 500 == 0:
                    session.commit()

    @classmethod
    def get_sync_date(cls, file: ConsentFile, summary: ParticipantSummary, latest_pairing_info: PairingHistoryRecord):
        """
        This encapsulates the code for determining if a consent would have been copied using the previous process
        for syncing the files. It checks to see if a file would have been available for sync based on any of the times
        that a sync would have been triggered by the data available for a participant.
        If any of them would have triggered a sync to the latest org's bucket, then the date is returned for the sync.
        Otherwise None is returned.
        """
        if not cls._org_date_valid_for_sync(
            upload_time=file.file_upload_time,
            latest_pair_time=latest_pairing_info.start_date
        ):
            return None

        primary_time = summary.consentForStudyEnrollmentTime
        if cls._in_sync_time_window(
            upload_time=file.file_upload_time,
            consent_time=primary_time
        ):
            return cls._determine_sync_month(primary_time)

        ehr_time = summary.consentForElectronicHealthRecordsTime
        if cls._in_sync_time_window(
            upload_time=file.file_upload_time,
            consent_time=ehr_time
        ):
            return cls._determine_sync_month(ehr_time)

        # GROR wouldn't have triggered it until the June 1st sync
        gror_time = summary.consentForGenomicsRORTime
        if (
            gror_time is not None
            and gror_time > datetime(2021, 4, 21)  # The June 1st sync would have looked for dates back to April 21st
            and cls._in_sync_time_window(upload_time=file.file_upload_time, consent_time=gror_time)
        ):
            return cls._determine_sync_month(gror_time)

        return None

    @classmethod
    def _in_sync_time_window(cls, consent_time: datetime, upload_time: datetime):
        if not consent_time:
            return False

        window_start_date = datetime(consent_time.year, consent_time.month, 1) - cls._SYNC_OVERLAP_DAYS_DELTA
        window_end_date = datetime(consent_time.year, consent_time.month, 1) + relativedelta(months=1)
        return window_start_date <= upload_time <= window_end_date

    @classmethod
    def _determine_sync_month(cls, timestamp: datetime):
        return datetime(timestamp.year, timestamp.month, 1) + relativedelta(months=1)

    @classmethod
    def _org_date_valid_for_sync(cls, upload_time: datetime, latest_pair_time: datetime):
        pair_window_start = datetime(latest_pair_time.year, latest_pair_time.month, 1) - cls._SYNC_OVERLAP_DAYS_DELTA
        return pair_window_start < upload_time


class ConsentSyncController:
    def __init__(self, consent_dao, participant_dao: ParticipantDao, storage_provider: GoogleCloudStorageProvider):
        self.consent_dao = consent_dao
        self.participant_dao = participant_dao
        self.storage_provider = storage_provider
        self._destination_folder = config.getSettingJson('consent_destination_prefix', default='Participant')

    def _build_sync_handler(self, zip_files: bool, bucket: str, pairing_info: Dict[int, ParticipantPairingInfo]):
        return FileSyncHandler(
            zip_files=zip_files,
            dest_bucket=bucket,
            storage_provider=self.storage_provider,
            root_destination_folder=self._destination_folder,
            participant_pairing_info=pairing_info
        )

    def sync_ready_files(self):
        """Syncs any validated consent files that are ready for syncing"""

        sync_config = config.getSettingJson(config.CONSENT_SYNC_BUCKETS)
        hpos_sync_config = sync_config['hpos']
        orgs_sync_config = sync_config['orgs']

        file_list: List[ConsentFile] = self.consent_dao.get_files_ready_to_sync(
            hpo_names=hpos_sync_config.keys(),
            org_names=orgs_sync_config.keys()
        )

        pairing_info_map = self._build_participant_pairing_map(file_list)

        # Build the sync handlers, storing them in dictionaries that are keyed by the org or hpo name
        org_sync_groups = {}
        for org_name, settings in sync_config['orgs'].items():
            org_sync_groups[org_name] = self._build_sync_handler(
                zip_files=settings['zip_consents'],
                bucket=settings['bucket'],
                pairing_info=pairing_info_map
            )
        hpo_sync_groups = {}
        for hpo_name, settings in sync_config['hpos'].items():
            hpo_sync_groups[hpo_name] = self._build_sync_handler(
                zip_files=settings['zip_consents'],
                bucket=settings['bucket'],
                pairing_info=pairing_info_map
            )

        for file in file_list:
            pairing_info = pairing_info_map.get(file.participant_id, None)
            if not pairing_info:
                # Skip files for unpaired participants
                continue

            # Retrieve the sync handler based on the pairing information
            file_group = None
            if pairing_info.org_name in org_sync_groups:
                file_group = org_sync_groups[pairing_info.org_name]
            elif pairing_info.hpo_name in hpo_sync_groups:
                file_group = hpo_sync_groups[pairing_info.hpo_name]

            if file_group:  # Ignore participants paired to an org or hpo we aren't syncing files for
                file_group.files_to_sync.append(file)

        with self.consent_dao.session() as session:
            for file_group in [*org_sync_groups.values(), *hpo_sync_groups.values()]:
                files_synced = file_group.sync_files()

                # Update the database after each group syncs so ones
                # that have succeeded so far get saved if a later one fails
                if len(files_synced):
                    self.consent_dao.batch_update_consent_files(session=session, consent_files=files_synced)
                    session.commit()

                    # Queue tasks to rebuild consent metrics resource data records (for PDR)
                    dispatch_rebuild_consent_metrics_tasks([file.id for file in files_synced])

    def _build_participant_pairing_map(self, files: List[ConsentFile]) -> Dict[int, ParticipantPairingInfo]:
        """
        Returns a dictionary mapping each participant's id to the external id for their organization
        and the google group name for their site
        """
        participant_ids = {file.participant_id for file in files}
        participant_pairing_data = self.participant_dao.get_pairing_data_for_ids(participant_ids)
        return {
            participant_id: ParticipantPairingInfo(hpo_name=hpo_name, org_name=org_name, site_name=site_name)
            for participant_id, hpo_name, org_name, site_name in participant_pairing_data
        }


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


def build_participant_query(session, org_ids, start_date=None, end_date=None, all_va=False, ids: list = None):
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
                ParticipantSummary.consentForElectronicHealthRecordsTime.between(start_date, end_date),
                ParticipantSummary.consentForGenomicsRORTime.between(start_date, end_date)
            )
        )
    elif start_date:
        participant_query = participant_query.filter(
            or_(
                ParticipantSummary.consentForStudyEnrollmentTime > start_date,
                ParticipantSummary.consentForElectronicHealthRecordsTime > start_date,
                ParticipantSummary.consentForGenomicsRORTime > start_date
            )
        )
    elif end_date:
        participant_query = participant_query.filter(
            or_(
                ParticipantSummary.consentForStudyEnrollmentTime < end_date,
                ParticipantSummary.consentForElectronicHealthRecordsTime < end_date,
                ParticipantSummary.consentForGenomicsRORTime < end_date
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
