from datetime import datetime, timedelta
import logging
import math
from typing import List

from sqlalchemy import bindparam
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.sql import func
from werkzeug.exceptions import HTTPException, InternalServerError, BadGateway

from rdr_service import config
from rdr_service.api_util import dispatch_task
from rdr_service.app_util import datetime_as_naive_utc
from rdr_service.cloud_utils import bigquery
from rdr_service.config import GAE_PROJECT
from rdr_service.dao.ehr_dao import EhrReceiptDao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.domain_model.ehr import ParticipantEhrFile
from rdr_service.model.ehr import ParticipantEhrReceipt
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import EhrStatus
from rdr_service.offline.bigquery_sync import dispatch_participant_rebuild_tasks

LOG = logging.getLogger(__name__)
ce_hpo_id_list = config.getSettingJson(config.CE_MEDIATED_HPO_ID, default=None)


class ParticipantEhrTracking:
    """Helper class for keeping track of participants with ehr files seen in a job"""

    def __init__(self, participant_ids_with_recent_ehr):
        self._files_in_batch: List[ParticipantEhrFile] = []

        self._ids_with_recent_ehr = set(participant_ids_with_recent_ehr)
        self._recent_ids_still_with_files = set()

    def record_file_uploaded(self, record: ParticipantEhrFile):
        self._files_in_batch.append(record)

        participant_id = record.participant_id
        if participant_id in self._ids_with_recent_ehr:
            # After parsing all the files, we want _ids_with_recent_ehr to only be the participants
            # that no longer have ehr files present. But we'll also want to know if a participant
            # didn't have one before, so this will move their id to another list.
            self._ids_with_recent_ehr.remove(participant_id)
            self._recent_ids_still_with_files.add(participant_id)

    def get_batch_list(self):
        return self._files_in_batch

    def clear_batch_list(self):
        self._files_in_batch = []

    def get_participants_no_longer_current(self):
        # After parsing all the current files, this will only be left with the participants that weren't
        # seen in the BQ view
        return self._ids_with_recent_ehr

    def get_participants_with_new_files(self):
        """Retrieve all the participant ids that didn't have files present in the BQ view recently, but do now"""
        return {
            record.participant_id
            for record in self._files_in_batch
            if record.participant_id not in self._recent_ids_still_with_files
        }

    @classmethod
    def is_ce_mediated_file(cls, file: ParticipantEhrFile):
        if ce_hpo_id_list is None:
            # Continue assuming everything's HPO uploaded if CE's id isn't set
            return False

        return file.hpo_id.lower() in ce_hpo_id_list


def update_ehr_status_organization():
    """
    Entrypoint, executed as a cron job.
    """
    update_organizations()
    logging.info('Update EHR complete')


def update_ehr_status_participant():
    """
    Entrypoint, executed as a cron job.
    """
    update_participant_summaries()
    logging.info('Update EHR complete')


def make_update_participant_summaries_job(project_id=None, bigquery_view=None):
    if bigquery_view is None:
        config_param = config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT
        try:
            bigquery_view = config.getSetting(config_param, None)
        except config.InvalidConfigException as e:
            LOG.warning("Config lookup exception for {}: {}".format(config_param, e))
            bigquery_view = None

    if bigquery_view:
        query = f"SELECT person_id, latest_upload_time, hpo_id FROM `{bigquery_view}`"
        return bigquery.BigQueryJob(query, default_dataset_id="operations_analytics", page_size=1000,
                                    project_id=project_id)
    else:
        return None


def update_participant_summaries():
    """
  Updates ehr status on participant summaries

  Loads results in batches and commits updates to database per batch.
  """
    job = make_update_participant_summaries_job()
    if job is not None:
        update_participant_summaries_from_job(job)
    else:
        LOG.warning("Skipping update_participant_summaries because of invalid config")


def _track_historical_participant_ehr_data(session, file_list: List[ParticipantEhrFile]):
    query = insert(ParticipantEhrReceipt).values({
        ParticipantEhrReceipt.participantId: bindparam('participant_id'),
        ParticipantEhrReceipt.fileTimestamp: bindparam('receipt_time'),
        ParticipantEhrReceipt.hpo_id: bindparam('hpo_id'),
        ParticipantEhrReceipt.firstSeen: func.utc_timestamp(),
        ParticipantEhrReceipt.lastSeen: func.utc_timestamp()
    }).on_duplicate_key_update({
        'last_seen': func.utc_timestamp()
    }).prefix_with('IGNORE')

    session.execute(query, [
        {
            'participant_id': record.participant_id,
            'receipt_time': record.receipt_time,
            'hpo_id': record.hpo_id
        }
        for record in file_list
    ])


def update_participant_summaries_from_job(job, project_id=GAE_PROJECT):
    job_start_time = datetime.utcnow()

    # record which participants have the current flags set,
    # so we can update PDR with the ones that don't have it set later
    summary_dao = ParticipantSummaryDao()
    hpo_ehr_tracking = ParticipantEhrTracking(
        summary_dao.get_participant_ids_with_hpo_ehr_data_available()
    )
    ce_ehr_tracking = ParticipantEhrTracking(
        summary_dao.get_participant_ids_with_mediated_ehr_data_available()
    )

    # clear the current flags in the db (they'll get set again if they still have files, otherwise they'll remain unset)
    summary_dao.prepare_for_ehr_status_update()

    batch_size = 100
    for i, page in enumerate(job):
        LOG.info("Processing page {} of results...".format(i))
        hpo_ehr_tracking.clear_batch_list()
        participant_ids_to_rebuild = set()

        with summary_dao.session() as session:
            for row in page:
                ehr_file = ParticipantEhrFile(
                    participant_id=row.person_id,
                    receipt_time=row.latest_upload_time,
                    hpo_id=row.hpo_id
                )
                if ParticipantEhrTracking.is_ce_mediated_file(ehr_file):
                    file_tracker = ce_ehr_tracking
                else:
                    file_tracker = hpo_ehr_tracking
                file_tracker.record_file_uploaded(ehr_file)

            hpo_files_in_batch = hpo_ehr_tracking.get_batch_list()
            ce_files_in_batch = ce_ehr_tracking.get_batch_list()
            _track_historical_participant_ehr_data(session, hpo_files_in_batch + ce_files_in_batch)

            hpo_update_count = summary_dao.bulk_update_hpo_ehr_status_with_session(session, hpo_files_in_batch)
            ce_update_count = summary_dao.bulk_update_mediated_ehr_status_with_session(session, ce_files_in_batch)

            session.commit()

            total_rows = hpo_update_count + ce_update_count
            LOG.info("Affected {} rows.".format(total_rows))

            if total_rows > 0:
                # Rebuild participants in the page that have new data available. Checking that the participant
                #  ehr receipts were created since the job started (offsetting by an hour to account for any
                #  difference between server times)
                participant_ids_in_page = [record.participant_id for record in hpo_files_in_batch]
                new_ehr_data_results = session.query(ParticipantEhrReceipt.participantId).filter(
                    ParticipantEhrReceipt.firstSeen >= job_start_time - timedelta(hours=1),
                    ParticipantEhrReceipt.participantId.in_(participant_ids_in_page)
                ).all()
                # TODO: only get the participants that actually changed

                participant_ids_with_new_ehr_data = [row.participantId for row in new_ehr_data_results]
                participant_ids_to_rebuild.update(participant_ids_with_new_ehr_data)

            participants_with_new_files = (
                hpo_ehr_tracking.get_participants_with_new_files()
                | ce_ehr_tracking.get_participants_with_new_files()
            )
            for participant_id in participants_with_new_files:
                # Update the enrollment status of any participants that didn't recently have ehr files
                summary = ParticipantSummaryDao.get_for_update_with_linked_data(
                    participant_id=participant_id,
                    session=session
                )
                if summary:
                    summary_dao.update_enrollment_status(session=session, summary=summary)
                    participant_ids_to_rebuild.add(participant_id)

            if participant_ids_to_rebuild:
                create_rebuild_tasks_for_participants(
                    list(participant_ids_to_rebuild), batch_size, project_id, summary_dao
                )

            for participant_id in participant_ids_to_rebuild:
                dispatch_task(endpoint='update_retention_status', payload={'participant_id': participant_id})

    # Rebuild any participants that had the "current" flag set before, but don't now
    # (because they didn't have a file today)
    participant_ids_that_previously_had_ehr = hpo_ehr_tracking.get_participants_no_longer_current()
    LOG.info(f'Rebuilding {len(participant_ids_that_previously_had_ehr)} '
             f'participants that no longer appear in the view')
    create_rebuild_tasks_for_participants(participant_ids_that_previously_had_ehr, batch_size, project_id, summary_dao)


def create_rebuild_tasks_for_participants(participant_id_list, batch_size, project_id, dao):
    with dao.session() as session:
        # TOD0:  Handle mediated EHR fields if they become part of the UpdateEhrStatus job.  Those fields not
        # currently part of the BQPDRParticipantSummarySchema pending official implementation in RDR
        records = session.query(
            ParticipantSummary.participantId,
            ParticipantSummary.ehrReceiptTime,
            ParticipantSummary.ehrUpdateTime,
            ParticipantSummary.isEhrDataAvailable,
            ParticipantSummary.wasEhrDataAvailable,
            # These may also be updated/recalculated in the course of processing the EHR status update ingestion
            ParticipantSummary.enrollmentStatusV3_2,
            ParticipantSummary.healthDataStreamSharingStatus,
            ParticipantSummary.healthDataStreamSharingStatusTime
        ).filter(
            ParticipantSummary.participantId.in_(participant_id_list)
        ).all()

    total_participants = len(records)
    count = int(math.ceil(float(total_participants) / float(batch_size)))
    LOG.info(
        f'UpdateEhrStatus: calculated {count} participant rebuild '
        f'tasks from {total_participants} records and batch size of {batch_size}'
    )

    patch_data = [{
        'pid': summary.participantId,
        'patch': {
            'ehr_status': str(EhrStatus.PRESENT),
            'ehr_status_id': int(EhrStatus.PRESENT),
            'ehr_receipt': summary.ehrReceiptTime,
            'ehr_update': summary.ehrUpdateTime,
            'is_ehr_data_available': int(summary.isEhrDataAvailable),
            'was_ehr_data_available': int(summary.wasEhrDataAvailable),
            'enrollment_status_v3_2': str(summary.enrollmentStatusV3_2),
            'enrollment_status_v3_2_id': int(summary.enrollmentStatusV3_2),
            'health_datastream_sharing_status': str(summary.healthDataStreamSharingStatus),
            'health_datastream_sharing_status_id': int(summary.healthDataStreamSharingStatus),
            'health_datastream_sharing_status_time': summary.healthDataStreamSharingStatusTime
        }
    } for summary in records]

    try:
        dispatch_participant_rebuild_tasks(patch_data, batch_size=batch_size, project_id=project_id)

    except BadGateway as e:
        LOG.error(f'Bad Gateway: {e}', exc_info=True)

    except InternalServerError as e:
        LOG.error(f'Internal Server Error: {e}', exc_info=True)

    except HTTPException as e:
        LOG.error(f'HTTP Exception: {e}', exc_info=True)

    except Exception:  # pylint: disable=broad-except
        LOG.error(f'Exception encountered', exc_info=True)


def make_update_organizations_job():
    config_param = config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION
    try:
        bigquery_view = config.getSetting(config_param, None)
    except config.InvalidConfigException as e:
        LOG.warning("Config lookup exception for {}: {}".format(config_param, e))
        bigquery_view = None
    if bigquery_view:
        query = "SELECT org_id, person_upload_time FROM `{}`".format(bigquery_view)
        twenty_second_timeout = 20_000
        return bigquery.BigQueryJob(
            query, default_dataset_id="operations_analytics", page_size=1000, socket_timeout=twenty_second_timeout
        )
    else:
        return None


def update_organizations():
    """
  Creates EhrRecipts for organizations

  Loads results in batches and commits updates to database per batch.
  """
    job = make_update_organizations_job()
    if job is not None:
        update_organizations_from_job(job)
    else:
        LOG.warning("Skipping update_organizations because of invalid config")


def update_organizations_from_job(job):
    organization_dao = OrganizationDao()
    receipt_dao = EhrReceiptDao()
    for page in job:
        for row in page:
            org = organization_dao.get_by_external_id(row.org_id)
            if org:
                try:
                    receipt_time = datetime_as_naive_utc(row.person_upload_time)
                except TypeError:
                    continue
                receipt_dao.get_or_create(
                    insert_if_created=True, organizationId=org.organizationId, receiptTime=receipt_time
                )
