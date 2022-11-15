from datetime import datetime, timedelta
import logging
import math
from sqlalchemy import bindparam
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.sql import func
from werkzeug.exceptions import HTTPException, InternalServerError, BadGateway

from rdr_service import config
from rdr_service.app_util import datetime_as_naive_utc
from rdr_service.cloud_utils import bigquery
from rdr_service.config import GAE_PROJECT
from rdr_service.dao.ehr_dao import EhrReceiptDao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.ehr import ParticipantEhrReceipt
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import EhrStatus
from rdr_service.offline.bigquery_sync import dispatch_participant_rebuild_tasks

LOG = logging.getLogger(__name__)


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
        query = "SELECT person_id, latest_upload_time FROM `{}`".format(bigquery_view)
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


def _track_historical_participant_ehr_data(session, parameter_sets):
    query = insert(ParticipantEhrReceipt).values({
        ParticipantEhrReceipt.participantId: bindparam('pid'),
        ParticipantEhrReceipt.fileTimestamp: bindparam('receipt_time'),
        ParticipantEhrReceipt.firstSeen: func.utc_timestamp(),
        ParticipantEhrReceipt.lastSeen: func.utc_timestamp()
    }).on_duplicate_key_update({
        'last_seen': func.utc_timestamp()
    }).prefix_with('IGNORE')

    session.execute(query, parameter_sets)


def update_participant_summaries_from_job(job, project_id=GAE_PROJECT):
    summary_dao = ParticipantSummaryDao()
    participant_ids_that_previously_had_ehr = summary_dao.get_participant_ids_with_ehr_data_available()
    new_ids_with_status_checked = set()  # Any participant ids with new files that have already had their status updated
    summary_dao.prepare_for_ehr_status_update()
    job_start_time = datetime.utcnow()

    batch_size = 100
    for i, page in enumerate(job):
        LOG.info("Processing page {} of results...".format(i))
        parameter_sets = []
        with summary_dao.session() as session:
            for row in page:
                participant_id = row.person_id
                file_upload_time = row.latest_upload_time

                parameter_sets.append({
                    "pid": participant_id,
                    "receipt_time": file_upload_time
                })
                if participant_id in participant_ids_that_previously_had_ehr:
                    # Remove any participants that are part of the current page, they're being rebuilt currently, so
                    # they won't need to rebuilt again at the end (when we get the ones that are no longer in the view)
                    participant_ids_that_previously_had_ehr.discard(participant_id)
                elif participant_id not in new_ids_with_status_checked:
                    # For any participants that got EHR files for the first time: check their enrollment status and
                    # make sure we don't check it again if they have another file
                    summary = summary_dao.get_for_update(session=session, obj_id=participant_id)
                    if summary is None:
                        LOG.error(f'No summary found for P{participant_id}')
                    else:
                        summary_dao.update_enrollment_status(session=session, summary=summary)
                    new_ids_with_status_checked.add(participant_id)

            _track_historical_participant_ehr_data(session, parameter_sets)
            query_result = summary_dao.bulk_update_ehr_status_with_session(session, parameter_sets)
            session.commit()

            total_rows = query_result.rowcount
            LOG.info("Affected {} rows.".format(total_rows))

            if total_rows > 0:
                # Rebuild participants in the page that have new data available. Checking that the participant
                #  ehr receipts were created since the job started (offsetting by an hour to account for any
                #  difference between server times)
                participant_ids_in_page = [param['pid'] for param in parameter_sets]
                new_ehr_data_results = session.query(ParticipantEhrReceipt.participantId).filter(
                    ParticipantEhrReceipt.firstSeen >= job_start_time - timedelta(hours=1),
                    ParticipantEhrReceipt.participantId.in_(participant_ids_in_page)
                ).all()
                # TODO: only get the participants that actually changed

                participant_ids_with_new_ehr_data = [row.participantId for row in new_ehr_data_results]
                create_rebuild_tasks_for_participants(participant_ids_with_new_ehr_data,
                                                      batch_size, project_id, summary_dao)

    LOG.info(f'Rebuilding {len(participant_ids_that_previously_had_ehr)} '
             f'participants that no longer appear in the view')
    create_rebuild_tasks_for_participants(participant_ids_that_previously_had_ehr, batch_size, project_id, summary_dao)


def create_rebuild_tasks_for_participants(participant_id_list, batch_size, project_id, dao):
    with dao.session() as session:
        records = session.query(
            ParticipantSummary.participantId,
            ParticipantSummary.ehrReceiptTime,
            ParticipantSummary.ehrUpdateTime,
            ParticipantSummary.isEhrDataAvailable
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
            'is_ehr_data_available': int(summary.isEhrDataAvailable)
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
        return bigquery.BigQueryJob(query, default_dataset_id="operations_analytics", page_size=1000)
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
