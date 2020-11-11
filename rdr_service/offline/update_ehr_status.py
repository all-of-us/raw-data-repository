import logging
import math

from werkzeug.exceptions import HTTPException, InternalServerError, BadGateway

from rdr_service import clock, config
from rdr_service.app_util import datetime_as_naive_utc
from rdr_service.cloud_utils import bigquery
from rdr_service.dao.ehr_dao import EhrReceiptDao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.ehr import ParticipantEhrReceipt
from rdr_service.model.participant import Participant
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


def _track_historical_participant_ehr_data(session, participant_id, file_time, job_time):
    record = session.query(ParticipantEhrReceipt).filter(
        ParticipantEhrReceipt.participantId == participant_id,
        ParticipantEhrReceipt.fileTimestamp == file_time
    ).one_or_none()

    if record is None:
        # Check that the participant exists
        participant = session.query(Participant).filter(Participant.participantId == participant_id).one_or_none()
        if participant is None:
            logging.warning(f'Skipping ehr receipt record for non-existent participant "{participant_id}"')
            return

        record = ParticipantEhrReceipt(
            participantId=participant_id,
            fileTimestamp=file_time,
            firstSeen=job_time
        )
        session.add(record)

    record.lastSeen = job_time


def update_participant_summaries_from_job(job, project_id=None):
    summary_dao = ParticipantSummaryDao()
    summary_dao.prepare_for_ehr_status_update()
    now = clock.CLOCK.now()
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
                _track_historical_participant_ehr_data(session, participant_id, file_upload_time, now)

        query_result = summary_dao.bulk_update_ehr_status(parameter_sets)
        total_rows = query_result.rowcount
        LOG.info("Affected {} rows.".format(total_rows))

        if total_rows > 0:
            count = int(math.ceil(float(total_rows) / float(batch_size)))
            LOG.info('UpdateEhrStatus: calculated {0} participant rebuild tasks from {1} records and batch size of {2}'.
                     format(count, total_rows, batch_size))
            pids = [param['pid'] for param in parameter_sets]

            with summary_dao.session() as session:
                cursor = session.query(
                    ParticipantSummary.participantId,
                    ParticipantSummary.ehrReceiptTime,
                    ParticipantSummary.ehrUpdateTime
                ).all()
                records = [r for r in cursor if r.participantId in pids]

            patch_data = [{
                'pid': summary.participantId,
                'patch': {
                    'ehr_status': str(EhrStatus.PRESENT),
                    'ehr_status_id': int(EhrStatus.PRESENT),
                    'ehr_receipt': summary.ehrReceiptTime,
                    'ehr_update': summary.ehrUpdateTime
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
