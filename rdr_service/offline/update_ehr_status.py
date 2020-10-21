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
    update_particiant_summaries()
    logging.info('Update EHR complete')


def make_update_participant_summaries_job():
    config_param = config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT
    try:
        bigquery_view = config.getSetting(config_param, None)
    except config.InvalidConfigException as e:
        LOG.warning("Config lookup exception for {}: {}".format(config_param, e))
        bigquery_view = None
    if bigquery_view:
        query = "SELECT person_id FROM `{}`".format(bigquery_view)
        return bigquery.BigQueryJob(query, default_dataset_id="operations_analytics", page_size=1000)
    else:
        return None


def update_particiant_summaries():
    """
  Updates ehr status on participant summaries

  Loads results in batches and commits updates to database per batch.
  """
    job = make_update_participant_summaries_job()
    if job is not None:
        update_participant_summaries_from_job(job)
    else:
        LOG.warning("Skipping update_participant_summaries because of invalid config")


def update_participant_summaries_from_job(job):
    summary_dao = ParticipantSummaryDao()
    now = clock.CLOCK.now()
    batch_size = 100
    for i, page in enumerate(job):
        LOG.info("Processing page {} of results...".format(i))
        parameter_sets = [{"pid": row.person_id, "receipt_time": now} for row in page]
        query_result = summary_dao.bulk_update_ehr_status(parameter_sets)
        total_rows = query_result.rowcount
        LOG.info("Affected {} rows.".format(total_rows))
        if total_rows > 0:

            count = int(math.ceil(float(total_rows) / float(batch_size)))
            LOG.info('UpdateEhrStatus: calculated {0} participant rebuild tasks from {1} records and batch size of {2}'.
                     format(count, total_rows, batch_size))
            pids = [param['pid'] for param in parameter_sets]

            with summary_dao.session() as session:
                cursor = session.query(ParticipantSummary.participantId, ParticipantSummary.ehrReceiptTime).all()
                records = [r for r in cursor if r.participantId in pids]

            patch_data = [{
                'pid': rec.participantId,
                'patch': {
                    'ehr_status': str(EhrStatus.PRESENT),
                    'ehr_status_id': int(EhrStatus.PRESENT),
                    'ehr_receipt': rec.ehrReceiptTime if rec.ehrReceiptTime else now,
                    'ehr_update': now}
            } for rec in records]
            try:
                dispatch_participant_rebuild_tasks(patch_data, batch_size=batch_size)

            except BadGateway as e:
                LOG.error(f'Bad Gateway: {e}', exc_info=True)

            except InternalServerError as e:
                LOG.error(f'Internal Server Error: {e}', exc_info=True)

            except HTTPException as e:
                LOG.error(f'HTTP Exception: {e}', exc_info=True)


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
