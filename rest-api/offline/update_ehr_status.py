import logging

import clock
import cloud_utils.bigquery
import config
from app_util import datetime_as_naive_utc
from dao.ehr_dao import EhrReceiptDao
from dao.organization_dao import OrganizationDao
from dao.participant_summary_dao import ParticipantSummaryDao


LOG = logging.getLogger(__name__)


def update_ehr_status():
  """
  Entrypoint, executed as a cron job.
  """
  update_particiant_summaries()
  update_organizations()


def make_update_participant_summaries_job():
  config_param = config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT
  try:
    bigquery_view = config.getSetting(config_param, None)
  except config.InvalidConfigException as e:
    LOG.warn("Config lookup exception for {}: {}".format(config_param, e))
    bigquery_view = None
  if bigquery_view:
    query = 'SELECT person_id FROM `{}`'.format(bigquery_view)
    return cloud_utils.bigquery.BigQueryJob(
      query,
      default_dataset_id='operations_analytics',
      page_size=1000
    )
  else:
    return None


def update_particiant_summaries():
  """
  Updates ehr status on participant summaries

  Loads results in batches and commits updates to database per batch.
  """
  job = make_update_participant_summaries_job()
  if job is not None:
    summary_dao = ParticipantSummaryDao()
    now = clock.CLOCK.now()
    for i, page in enumerate(job):
      LOG.info("Processing page {} of results...".format(i))
      parameter_sets = [
        {
          'pid': row.person_id,
          'receipt_time': now,
        }
        for row in page
      ]
      query_result = summary_dao.bulk_update_ehr_status(parameter_sets)
      LOG.info("Affected {} rows.".format(query_result.rowcount))
  else:
    LOG.warn("Skipping update_participant_summaries because of invalid config")


def make_update_organizations_job():
  config_param = config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION
  try:
    bigquery_view = config.getSetting(config_param, None)
  except config.InvalidConfigException as e:
    LOG.warn("Config lookup exception for {}: {}".format(config_param, e))
    bigquery_view = None
  if bigquery_view:
    query = 'SELECT org_id, person_upload_time FROM `{}`'.format(bigquery_view)
    return cloud_utils.bigquery.BigQueryJob(
      query,
      default_dataset_id='operations_analytics',
      page_size=1000
    )
  else:
    return None


def update_organizations():
  job = make_update_organizations_job()
  if job is not None:
    organization_dao = OrganizationDao()
    receipt_dao = EhrReceiptDao()
    for page in job:
      for row in page:
        org = organization_dao.get_by_external_id(row.org_id)
        receipt_dao.get_or_create(
          insert_if_created=True,
          organizationId=org.organizationId,
          receiptTime=datetime_as_naive_utc(row.person_upload_time)
        )
  else:
    LOG.warn("Skipping update_organizations because of invalid config")
