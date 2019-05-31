import logging

import pytz

import clock
import cloud_utils.bigquery
from dao.ehr_dao import EhrReceiptDao
from dao.organization_dao import OrganizationDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.ehr import EhrReceipt

LOG = logging.getLogger(__name__)


def update_ehr_status():
  """
  Entrypoint, executed as a cron job.
  """
  update_particiant_summaries()
  update_organizations()


def make_update_participant_summaries_job():
  query = 'SELECT * FROM `aou-res-curation-prod.operations_analytics.ehr_upload_pids`'
  return cloud_utils.bigquery.BigQueryJob(
    query,
    project_id='all-of-us-rdr-sandbox',
    default_dataset_id='operations_analytics',
    page_size=1000
  )


def update_particiant_summaries():
  """
  Updates ehr status on participant summaries

  Loads results in batches and commits updates to database per batch.
  """
  job = make_update_participant_summaries_job()
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


def make_update_organizations_job():
  query = (
    'SELECT org_id, person_upload_time '
    'FROM `aou-res-curation-prod.operations_analytics'
    '.table_counts_with_upload_timestamp_for_hpo_sites_v2`'
  )
  return cloud_utils.bigquery.BigQueryJob(
    query,
    project_id='all-of-us-rdr-sandbox',
    default_dataset_id='operations_analytics',
    page_size=1000
  )


def update_organizations():
  job = make_update_organizations_job()
  organization_dao = OrganizationDao()
  receipt_dao = EhrReceiptDao()
  for page in job:
    for row in page:
      org = organization_dao.get_by_external_id(row.org_id)
      receipt = EhrReceipt(
        organizationId=org.organizationId,
        receiptTime=row.person_upload_time.astimezone(pytz.UTC).replace(tzinfo=None)
      )
      receipt_dao.insert(receipt)
