import logging

import clock
import cloud_utils.bigquery
from dao.participant_summary_dao import ParticipantSummaryDao


LOG = logging.getLogger(__name__)


def update_ehr_status():
  """
  Entrypoint, executed as a cron job.

  Loads results in batches and commits updates to database per batch.
  """
  query = 'SELECT * FROM `aou-res-curation-prod.operations_analytics.ehr_upload_pids`'
  job = cloud_utils.bigquery.BigQueryJob(
    query,
    project_id='all-of-us-rdr-sandbox',
    default_dataset_id='operations_analytics',
    page_size=1000
  )

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
