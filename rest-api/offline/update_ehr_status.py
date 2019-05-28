import collections
import logging

import clock
from dao.participant_summary_dao import ParticipantSummaryDao

import cloud_utils.bigquery
from participant_enums import EhrStatus

LOG = logging.getLogger(__name__)


SubmissionInfo = collections.namedtuple('SubmissionInfo', ('bucket_name', 'date', 'person_file'))
OrganizationInfo = collections.namedtuple('OrganizationInfo',
                                          ('id', 'submission_date', 'person_file'))


def update_ehr_status():
  """
  Entrypoint, executed as a cron job
  """
  query = 'SELECT * FROM `aou-res-curation-prod.operations_analytics.ehr_upload_pids`'
  job = cloud_utils.bigquery.BigQueryJob(
    query,
    project_id='all-of-us-rdr-sandbox',
    dataset_id='operations_analytics',
    page_size=1000
  )

  #individual_update(job)
  batched_update(job)


def individual_update(job):
  summary_dao = ParticipantSummaryDao()
  for page in job.execute_and_iter_pages():
    for row in page:
      summary = summary_dao.get(row.person_id)
      if summary.ehrStatus != EhrStatus.PRESENT:
        summary.ehrStatus = EhrStatus.PRESENT
        summary.ehrReciptTime = clock.CLOCK.now()
        summary_dao.update(summary)


def batched_update(job):
  summary_dao = ParticipantSummaryDao()
  for page in job.execute_and_iter_pages():
    participant_ids = set()
    for row in page:
      participant_ids.add(row.person_id)
    summary_dao.bulk_update_ehr_status(participant_ids)
