import clock
import cloud_utils.curation
import config
from dao.ehr_dao import EhrReceiptDao
from dao.participant_summary_dao import ParticipantSummaryDao
from google.appengine.ext import deferred


def update_ehr_status():
  now = clock.CLOCK.now()
  for row in _query_ehr_upload_pids():
    deferred.defer(
      _do_update,
      participant_id=int(row['person_id']),
      # NOTE: the recorded_time is missing data, fill in with NOW until solved
      recorded_time=now,
      received_time=now
    )


def _query_ehr_upload_pids():
  query_string = 'select * from `{project}`.`{dataset}`.`{view}` limit 1'.format(
    project=config.getSetting(config.CURATION_BIGQUERY_PROJECT),
    dataset='operations_analytics',
    view='ehr_upload_pids'
  )
  return cloud_utils.curation.query(query_string)


def _do_update(
  participant_id,
  recorded_time,
  received_time
):
  summary_dao = ParticipantSummaryDao()
  receipt_dao = EhrReceiptDao()
  summary = summary_dao.get(participant_id)
  receipt = summary_dao.update_with_new_ehr(
    participant_summary=summary,
    recorded_time=recorded_time,
    received_time=received_time
  )
  summary_dao.update(summary)
  receipt_dao.insert(receipt)
