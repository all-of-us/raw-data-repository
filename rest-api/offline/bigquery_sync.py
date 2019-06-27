import json
import logging
import math
from datetime import datetime

from google.appengine.api import app_identity
from googleapiclient.discovery import build
from sqlalchemy import or_

from cloud_utils.bigquery import BigQueryJob
from dao.bigquery_sync_dao import BQParticipantSummaryGenerator, BigQuerySyncDao
from dao.participant_dao import ParticipantDao
from model.bigquery_sync import BigQuerySync
from model.participant import Participant


def rebuild_bigquery_data():
  """
  Loop through all participants and generate the BQ participant summary data and
  store it in the biguqery_sync table.
  Warning: this will force a rebuild and eventually a re-sync for every participant record.
  """
  try:
    app_id = app_identity.get_application_id()
  except AttributeError:
    app_id = 'localhost'
  dao = ParticipantDao()
  bqgen = BQParticipantSummaryGenerator()

  with dao.session() as session:
    results = session.query(Participant.participantId).all()
    count = 0
    excluded = 0
    # put a log entry in every 5,000 records. Should be approximately every 10 minutes.
    for row in results:
      count += 1
      if count % 5000 == 0:
        logging.info('processed {0} participants'.format(count))

      bqr = bqgen.make_participant_summary(row.participantId)
      # filter test or ghost participants if production
      if app_id == 'all-of-us-rdr-prod':  # or app_id == 'localhost':
        if bqr.is_ghost_id == 1 or not bqr.hpo or bqr.hpo == 'TEST' or not bqr.email or '@example.com' in bqr.email:
          excluded += 1
          continue

      bqgen.save_participant_summary(row.participantId, bqr)

    logging.info('rebuilt bigquery data for {0} participants, excluded {1} test or ghost participants.'.format(
      count - excluded, excluded))


def insert_batch_into_bq(bq, dataset, table, batch, dryrun=False):
  """
  Bulk insert table rows into bigquery using the InsertAll api.
  :param bq: bigquery object created from build()
  :param dataset: dataset name to insert into
  :param table: table name to insert into
  :param batch: A list of rows to insert into bigquery
  :param dryrun: Don't send to bigquery if True
  :return: True if insert was successful, otherwise False
  """
  body = {
    'kind': 'bigquery#tableDataInsertAllRequest',
    'ignoreUnknownValues': 'true',
    'skipInvalidRows': 'true',
    'rows': batch,
  }

  if dryrun is False:
    task = bq.tabledata().insertAll(projectId=app_identity.get_application_id(), datasetId=dataset,
                                    tableId=table, body=body)
    resp = task.execute()
  else:
    resp = {u'kind': u'bigquery#tableDataInsertAllResponse'}
  # success resp : {u'kind': u'bigquery#tableDataInsertAllResponse'}
  # error resp   : {u'kind': u'bigquery#tableDataInsertAllResponse', u'insertErrors': [
  #                  {u'index': 0, u'errors': [{u'debugInfo': u'', u'reason': u'invalid', u'message':
  #                         u'Missing required field: biobank_id.', u'location': u''}]}
  #                ]}
  if 'insertErrors' in resp:
    return False, resp
  return True, resp


def sync_bigquery(dryrun=False):
  """
  Cron entry point, Sync MySQL records to bigquery.
  :param dryrun: Don't send to bigquery if True
  Links for Streaming Inserts:
  # https://cloud.google.com/bigquery/streaming-data-into-bigquery
  # https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency
  # https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
  # https://cloud.google.com/bigquery/troubleshooting-errors#streaming
  """
  dataset = 'rdr_ops_data_view'
  tables = ['participant_summary']
  dao = BigQuerySyncDao()
  bq = build('bigquery', 'v2') if dryrun is False else None
  total_inserts = 0
  # Google says maximum of 500 in a batch. Pretty sure they are talking about log shipping, I believe
  # that one participant summary record is larger than their expected average log record size.
  batch_size = 250
  run_limit = (2 * 60) - 30  # Only run for 90 seconds before exiting, so we don't have overlapping cron jobs.
  start_ts = datetime.now()

  with dao.session() as session:

    for table in tables:
      count = 0
      errors = ''
      max_created, max_modified = \
            _get_remote_max_timestamps(dataset, table) if dryrun is False else datetime.min
      # figure out how many records need to be sync'd and divide into slices.
      total_rows = session.query(BigQuerySync.id). \
                  filter(or_(BigQuerySync.created > max_created, BigQuerySync.modified > max_modified)).count()
      slices = int(math.ceil(float(total_rows) / float(batch_size)))
      slice_num = 0

      while slice_num < slices:
        results = session.query(BigQuerySync.id, BigQuerySync.created, BigQuerySync.modified). \
              filter(or_(BigQuerySync.created > max_created, BigQuerySync.modified > max_modified)).\
              order_by(BigQuerySync.modified).\
              slice(slice_num * batch_size, (slice_num + 1) * batch_size).\
              all()
        slice_num += 1
        batch = list()

        for row in results:
          count += 1
          rec = session.query(BigQuerySync.resource).filter(BigQuerySync.id == row.id).first()
          rec_data = json.loads(rec.resource)
          rec_data['id'] = row.id
          rec_data['created'] = row.created.isoformat()
          rec_data['modified'] = row.modified.isoformat()
          data = {
            'insertId': str(row.id),
            'json': rec_data
          }
          batch.append(data)

        if len(batch) > 0:
          result, resp = insert_batch_into_bq(bq, dataset, table, batch, dryrun)
          if result is False:
            # errors are cumulative, so wait until the end of the while
            # statement before printing errors.
            errors = resp
        # Don't exceed our execution time limit.
        if (datetime.now() - start_ts).seconds > run_limit:
          logging.info('hit {0} second time limit.'.format(run_limit))
          break

      if errors:
        logging.error(errors)

      total_inserts += count
      logging.info('inserted {0} records into {1}.{2}.'.format(count, dataset, table))

  return total_inserts


def _get_remote_max_timestamps(dataset, table):
  """
  Get the max created and modified dates from the BigQuery table so we can
  determine which records we want to sync from MySQL.
  :param dataset: dataset name
  :param table: table name
  :return: tuple (max_created, max_modified)
  """
  query = 'select max(created) as max_created, max(modified) as max_modified from {0}.{1}'.format(dataset, table)

  job = BigQueryJob(query, project_id=app_identity.get_application_id(), default_dataset_id=dataset)
  for page in job:
    for row in page:
      mc = row.max_created if row.max_created else datetime.min
      mm = row.max_modified if row.max_modified else datetime.min
      return mc, mm

  raise LookupError('failed to get max created and modified values from bigquery table {0}.{1}'.
                    format(dataset, table))
