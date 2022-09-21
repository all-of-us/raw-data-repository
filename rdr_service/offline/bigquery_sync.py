import json
import logging
import math
import random
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import func

from rdr_service import config
from rdr_service.cloud_utils.bigquery import BigQueryJob
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.config import GAE_PROJECT
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.bq_code_dao import rebuild_bq_codebook_task
from rdr_service.dao.bq_hpo_dao import bq_hpo_update
from rdr_service.dao.bq_organization_dao import bq_organization_update
from rdr_service.dao.bq_site_dao import bq_site_update
from rdr_service.model.bigquery_sync import BigQuerySync
from rdr_service.model.participant import Participant
from rdr_service.resource.generators.code import rebuild_codebook_resources_task
from rdr_service.resource.tasks import batch_rebuild_participants_task


# disable pylint warning for 'Exception':
# pylint: disable=redefined-builtin
# pylint: disable=unused-argument

class BigQueryJobError(BaseException):
    """ BigQuery Job Exception """


# Only perform BQ/Resource operations in these environments.
_bq_env = ['localhost', 'pmi-drc-api-test', 'all-of-us-rdr-sandbox', 'all-of-us-rdr-stable', 'all-of-us-rdr-prod']


def dispatch_participant_rebuild_tasks(pid_list, batch_size=100, project_id=GAE_PROJECT, build_locally=None,
                                       build_modules=True, build_participant_summary=True):
    """
    A utility routine to handle dispatching batched requests for rebuilding participants.  Is also called
    from other cron job endpoint handlers (e.g., biobank reconciliation and EHR status update jobs)
    :param pid_list:  List of participant_id values or dicts with patch data to rebuild
    :param batch_size:  Size of the batch of participant IDs to include in the rebuild task payload
    :param project_id: String identifier for the GAE project
    :param build_locally: Boolean value indicating whether to build participant summaries in this process.
        If False is given, GCP tasks are created for rebuilding participants. Defaults to True if the project_id value
        is localhost
    :param build_participant_summary:  Boolean value indicating whether PDR participant summary data should be rebuilt
    :param build_modules: Boolean value indicating whether PDR module data for the participant should be rebuilt
    """

    if config.GAE_PROJECT not in _bq_env:
        logging.warning(f'BigQuery operations not supported in {config.GAE_PROJECT}, skipping.')
        return

    count = 0
    batch_count = 0
    batch = list()
    task = GCPCloudTask()

    if build_locally is None:
        build_locally = project_id == 'localhost'

    # queue up a batch of participant ids and send them to be rebuilt.
    for pid_data in pid_list:
        if isinstance(pid_data, (int, str)):
            batch.append({'pid': pid_data})
        elif isinstance(pid_data, dict):
            # payload = {'pid': pid_data['pid'], 'patch': pid_data['patch']}
            batch.append(pid_data)

        count += 1

        if count == batch_size:
            payload = {'batch': batch, 'build_participant_summary': build_participant_summary,
                       'build_modules': build_modules}

            if build_locally:
                batch_rebuild_participants_task(payload, project_id=project_id)
            else:
                task.execute('rebuild_participants_task', payload=payload, in_seconds=30,
                             queue='resource-rebuild', quiet=True, project_id=project_id)

            batch_count += 1
            # reset for next batch
            batch = list()
            count = 0

    # send last batch if needed.
    if count:
        payload = {'batch': batch, 'build_participant_summary': build_participant_summary,
                   'build_modules': build_modules}
        batch_count += 1
        if build_locally:
            batch_rebuild_participants_task(payload, project_id=project_id)
        else:
            task.execute('rebuild_participants_task', payload=payload, in_seconds=30,
                         queue='resource-rebuild', quiet=True, project_id=project_id)

    logging.info(f'Submitted {batch_count} tasks.')


def rebuild_bigquery_handler():
    """
    Cron job handler, setup queued tasks to rebuild bigquery data.
    Tasks call the default API service, so we want to use small batch sizes.
    """
    if config.GAE_PROJECT not in _bq_env:
        logging.warning(f'BigQuery operations not supported in {config.GAE_PROJECT}, skipping.')
        return

    batch_size = 100
    ro_dao = BigQuerySyncDao(backup=True)
    with ro_dao.session() as ro_session:
        total_rows = ro_session.query(func.count(Participant.participantId)).first()[0]
        count = int(math.ceil(float(total_rows) / float(batch_size)))
        logging.info('Calculated {0} tasks from {1} records with a batch size of {2}.'.
                     format(count, total_rows, batch_size))

        pids = [row.participantId for row in ro_session.query(Participant.participantId).all()]
        dispatch_participant_rebuild_tasks(pids, batch_size=batch_size)

    #
    # Process tables that don't need to be broken up into smaller tasks.
    #
    # Code Table
    rebuild_bq_codebook_task()
    rebuild_codebook_resources_task()
    # HPO Table
    bq_hpo_update()
    # Organization Table
    bq_organization_update()
    # Site Table
    bq_site_update()


def daily_rebuild_bigquery_handler():
    """
    Cron job handler, setup queued tasks to with participants that need to be rebuilt.
    Tasks call the default API service, so we want to use small batch sizes.
    """
    if config.GAE_PROJECT not in _bq_env:
        logging.warning(f'BigQuery operations not supported in {config.GAE_PROJECT}, skipping.')
        return

    ro_dao = BigQuerySyncDao(backup=True)
    with ro_dao.session() as ro_session:
        # Find all BQ records where enrollment status or withdrawn statuses are different.
        sql = """
        select bqs.pk_id as participantId
        from participant_summary ps
             JOIN bigquery_sync bqs ON bqs.pk_id = ps.participant_id
        where (ps.enrollment_status = 3 and JSON_EXTRACT(resource, "$.enrollment_status_id") <> 3) or
              (ps.withdrawal_status = 2 and JSON_EXTRACT(resource, "$.withdrawal_status_id") <> 2);
        """

        participants = ro_session.execute(sql)
        if not participants:
            logging.info(f'No participants found to rebuild.')
            return
        pid_list = [p.participantId for p in participants]
        dispatch_participant_rebuild_tasks(pid_list, batch_size=100)


def insert_batch_into_bq(bq, project_id, dataset, table, batch, dryrun=False):
    """
    Bulk insert table rows into bigquery using the InsertAll api.
    :param bq: bigquery object created from build()
    :param project_id: gcp project id to send data to.
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

    if not project_id:
        project_id = config.GAE_PROJECT

    if dryrun is False:
        task = bq.tabledata().insertAll(projectId=project_id, datasetId=dataset, tableId=table, body=body)
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


def sync_bigquery_handler(dryrun=False):
    """
    Cron entry point, Sync MySQL records to bigquery.
    :param dryrun: Don't send to bigquery if True
    Links for Streaming Inserts:
    # https://cloud.google.com/bigquery/streaming-data-into-bigquery
    # https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency
    # https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll
    # https://cloud.google.com/bigquery/troubleshooting-errors#streaming
    """
    if config.GAE_PROJECT not in _bq_env:
        return

    ro_dao = BigQuerySyncDao(backup=True)
    # https://github.com/googleapis/google-api-python-client/issues/299
    # https://github.com/pior/appsecrets/issues/7
    bq = build('bigquery', 'v2', cache_discovery=False) if dryrun is False else None
    total_inserts = 0
    # Google says maximum of 500 in a batch. Pretty sure they are talking about log shipping, I believe
    # that one participant summary record is larger than their expected average log record size.
    batch_size = 100
    run_limit = (2 * 60) - 10  # Run for 110 seconds before exiting, so we don't have overlapping cron jobs.
    limit_reached = False
    start_ts = datetime.now()
    table_list = list()

    with ro_dao.session() as ro_session:
        tables = ro_session.query(BigQuerySync.projectId, BigQuerySync.datasetId, BigQuerySync.tableId). \
            distinct(BigQuerySync.projectId, BigQuerySync.datasetId, BigQuerySync.tableId). \
            filter(BigQuerySync.projectId != None).all()

        # don't always process the list in the same order so we don't get stuck processing the same table each run.
        for table_row in tables:
            table_list.append((table_row.projectId, table_row.datasetId, table_row.tableId))
        random.shuffle(table_list)

        for item in table_list:
            project_id = item[0]
            dataset_id = item[1]
            table_id = item[2]
            count = 0
            errors = ''
            error_count = 0
            try:
                # pylint: disable=unused-variable
                max_created, max_modified = _get_remote_max_timestamps(project_id, dataset_id, table_id) \
                    if dryrun is False else (datetime.min, datetime.min)
            except BigQueryJobError:
                logging.warning('Failed to retrieve max date values from bigquery, skipping this run.')
                return 0

            # figure out how many records need to be sync'd and divide into slices.
            total_rows = ro_session.query(BigQuerySync.id). \
                filter(BigQuerySync.projectId == project_id, BigQuerySync.tableId == table_id,
                       BigQuerySync.datasetId == dataset_id, BigQuerySync.modified >= max_modified).count()

            if total_rows == 0:
                logging.info('No rows to sync for {0}.{1}.'.format(dataset_id, table_id))
                continue
            slices = int(math.ceil(float(total_rows) / float(batch_size)))
            slice_num = 0

            while slice_num < slices:
                results = ro_session.query(BigQuerySync.id, BigQuerySync.created, BigQuerySync.modified). \
                    filter(BigQuerySync.projectId == project_id, BigQuerySync.tableId == table_id,
                           BigQuerySync.datasetId == dataset_id, BigQuerySync.modified >= max_modified). \
                    order_by(BigQuerySync.modified).limit(batch_size).all()
                slice_num += 1
                batch = list()

                for row in results:
                    count += 1
                    max_modified = row.modified
                    rec = ro_session.query(BigQuerySync.resource).filter(BigQuerySync.id == row.id).first()
                    if isinstance(rec.resource, str):
                        rec_data = json.loads(rec.resource)
                    else:
                        rec_data = rec.resource
                    rec_data['id'] = row.id
                    rec_data['created'] = row.created.isoformat()
                    rec_data['modified'] = row.modified.isoformat()
                    data = {
                        'insertId': str(row.id),
                        'json': rec_data
                    }
                    batch.append(data)

                if len(batch) > 0:
                    result, resp = insert_batch_into_bq(bq, project_id, dataset_id, table_id, batch, dryrun)
                    if result is False:
                        # errors are cumulative, so wait until the end of the while
                        # statement before printing errors.
                        errors = resp
                        error_count += len(resp['insertErrors'])
                # Don't exceed our execution time limit.
                if (datetime.now() - start_ts).seconds > run_limit:
                    logging.info('Hit {0} second time limit.'.format(run_limit))
                    limit_reached = True
                    break

            if errors:
                logging.error(errors)

            total_inserts += (count - error_count)
            msg = '{0} inserts and {1} errors for {2}.{3}.{4}'.format(count, error_count, project_id, dataset_id,
                                                                      table_id)
            if error_count == 0:
                logging.info(msg)
            else:
                logging.info(msg)

            if limit_reached:
                break

    return total_inserts


def _get_remote_max_timestamps(project_id, dataset_id, table_id):
    """
    Get the max created and modified dates from the BigQuery table so we can
    determine which records we want to sync from MySQL.
    :param dataset_id: dataset name
    :param table_id: table name
    :return: tuple (max_created, max_modified)
    """
    query = 'select max(created) as max_created, max(modified) as max_modified from {0}.{1}'. \
        format(dataset_id, table_id)

    try:
        job = BigQueryJob(query, project_id=project_id, default_dataset_id=dataset_id)
    except HttpError:
        raise BigQueryJobError("Failed to retrieve max timestamps from BigQuery.")

    for page in job:
        for row in page:
            mc = row.max_created if row.max_created else datetime.min
            mm = row.max_modified if row.max_modified else datetime.min
            return mc, mm

    raise LookupError('Failed to get max created and modified values from bigquery table {0}.{1}.{2}'.
                      format(project_id, dataset_id, table_id))
