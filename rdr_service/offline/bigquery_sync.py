import json
import logging
import math
import random
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import or_, func, and_

from rdr_service import config
from rdr_service.cloud_utils.bigquery import BigQueryJob
from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.dao.bq_code_dao import bq_codebook_update_task
from rdr_service.dao.bq_hpo_dao import bq_hpo_update
from rdr_service.dao.bq_organization_dao import bq_organization_update
from rdr_service.dao.bq_participant_summary_dao import BQParticipantSummaryGenerator, rebuild_bq_participant
from rdr_service.dao.bq_pdr_participant_summary_dao import BQPDRParticipantSummaryGenerator
from rdr_service.dao.bq_questionnaire_dao import BQPDRQuestionnaireResponseGenerator
from rdr_service.dao.bq_site_dao import bq_site_update
from rdr_service.model.bigquery_sync import BigQuerySync
from rdr_service.model.bq_questionnaires import BQPDRConsentPII, BQPDRTheBasics, BQPDRLifestyle, BQPDROverallHealth, \
    BQPDREHRConsentPII, BQPDRDVEHRSharing
from rdr_service.model.participant import Participant
from rdr_service.services.flask import celery


# disable pylint warning for 'Exception':
# pylint: disable=redefined-builtin
# pylint: disable=unused-argument
class BigQueryJobError(BaseException):
    """ BigQuery Job Exception """


def rebuild_bigquery_handler():
    """
    Cron job handler, setup queued tasks to rebuild bigquery data
    # TODO: Future: Currently rebuild is synchronous, this could be asynchronous if we
    #       passed a set of participant ids to each task.  GET requests are limited to 2,083
    #       characters, so we probably would have to create a temp MySQL table to store
    #       batches of participant ids and then we could pass a batch id in the GET request.
    """
    # pylint: disable=unused-variable
    timestamp = datetime.utcnow()
    batch_size = 300

    dao = BigQuerySyncDao(backup=True)
    with dao.session() as session:
        total_rows = session.query(func.count(Participant.participantId)).first()[0]
        count = int(math.ceil(float(total_rows) / float(batch_size)))
        logging.info('Calculated {0} tasks from {1} records and a batch size of {2}.'.
                     format(count, total_rows, batch_size))

        while count > 0:
            task = rebuild_bq_participant_task.apply_async(queue='offline', args=(timestamp, batch_size))
            task.forget()
            count -= 1
    #
    # Process tables that don't need to be broken up into smaller tasks.
    #
    # Code Table
    bq_codebook_update_task()
    # HPO Table
    bq_hpo_update()
    # Organization Table
    bq_organization_update()
    # Site Table
    bq_site_update()


@celery.task()
def rebuild_bq_participant_task(timestamp, limit=0):
    """
    Loop through all participants and generate the BQ participant summary data and
    store it in the biguqery_sync table.
    Warning: this will force a rebuild and eventually a re-sync for every participant record.
    :param timestamp: datetime: to be used to rebuild any records old than this.
    :param limit: integer: 0 = all, otherwise only process records until limit has been reached.
    """
    if not limit or not isinstance(limit, int) or limit < 0:
        raise ValueError('invalid limit value.')

    if not timestamp:
        timestamp = datetime.utcnow()

    # try:
    #   app_id = app_identity.get_application_id()
    # except AttributeError:
    #   app_id = 'localhost'
    ro_dao = BigQuerySyncDao(backup=True)
    ps_bqgen = BQParticipantSummaryGenerator()
    pdr_bqgen = BQPDRParticipantSummaryGenerator()
    mod_bqgen = BQPDRQuestionnaireResponseGenerator()
    count = 0

    with ro_dao.session() as ro_session:
        while limit:
            limit -= 1
            # Note: Since we are being run in parallel now, only grab one participant to rebuild.
            sq = ro_session.query(Participant.participantId, BigQuerySync.id, BigQuerySync.modified). \
                outerjoin(BigQuerySync, and_(
                BigQuerySync.pk_id == Participant.participantId,
                or_(BigQuerySync.tableId == 'participant_summary', BigQuerySync.tableId == 'pdr_participant'))).\
                subquery()
            query = ro_session.query(sq.c.participant_id.label('participantId')). \
                filter(or_(sq.c.id == None, sq.c.modified < timestamp)).order_by(sq.c.modified)
            query = query.limit(1)

            # sql = dao.query_to_text(query)
            results = query.all()


            for row in results:
                count += 1
                # All logic for generating a participant summary is here.
                rebuild_bq_participant(row.participantId, ro_dao=ro_dao, ro_session=ro_session, ps_bqgen=ps_bqgen,
                                       pdr_bqgen=pdr_bqgen)

                # Generate participant questionnaire module response data
                modules = (
                    BQPDRConsentPII,
                    BQPDRTheBasics,
                    BQPDRLifestyle,
                    BQPDROverallHealth,
                    BQPDREHRConsentPII,
                    BQPDRDVEHRSharing
                )
                for module in modules:
                    mod = module()
                    table, mod_bqrs = mod_bqgen.make_bqrecord(row.participantId, mod.get_schema().get_module_name())
                    if not table:
                        continue

                    w_dao = BigQuerySyncDao()
                    with w_dao.session() as w_session:
                        for mod_bqr in mod_bqrs:
                            mod_bqgen.save_bqrecord(
                                mod_bqr.questionnaire_response_id, mod_bqr, bqtable=table, w_dao=w_dao, w_session=w_session)

    logging.info('Rebuilt BigQuery data for {0} participants.'.format(count))


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
    ro_dao = BigQuerySyncDao(backup=True)
    # https://github.com/googleapis/google-api-python-client/issues/299
    # https://github.com/pior/appsecrets/issues/7
    bq = build('bigquery', 'v2', cache_discovery=False) if dryrun is False else None
    total_inserts = 0
    # Google says maximum of 500 in a batch. Pretty sure they are talking about log shipping, I believe
    # that one participant summary record is larger than their expected average log record size.
    batch_size = 250
    run_limit = (2 * 60) - 10  # Run for 110 seconds before exiting, so we don't have overlapping cron jobs.
    limit_reached = False
    start_ts = datetime.now()
    table_list = list()

    with ro_dao.session() as ro_session:
        tables = ro_session.query(BigQuerySync.projectId, BigQuerySync.datasetId, BigQuerySync.tableId). \
            distinct(BigQuerySync.projectId, BigQuerySync.datasetId, BigQuerySync.tableId).all()

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
