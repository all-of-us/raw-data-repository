import logging
from flask import request
from flask_restful import Resource

from werkzeug.exceptions import NotFound

from rdr_service.dao.bq_participant_summary_dao import bq_participant_summary_update_task
from rdr_service.api.data_gen_api import generate_samples_task
from rdr_service.dao.bq_questionnaire_dao import bq_questionnaire_update_task
from rdr_service.offline.sync_consent_files import cloudstorage_copy_objects_task
from rdr_service.app_util import task_auth_required
from rdr_service.offline.bigquery_sync import rebuild_bq_participant_task
from rdr_service.dao.bq_code_dao import rebuild_bq_codebook_task


def log_task_headers():
    """
    Log the task headers
    """
    msg = 'Task Information:\n'
    msg += f'Task name: {request.headers.get("X-Appengine-Taskname", "unknown")}\n'
    msg += f'Queue: {request.headers.get("X-Appengine-Queuename", "unknown")}\n'
    msg += f'Retry count: {request.headers.get("X-Appengine-Taskretrycount", "1")}\n'
    logging.info(msg)


class RebuildParticipantsBQTaskApi(Resource):
    """
    Cloud Task endpoint: Rebuild all participant records for BigQuery.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        rebuild_bq_participant_task(data)
        return '{"success": "true"}'


class BQRebuildOneParticipantTaskApi(Resource):
    """
    Cloud Task endpoint: Rebuild one participant record for BigQuery.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        p_id = int(data.get('p_id', 0))
        if not p_id:
            raise NotFound('Invalid participant id')
        logging.info(f'Rebuilding participant summary for P{p_id}.')
        bq_participant_summary_update_task(p_id)
        logging.info('Complete.')
        return '{"success": "true"}'


class RebuildCodebookBQTaskApi(Resource):
    """
    Cloud Task endpoint: Rebuild Codebook records for BigQuery.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        logging.info('Rebuilding Codebook.')
        rebuild_bq_codebook_task()
        logging.info('Complete.')
        return '{"success": "true"}'


class CopyCloudStorageObjectTaskApi(Resource):
    """
    Cloud Task endpoint: Copy cloud storage object.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        source = data.get('source', None)
        destination = data.get('destination', None)
        date_limit = data.get('date_limit', None)

        if not source or not destination:
            raise NotFound('Invalid cloud storage path: Copy {0} to {1}.'.format(source, destination))

        logging.info('Copying cloud object.')
        cloudstorage_copy_objects_task(source, destination, date_limit=date_limit)
        logging.info('Complete.')
        return '{"success": "true"}'


class BQRebuildQuestionnaireTaskApi(Resource):
    """
    Cloud Task endpoint: Rebuild questionnaire response for BigQuery.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        p_id = int(data.get('p_id', 0))
        qr_id = int(data.get('qr_id', 0))
        if not p_id:
            raise NotFound('Invalid participant id')
        if not qr_id:
            raise NotFound('Invalid questionnaire response id.')

        logging.info(f'Rebuilding Questionnaire {qr_id} for P{p_id}.')
        bq_questionnaire_update_task(p_id, qr_id)
        logging.info('Complete')
        return '{"success": "true"}'


class GenerateBiobankSamplesTaskApi(Resource):
    """
    Cloud Task endpoint: Generate Biobank sample records.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        fraction = float(data.get('fraction', 0.0))
        logging.info('Generating Biobank sample record.')
        generate_samples_task(fraction)
        logging.info('Complete.')
        return '{"success": "true"}'
