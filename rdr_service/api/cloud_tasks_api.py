import logging

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import NotFound

from rdr_service.api.data_gen_api import generate_samples_task
from rdr_service.api_util import parse_date, returns_success
from rdr_service.app_util import task_auth_required
from rdr_service.dao.bq_code_dao import rebuild_bq_codebook_task
from rdr_service.dao.bq_participant_summary_dao import bq_participant_summary_update_task
from rdr_service.dao.bq_questionnaire_dao import bq_questionnaire_update_task
from rdr_service.dao.bq_workbench_dao import bq_workspace_batch_update, bq_workspace_user_batch_update, \
    bq_institutional_affiliations_batch_update, bq_researcher_batch_update
from rdr_service.offline import retention_eligible_import
from rdr_service.offline.requests_log_migrator import RequestsLogMigrator
from rdr_service.offline.sync_consent_files import cloudstorage_copy_objects_task
from rdr_service.resource.generators.code import rebuild_codebook_resources_task
from rdr_service.resource.generators.participant import participant_summary_update_resource_task
from rdr_service.resource.generators.workbench import res_workspace_batch_update, res_workspace_user_batch_update, \
    res_institutional_affiliations_batch_update, res_researcher_batch_update
from rdr_service.resource.tasks import batch_rebuild_participants_task, batch_rebuild_retention_metrics_task, \
    batch_rebuild_consent_metrics_task, batch_rebuild_user_event_metrics_task, check_consent_errors_task
from rdr_service.services.participant_data_validation import ParticipantDataValidation
from rdr_service.services.slack_utils import SlackMessageHandler
from rdr_service import config
from rdr_service.config import RDR_SLACK_WEBHOOKS


def log_task_headers():
    """
    Log the task headers
    """
    msg = 'Task Information:\n'
    msg += f'Task name: {request.headers.get("X-Appengine-Taskname", "unknown")}\n'
    msg += f'Queue: {request.headers.get("X-Appengine-Queuename", "unknown")}\n'
    msg += f'Retry count: {request.headers.get("X-Appengine-Taskretrycount", "1")}\n'
    logging.info(msg)


class RebuildParticipantsTaskApi(Resource):
    """
    Cloud Task endpoint: Rebuild all participant records for Resource/BigQuery.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        batch_rebuild_participants_task(data)
        return '{"success": "true"}'


class RebuildOneParticipantTaskApi(Resource):
    """
    Cloud Task endpoint: Rebuild one participant record for Resource/BigQuery.
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
        participant_summary_update_resource_task(p_id)
        logging.info('Complete.')
        return '{"success": "true"}'


class RebuildCodebookTaskApi(Resource):
    """
    Cloud Task endpoint: Rebuild Codebook records for BigQuery.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        logging.info('Rebuilding Codebook.')
        rebuild_bq_codebook_task()
        rebuild_codebook_resources_task()
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
        file_filter = data.get('file_filter', 'pdf')

        if not source or not destination:
            raise NotFound('Invalid cloud storage path: Copy {0} to {1}.'.format(source, destination))

        logging.info('Copying cloud object.')
        cloudstorage_copy_objects_task(source, destination, date_limit=date_limit, file_filter=file_filter)
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


class RebuildResearchWorkbenchTableRecordsApi(Resource):
    """
    Cloud Task endpoint: Rebuild Research Workbench table records for Resource/BigQuery.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        table = data['table']
        batch = data['ids']

        logging.info(f'Rebuilding {len(batch)} records for table {table}.')

        if table == 'workspace':
            bq_workspace_batch_update(batch)
            res_workspace_batch_update(batch)
        elif table == 'workspace_user':
            bq_workspace_user_batch_update(batch)
            res_workspace_user_batch_update(batch)
        elif table == 'institutional_affiliations':
            bq_institutional_affiliations_batch_update(batch)
            res_institutional_affiliations_batch_update(batch)
        elif table == 'researcher':
            bq_researcher_batch_update(batch)
            res_researcher_batch_update(batch)

        logging.info(f'Rebuild complete.')
        return '{"success": "true"}'


class ArchiveRequestLogApi(Resource):
    """
    Cloud Task endpoint: Archive a request log
    """
    @task_auth_required
    def post(self):
        log_task_headers()

        data = request.get_json(force=True)
        log_id = data.get('log_id')

        RequestsLogMigrator.archive_log(log_id)
        return '{"success": "true"}'


class PtscHealthDataTransferValidTaskApi(Resource):
    """
    Cloud Task endpoint: Ptsc Health Data Transfer Result
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        logging.info(f'Ptsc Health Data Transfer Result: {data.get("attributes").get("eventType")}')
        # possible event types: TRANSFER_OPERATION_SUCCESS, TRANSFER_OPERATION_FAILED, TRANSFER_OPERATION_ABORTED
        event_type = data.get("attributes").get("eventType")
        if event_type in ['TRANSFER_OPERATION_FAILED', 'TRANSFER_OPERATION_ABORTED']:
            slack_config = config.getSettingJson(RDR_SLACK_WEBHOOKS, {})
            webhook_url = slack_config.get('rdr_ptsc_health_data_transfer_alerts')
            slack_alert_helper = SlackMessageHandler(webhook_url=webhook_url)
            logging.info('sending PTSC health data transfer error alert')
            message_data = {
                'text': f'PTSC health data transfer status: {event_type}, please check data transfer log for detail'}
            slack_alert_helper.send_message_to_webhook(message_data=message_data)

        logging.info('Complete.')
        return '{"success": "true"}'


class ImportRetentionEligibleFileTaskApi(Resource):
    """
    Cloud Task endpoint: Import Retention Eligible file
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        logging.info(f'Import Retention Eligible Metrics file: {data.get("filename")}')

        # Set up file/JSON
        task_data = {
            "bucket": data["bucket_name"],
            "upload_date": data["upload_date"],
            "file_path": data["file_path"]
        }

        retention_eligible_import.import_retention_eligible_metrics_file(task_data)

        logging.info('Complete.')
        return '{"success": "true"}'


class RebuildRetentionEligibleMetricsApi(Resource):
    """
    Cloud Task endpoint: Rebuild Retention Eligible Metrics records Resource records.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        batch_rebuild_retention_metrics_task(data)
        return '{"success": "true"}'


class RebuildConsentMetricApi(Resource):
    """
    Cloud Task endpoint: Rebuild Consent Validation metrics resource records
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        batch_rebuild_consent_metrics_task(data)
        return '{"success": "true"}'


class CheckConsentErrorsApi(Resource):
    """
    Cloud Task endpoint: Check for newly discovered consent validation errors and generate an error report
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        check_consent_errors_task(data)
        return '{"success": "true"}'


class RebuildUserEventMetricsApi(Resource):
    """
    Cloud Task endpoint: Rebuild Color User Event Metrics records Resource records.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        batch_rebuild_user_event_metrics_task(data)
        return '{"success": "true"}'


class ValidateDateOfBirthApi(Resource):
    @task_auth_required
    @returns_success
    def post(self):
        task_data = request.get_json(force=True)
        date_of_birth = parse_date(task_data['date_of_birth'])

        ParticipantDataValidation.analyze_date_of_birth(
            participant_id=task_data['participant_id'],
            date_of_birth=date_of_birth
        )
