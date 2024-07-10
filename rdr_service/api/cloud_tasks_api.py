import logging

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import NotFound

from rdr_service.api.data_gen_api import generate_samples_task
from rdr_service.api_util import parse_date, returns_success
from rdr_service.app_util import task_auth_required
from rdr_service.dao.bq_code_dao import rebuild_bq_codebook_task
from rdr_service.dao.bq_hpo_dao import bq_hpo_update_all
from rdr_service.dao.bq_organization_dao import bq_organization_update_all
from rdr_service.dao.bq_site_dao import bq_site_update_all
from rdr_service.dao.bq_participant_summary_dao import bq_participant_summary_update_task
from rdr_service.dao.bq_questionnaire_dao import bq_questionnaire_update_task
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.retention_eligible_metrics_dao import RetentionEligibleMetricsDao
from rdr_service.offline import retention_eligible_import
from rdr_service.offline.requests_log_migrator import RequestsLogMigrator
from rdr_service.offline.sync_consent_files import cloudstorage_copy_objects_task
from rdr_service.resource.generators.code import rebuild_codebook_resources_task
from rdr_service.resource.generators.participant import participant_summary_update_resource_task
from rdr_service.resource.generators.onsite_id_verification import onsite_id_verification_build_task, \
    onsite_id_verification_batch_rebuild_task
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

class RebuildHpoAllTaskApi(Resource):
    """
     Cloud Task endpoint: Rebuild all the HPO records for PDR
     Triggered by resource tool, on resource-rebuild queue
     No payload expected
     """
    @task_auth_required
    def post(self):
        log_task_headers()
        logging.info('Rebuilding all HPO table records')
        bq_hpo_update_all()
        logging.info('Complete')
        return '{"success": "true"}'

class RebuildOrganizationAllTaskApi(Resource):
    """
     Cloud Task endpoint: Rebuild all the Organization records for PDR
     Triggered by resource tool, on resource-rebuild queue
     No payload expected
     """
    @task_auth_required
    def post(self):
        log_task_headers()
        logging.info('Rebuilding all Organization table records')
        bq_organization_update_all()
        logging.info('Complete')
        return '{"success": "true"}'

class RebuildSiteAllTaskApi(Resource):
    """
     Cloud Task endpoint: Rebuild all the Organization records for PDR
     Triggered by resource tool, on resource-rebuild queue
     No payload expected
     """
    @task_auth_required
    def post(self):
        log_task_headers()
        logging.info('Rebuilding all Site table records')
        bq_site_update_all()
        logging.info('Complete')
        return '{"success": "true"}'

class OnSiteIdVerificationBuildTaskApi(Resource):
    """
    Cloud Task endpoint:  Build a single OnSiteIdVerification resource record for PDR
    Triggered by RDR POST /Onsite/Id/Verification/, on resource-tasks queue
    Payload contains a single record id (primary key) to rebuild
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        ov_id = data.get('onsite_verification_id', 0)
        if not ov_id:
            raise NotFound('OnSiteIdVerification record id invalid')

        logging.info(f'Rebuilding onsite_id_verification record: {ov_id}')
        onsite_id_verification_build_task(ov_id)
        logging.info('Complete.')
        return '{"success": "true"}'

class OnSiteIdVerificationBatchRebuildTaskApi(Resource):
    """
    Cloud Task endpoint:  Rebuild a list of OnSiteIdVerification resource records for PDR
    Triggered by resource tool on resource-rebuild queue.
    Payload contains a list of integer ids (rec primary keys) to rebuild
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        ov_ids = data.get('onsite_verification_id_list', [])
        if not ov_ids:
            raise NotFound('OnSiteIdVerification record id list invalid')

        logging.info('Rebuilding onsite_id_verification records')
        logging.debug(f'Rebuild id list: {ov_ids}')
        onsite_id_verification_batch_rebuild_task(ov_ids)
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

        # PDR-2517 Don't expect to reach this endpoint after deleting RWB old pipeline build tasks.  Log a warning
        logging.warning(f'Resource/BigQuery builds for table {table} are disabled')
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
        if event_type == 'TRANSFER_OPERATION_ABORTED':
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
        validation_params = {
            'participant_id': task_data['participant_id'],
            'date_of_birth': date_of_birth
        }

        age_range_str = task_data.get('age_range')
        if age_range_str:
            age_min, age_max = age_range_str.split('_')
            validation_params.update({
                'age_min': int(age_min),
                'age_max': int(age_max)
            })

        ParticipantDataValidation.analyze_date_of_birth(**validation_params)


class UpdateEnrollmentStatus(Resource):
    @task_auth_required
    @returns_success
    def post(self):
        task_data = request.get_json(force=True)
        participant_id = task_data['participant_id']

        dao = ParticipantSummaryDao()
        with dao.session() as session:
            summary = ParticipantSummaryDao.get_for_update_with_linked_data(
                session=session,
                participant_id=participant_id
            )

            additional_parameters = {}
            for param_name in ('allow_downgrade', 'pdr_pubsub'):
                if param_name in task_data:
                    additional_parameters[param_name] = task_data[param_name]

            dao.update_enrollment_status(
                summary=summary,
                session=session,
                **additional_parameters
            )


class UpdateRetentionEligibleStatus(Resource):
    @task_auth_required
    @returns_success
    def post(self):

        # disable the retention calculation until ready
        if not config.getSettingJson('enable_retention_calc_task', default=False):
            return

        task_data = request.get_json(force=True)
        participant_id = task_data['participant_id']

        dao = ParticipantSummaryDao()
        with dao.session() as session:
            retention_data = retention_eligible_import.build_retention_data(
                participant_id=participant_id,
                session=session
            )
            if retention_data:
                RetentionEligibleMetricsDao.upsert_retention_data(
                    participant_id=participant_id,
                    retention_data=retention_data,
                    session=session
                )
