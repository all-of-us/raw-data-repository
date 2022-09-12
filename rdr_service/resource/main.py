"""The main API definition file for endpoints that trigger MapReduces and batch tasks."""

from flask import Flask, got_request_exception
from flask_restful import Api
from sqlalchemy.exc import DBAPIError

from rdr_service import app_util
from rdr_service.api import cloud_tasks_api, genomic_cloud_tasks_api, message_broker_cloud_tasks_api
from rdr_service.api.resource_api import ResourceRequestApi

from rdr_service.services.flask import RESOURCE_PREFIX, TASK_PREFIX, flask_start, flask_stop
from rdr_service.services.gcp_logging import begin_request_logging, end_request_logging, \
    flask_restful_log_exception_error


# noinspection PyPackageRequirements
def _build_resource_app():
    _app = Flask(__name__)
    _api = Api(_app)

    #
    # Cloud Task API endpoints
    #

    _api.add_resource(cloud_tasks_api.ArchiveRequestLogApi, TASK_PREFIX + 'MigrateRequestLog',
                      endpoint='archive_request_log', methods=['POST'])
    # Task Queue API endpoint to rebuild participant summary resources.
    _api.add_resource(cloud_tasks_api.RebuildParticipantsTaskApi, TASK_PREFIX + "RebuildParticipantsTaskApi",
                      endpoint="rebuild_participants_task", methods=["POST"])
    # Task Queue API endpoint to rebuild ONE participant resource.
    _api.add_resource(cloud_tasks_api.RebuildOneParticipantTaskApi, TASK_PREFIX + "RebuildOneParticipantTaskApi",
                      endpoint="rebuild_one_participant_task", methods=["POST"])
    # Task Queue API endpoing to rebuild codebook resources.
    _api.add_resource(cloud_tasks_api.RebuildCodebookTaskApi, TASK_PREFIX + "RebuildCodebookTaskApi",
                      endpoint="rebuild_codebook_task", methods=["POST"])
    _api.add_resource(cloud_tasks_api.BQRebuildQuestionnaireTaskApi, TASK_PREFIX + "RebuildQuestionnaireTaskApi",
                      endpoint="rebuild_questionnaire_task", methods=["POST"])

    _api.add_resource(cloud_tasks_api.CopyCloudStorageObjectTaskApi, TASK_PREFIX + "CopyCloudStorageObjectTaskApi",
                      endpoint="copy_cloudstorage_object_task", methods=["POST"])

    _api.add_resource(cloud_tasks_api.GenerateBiobankSamplesTaskApi, TASK_PREFIX + "GenerateBiobankSamplesTaskApi",
                      endpoint="generate_bio_samples_task", methods=["POST"])

    _api.add_resource(cloud_tasks_api.RebuildResearchWorkbenchTableRecordsApi,
                      TASK_PREFIX + "RebuildResearchWorkbenchTableRecordsApi",
                      endpoint="rebuild_research_workbench_table_records_task", methods=["POST"])

    _api.add_resource(cloud_tasks_api.ImportRetentionEligibleFileTaskApi,
                      TASK_PREFIX + "ImportRetentionEligibleFileApi",
                      endpoint="import_retention_eligible_file_task", methods=["POST"])

    _api.add_resource(cloud_tasks_api.RebuildRetentionEligibleMetricsApi,
                      TASK_PREFIX + "RebuildRetentionEligibleMetricsApi",
                      endpoint="batch_rebuild_retention_eligible_task", methods=["POST"])

    _api.add_resource(cloud_tasks_api.PtscHealthDataTransferValidTaskApi,
                      TASK_PREFIX + "PtscHealthDataTransferValidTaskApi",
                      endpoint="ptsc_health_data_transfer_valid_task", methods=["POST"])

    # Store message broker event data
    _api.add_resource(message_broker_cloud_tasks_api.StoreMessageBrokerEventDataTaskApi,
                      TASK_PREFIX + "StoreMessageBrokerEventDataTaskApi",
                      endpoint="store_message_broker_event_data_task", methods=["POST"])

    # Consent Metrics / Error reporting Cloud Task API endpoints
    _api.add_resource(cloud_tasks_api.RebuildConsentMetricApi,
                      TASK_PREFIX + "RebuildConsentMetricApi",
                      endpoint="batch_rebuild_consent_metrics_task", methods=["POST"])

    _api.add_resource(cloud_tasks_api.CheckConsentErrorsApi,
                      TASK_PREFIX + "CheckConsentErrorsApi",
                      endpoint="check_consent_errors_task", methods=["POST"])

    # Color User Event Metrics Cloud Task API endpoint
    _api.add_resource(cloud_tasks_api.RebuildUserEventMetricsApi,
                      TASK_PREFIX + "RebuildUserEventMetricsApi",
                      endpoint="batch_rebuild_user_event_metrics_task", methods=["POST"])

    #
    # Begin Genomic Cloud Task API Endpoints
    #

    _api.add_resource(genomic_cloud_tasks_api.RebuildGenomicTableRecordsApi,
                      TASK_PREFIX + "RebuildGenomicTableRecordsApi",
                      endpoint="rebuild_genomic_table_records_task", methods=["POST"])

    # Load AW1/AW2 raw manifest
    _api.add_resource(genomic_cloud_tasks_api.LoadRawAWNManifestDataAPI,
                      TASK_PREFIX + "LoadRawAWNManifestDataAPI",
                      endpoint="load_awn_raw_data_task", methods=["POST"])

    # Ingest AW1 manifest
    _api.add_resource(genomic_cloud_tasks_api.IngestAW1ManifestTaskApi,
                      TASK_PREFIX + "IngestAW1ManifestTaskApi",
                      endpoint="ingest_aw1_manifest_task", methods=["POST"])

    # Ingest AW2 manifest
    _api.add_resource(genomic_cloud_tasks_api.IngestAW2ManifestTaskApi,
                      TASK_PREFIX + "IngestAW2ManifestTaskApi",
                      endpoint="ingest_aw2_manifest_task", methods=["POST"])

    # Ingest AW4 manifest
    _api.add_resource(genomic_cloud_tasks_api.IngestAW4ManifestTaskApi,
                      TASK_PREFIX + "IngestAW4ManifestTaskApi",
                      endpoint="ingest_aw4_manifest_task", methods=["POST"])

    # Ingest AW5 manifest
    _api.add_resource(genomic_cloud_tasks_api.IngestAW5ManifestTaskApi,
                      TASK_PREFIX + "IngestAW5ManifestTaskApi",
                      endpoint="ingest_aw5_manifest_task", methods=["POST"])

    # Ingest W2SC manifest
    _api.add_resource(genomic_cloud_tasks_api.IngestCVLManifestTaskApi,
                      TASK_PREFIX + "IngestCVLManifestTaskApi",
                      endpoint="ingest_cvl_manifest_task", methods=["POST"])

    # Ingest member samples from raw models
    _api.add_resource(genomic_cloud_tasks_api.IngestSamplesFromRawTaskAPI,
                      TASK_PREFIX + "IngestSamplesFromRawTaskAPI",
                      endpoint="ingest_samples_from_raw_task", methods=["POST"])

    # Ingest user event metrics from files
    _api.add_resource(genomic_cloud_tasks_api.IngestUserEventMetricsApi,
                      TASK_PREFIX + "IngestUserEventMetricsApi",
                      endpoint="ingest_user_event_metrics", methods=["POST"])

    # Ingest member samples from raw models
    _api.add_resource(genomic_cloud_tasks_api.IngestDataFilesTaskApi,
                      TASK_PREFIX + "IngestDataFilesTaskApi",
                      endpoint="ingest_data_files_task", methods=["POST"])

    # Calculate manifest file record count
    _api.add_resource(genomic_cloud_tasks_api.CalculateRecordCountTaskApi,
                      TASK_PREFIX + "CalculateRecordCountTaskApi",
                      endpoint="calculate_record_count_task", methods=["POST"])
    # Calculate Contamination Category
    _api.add_resource(genomic_cloud_tasks_api.CalculateContaminationCategoryApi,
                      TASK_PREFIX + "CalculateContaminationCategoryApi",
                      endpoint="calculate_contamination_category_task", methods=["POST"])

    # Ingest Message Broker Data
    _api.add_resource(genomic_cloud_tasks_api.IngestGenomicMessageBrokerDataApi,
                      TASK_PREFIX + "IngestGenomicMessageBrokerDataApi",
                      endpoint="ingest_genomic_message_broker_data_task", methods=["POST"])

    # Ingest Message Broker Data - appointments only
    _api.add_resource(genomic_cloud_tasks_api.IngestGenomicMessageBrokerAppointmentApi,
                      TASK_PREFIX + "IngestGenomicMessageBrokerAppointmentApi",
                      endpoint="ingest_genomic_message_broker_appointment_task", methods=["POST"])

    # Update Genomic Set Member Job Run
    _api.add_resource(genomic_cloud_tasks_api.GenomicSetMemberUpdateApi,
                      TASK_PREFIX + "GenomicSetMemberUpdateApi",
                      endpoint="genomic_set_member_update_task", methods=["POST"])

    #
    # End Genomic Cloud Task API endpoints
    #

    #
    # Primary Resource API endpoint
    #
    _api.add_resource(ResourceRequestApi, RESOURCE_PREFIX + "<path:path>",
                      endpoint="resource_request", methods=["GET", "POST"])
    #
    # End primary Resource API endpoint
    #

    _api.add_resource(cloud_tasks_api.ValidateDateOfBirthApi,
                      TASK_PREFIX + 'ValidateDateOfBirth',
                      endpoint='check_date_of_birth', methods=['POST'])

    _app.add_url_rule('/_ah/start', endpoint='start', view_func=flask_start, methods=["GET"])
    _app.add_url_rule('/_ah/stop', endpoint='stop', view_func=flask_stop, methods=["GET"])

    _app.before_request(begin_request_logging)  # Must be first before_request() call.
    _app.before_request(app_util.request_logging)

    _app.after_request(app_util.add_headers)
    _app.after_request(end_request_logging)  # Must be last after_request() call.

    _app.register_error_handler(DBAPIError, app_util.handle_database_disconnect)

    got_request_exception.connect(flask_restful_log_exception_error, _app)

    return _app


app = _build_resource_app()
