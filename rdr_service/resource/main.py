"""The main API definition file for endpoints that trigger MapReduces and batch tasks."""

from flask import Flask, got_request_exception
from flask_restful import Api
from sqlalchemy.exc import DBAPIError

from rdr_service import app_util
from rdr_service.api.cloud_tasks_api import RebuildParticipantsTaskApi, RebuildCodebookTaskApi, \
    CopyCloudStorageObjectTaskApi, BQRebuildQuestionnaireTaskApi, GenerateBiobankSamplesTaskApi, \
    RebuildOneParticipantTaskApi, IngestAW1ManifestTaskApi, RebuildGenomicTableRecordsApi, IngestAW2ManifestTaskApi, \
    CalculateContaminationCategoryApi, RebuildResearchWorkbenchTableRecordsApi, ImportRetentionEligibleFileTaskApi
from rdr_service.services.flask import RESOURCE_PREFIX, TASK_PREFIX, flask_start, flask_stop
from rdr_service.services.gcp_logging import begin_request_logging, end_request_logging, \
    flask_restful_log_exception_error
from rdr_service.api.resource_api import ResourceRequestApi


# noinspection PyPackageRequirements
def _build_resource_app():
    _app = Flask(__name__)
    _api = Api(_app)
    #
    # Cloud Task API endpoints
    #
    # Task Queue API endpoint to rebuild participant summary resources.
    _api.add_resource(RebuildParticipantsTaskApi, TASK_PREFIX + "RebuildParticipantsTaskApi",
                      endpoint="rebuild_participants_task", methods=["POST"])
    # Task Queue API endpoint to rebuild ONE participant resource.
    _api.add_resource(RebuildOneParticipantTaskApi, TASK_PREFIX + "RebuildOneParticipantTaskApi",
                      endpoint="rebuild_one_participant_task", methods=["POST"])
    # Task Queue API endpoing to rebuild codebook resources.
    _api.add_resource(RebuildCodebookTaskApi, TASK_PREFIX + "RebuildCodebookTaskApi",
                      endpoint="rebuild_codebook_task", methods=["POST"])
    _api.add_resource(BQRebuildQuestionnaireTaskApi, TASK_PREFIX + "RebuildQuestionnaireTaskApi",
                      endpoint="rebuild_questionnaire_task", methods=["POST"])

    _api.add_resource(CopyCloudStorageObjectTaskApi, TASK_PREFIX + "CopyCloudStorageObjectTaskApi",
                     endpoint="copy_cloudstorage_object_task", methods=["POST"])

    _api.add_resource(GenerateBiobankSamplesTaskApi, TASK_PREFIX + "GenerateBiobankSamplesTaskApi",
                     endpoint="generate_bio_samples_task", methods=["POST"])

    _api.add_resource(RebuildGenomicTableRecordsApi, TASK_PREFIX + "RebuildGenomicTableRecordsApi",
                      endpoint="rebuild_genomic_table_records_task", methods=["POST"])

    _api.add_resource(RebuildResearchWorkbenchTableRecordsApi, TASK_PREFIX + "RebuildResearchWorkbenchTableRecordsApi",
                      endpoint="rebuild_research_workbench_table_records_task", methods=["POST"])

    _api.add_resource(ImportRetentionEligibleFileTaskApi, TASK_PREFIX + "ImportRetentionEligibleFileApi",
                      endpoint="import_retention_eligible_file_task", methods=["POST"])

    #
    # Begin Genomic Cloud Task API Endpoints
    #

    # Ingest AW1 manifest
    _api.add_resource(IngestAW1ManifestTaskApi, TASK_PREFIX + "IngestAW1ManifestTaskApi",
                      endpoint="ingest_aw1_manifest_task", methods=["POST"])

    # Ingest AW2 manifest
    _api.add_resource(IngestAW2ManifestTaskApi, TASK_PREFIX + "IngestAW2ManifestTaskApi",
                      endpoint="ingest_aw2_manifest_task", methods=["POST"])

    # Calculate Contamination Category
    _api.add_resource(CalculateContaminationCategoryApi, TASK_PREFIX + "CalculateContaminationCategoryApi",
                      endpoint="calculate_contamination_category_task", methods=["POST"])

    #
    # End Task API endpoints
    #

    #
    # Primary Resource API endpoint
    #
    _api.add_resource(ResourceRequestApi, RESOURCE_PREFIX + "<path:path>",
                      endpoint="resource_request", methods=["GET"])
    #
    # End primary Resource API endpoint
    #

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
