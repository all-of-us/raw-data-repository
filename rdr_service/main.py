"""The main API definition file.

This defines the APIs and the handlers for the APIs. All responses are JSON.
"""
import logging
import os
import signal

# pylint: disable=unused-import
from flask import got_request_exception
from flask_restful import Api
from sqlalchemy.exc import DBAPIError
from werkzeug.exceptions import HTTPException

from rdr_service import config_api
from rdr_service import version_api
from rdr_service import app_util
from rdr_service.api import metrics_ehr_api
from rdr_service.api.awardee_api import AwardeeApi
from rdr_service.api.bigquery_participant_summary_api import BQParticipantSummaryApi
from rdr_service.api.cloud_tasks_api import RebuildParticipantsBQTaskApi, RebuildCodebookBQTaskApi, \
    CopyCloudStorageObjectTaskApi, BQRebuildQuestionnaireTaskApi, GenerateBiobankSamplesTaskApi, \
    BQRebuildOneParticipantTaskApi
from rdr_service.api.biobank_order_api import BiobankOrderApi
from rdr_service.api.check_ppi_data_api import check_ppi_data
from rdr_service.api.data_gen_api import DataGenApi, SpecDataGenApi
from rdr_service.api.dv_order_api import DvOrderApi
from rdr_service.api.import_codebook_api import import_codebook
from rdr_service.api.metric_sets_api import MetricSetsApi
from rdr_service.api.metrics_api import MetricsApi
from rdr_service.api.metrics_fields_api import MetricsFieldsApi
from rdr_service.api.participant_api import ParticipantApi
from rdr_service.api.participant_counts_over_time_api import ParticipantCountsOverTimeApi
from rdr_service.api.participant_summary_api import ParticipantSummaryApi, ParticipantSummaryModifiedApi
from rdr_service.api.patient_status import PatientStatusApi, PatientStatusHistoryApi
from rdr_service.api.physical_measurements_api import PhysicalMeasurementsApi, sync_physical_measurements
from rdr_service.api.public_metrics_api import PublicMetricsApi
from rdr_service.api.questionnaire_api import QuestionnaireApi
from rdr_service.api.questionnaire_response_api import ParticipantQuestionnaireAnswers, QuestionnaireResponseApi
from rdr_service.config import get_config, get_db_config

from rdr_service.services.flask import app, API_PREFIX, TASK_PREFIX, finalize_request_logging


def _warmup():
    # Load configurations into the cache.
    # Not called in AppEngine2????
    get_config()
    get_db_config()
    return '{ "success": "true" }'

def _start():
    get_config()
    get_db_config()
    return '{ "success": "true" }'

def _stop():
    pid_file = '/tmp/supervisord.pid'
    if os.path.exists(pid_file):
        try:
            pid = int(open(pid_file).read())
            if pid:
                os.kill(pid, signal.SIGTERM)
                logging.info('******** Shutting down, sent supervisor the termination signal. ********')
        except TypeError:
            logging.warning('******** Shutting down, supervisor pid file is invalid. ********')
            pass
    return '{ "success": "true" }'

def _log_request_exception(sender, exception, **extra):  # pylint: disable=unused-argument
    """Logs HTTPExceptions.

    flask_restful automatically returns exception messages for JSON endpoints, but forgoes logs
    for HTTPExceptions.
    """
    if isinstance(exception, HTTPException):
        # Log everything at error. This handles 400s which, since we have few/predefined clients,
        # we want to notice (and we don't see client-side logs); Stackdriver error reporting only
        # reports error logs with stack traces. (500s are logged with stacks by Flask automatically.)
        logging.error("%s: %s", exception, exception.description, exc_info=True)


got_request_exception.connect(_log_request_exception, app)


#
# The REST-ful resources that are the bulk of the API.
#

api = Api(app)

api.add_resource(
    ParticipantApi,
    API_PREFIX + "Participant/<participant_id:p_id>",
    API_PREFIX + "Participant",
    endpoint="participant",
    methods=["GET", "POST", "PUT"],
)

api.add_resource(
    ParticipantSummaryApi,
    API_PREFIX + "Participant/<participant_id:p_id>/Summary",
    API_PREFIX + "ParticipantSummary",
    endpoint="participant.summary",
    methods=["GET"],
)

# BigQuery version of Participant Summary API
api.add_resource(
    BQParticipantSummaryApi,
    API_PREFIX + "Participant/<participant_id:p_id>/TestSummary",
    endpoint="bq_participant.summary",
    methods=["GET"],
)

api.add_resource(
    ParticipantSummaryModifiedApi,
    API_PREFIX + "ParticipantSummary/Modified",
    endpoint="participant.summary.modified",
    methods=["GET"],
)

api.add_resource(
    PatientStatusApi,
    API_PREFIX + "PatientStatus/<participant_id:p_id>/Organization/<string:org_id>",
    endpoint="patient.status",
    methods=["GET", "POST", "PUT"],
)

api.add_resource(
    PatientStatusHistoryApi,
    API_PREFIX + "PatientStatus/<participant_id:p_id>/Organization/<string:org_id>/History",
    endpoint="patient.status.history",
    methods=["GET"],
)

api.add_resource(
    PhysicalMeasurementsApi,
    API_PREFIX + "Participant/<participant_id:p_id>/PhysicalMeasurements",
    API_PREFIX + "Participant/<participant_id:p_id>/PhysicalMeasurements/<string:id_>",
    endpoint="participant.physicalMeasurements",
    methods=["GET", "POST", "PATCH"],
)

#api.add_resource(MetricsApi, API_PREFIX + "Metrics", endpoint="metrics", methods=["POST"])

api.add_resource(
    ParticipantCountsOverTimeApi,
    API_PREFIX + "ParticipantCountsOverTime",
    endpoint="participant_counts_over_time",
    methods=["GET"],
)

# Returns fields in metrics configs. Used in dashboards.
api.add_resource(MetricsFieldsApi, API_PREFIX + "MetricsFields", endpoint="metrics_fields", methods=["GET"])

#api.add_resource(
#    MetricSetsApi,
#    API_PREFIX + "MetricSets",
#    API_PREFIX + "MetricSets/<string:ms_id>/Metrics",
#    endpoint="metric_sets",
#    methods=["GET"],
#)

# Used by participant_counts_over_time
api.add_resource(metrics_ehr_api.MetricsEhrApi, API_PREFIX + "MetricsEHR", endpoint="metrics_ehr", methods=["GET"])

api.add_resource(
    metrics_ehr_api.ParticipantEhrMetricsOverTimeApi,
    API_PREFIX + "MetricsEHR/ParticipantsOverTime",
    endpoint="metrics_ehr.participants_over_time",
    methods=["GET"],
)

api.add_resource(
    metrics_ehr_api.OrganizationMetricsApi,
    API_PREFIX + "MetricsEHR/Organizations",
    endpoint="metrics_ehr.sites",
    methods=["GET"],
)

api.add_resource(PublicMetricsApi, API_PREFIX + "PublicMetrics", endpoint="public_metrics", methods=["GET"])

api.add_resource(
    QuestionnaireApi,
    API_PREFIX + "Questionnaire",
    API_PREFIX + "Questionnaire/<string:id_>",
    endpoint="questionnaire",
    methods=["POST", "GET", "PUT"],
)

api.add_resource(
    QuestionnaireResponseApi,
    API_PREFIX + "Participant/<participant_id:p_id>/QuestionnaireResponse/<string:id_>",
    API_PREFIX + "Participant/<participant_id:p_id>/QuestionnaireResponse",
    endpoint="participant.questionnaire_response",
    methods=["POST", "GET"],
)

api.add_resource(
    ParticipantQuestionnaireAnswers,
    API_PREFIX + "Participant/<participant_id:p_id>/QuestionnaireAnswers/<string:module>",
    endpoint="participant.questionnaire_answers",
    methods=["GET"],
)

api.add_resource(
    BiobankOrderApi,
    API_PREFIX + "Participant/<participant_id:p_id>/BiobankOrder/<string:bo_id>",
    API_PREFIX + "Participant/<participant_id:p_id>/BiobankOrder",
    endpoint="participant.biobank_order",
    methods=["POST", "GET", "PUT", "PATCH"],
)

api.add_resource(
    DvOrderApi,
    API_PREFIX + "SupplyRequest/<string:bo_id>",
    API_PREFIX + "SupplyRequest",
    API_PREFIX + "SupplyDelivery",
    API_PREFIX + "SupplyDelivery/<string:bo_id>",
    endpoint="participant.dv_order",
    methods=["POST", "GET", "PUT"],
)

api.add_resource(AwardeeApi, API_PREFIX + "Awardee", API_PREFIX + "Awardee/<string:a_id>",
                        endpoint="awardee", methods=["GET"])

# Configuration API for admin use.  # note: temporarily disabled until decided
api.add_resource(
    config_api.ConfigApi,
    API_PREFIX + "Config",
    API_PREFIX + "Config/<string:key>",
    endpoint="config",
    methods=["GET", "POST", "PUT"],
)

# Version API for prober and release management use.
api.add_resource(version_api.VersionApi, "/", API_PREFIX, endpoint="version", methods=["GET"])  # Default behavior

# Data generator API used to load fake data into the database.
api.add_resource(DataGenApi, API_PREFIX + "DataGen", endpoint="datagen", methods=["POST", "PUT"])

#
# Cloud Tasks API endpoints
#
# Task Queue API endpoint to rebuild BQ participant summary records.
api.add_resource(RebuildParticipantsBQTaskApi, TASK_PREFIX + "BQRebuildParticipantsTaskApi",
                 endpoint="bq_rebuild_participants_task", methods=["GET"])
# Task Queue API endpoint to rebuild ONE participant id.
api.add_resource(BQRebuildOneParticipantTaskApi, TASK_PREFIX + "BQRebuildOneParticipantTaskApi",
                 endpoint="bq_rebuild_one_participant_task", methods=["GET"])
# Task Queue API endpoing to rebuild BQ codebook records.
api.add_resource(RebuildCodebookBQTaskApi, TASK_PREFIX + "BQRebuildCodebookTaskApi",
                 endpoint="bq_rebuild_codebook_task", methods=["GET"])

api.add_resource(CopyCloudStorageObjectTaskApi, TASK_PREFIX + "CopyCloudStorageObjectTaskApi",
                 endpoint="copy_cloudstorage_object_task", methods=["GET"])

api.add_resource(BQRebuildQuestionnaireTaskApi, TASK_PREFIX + "BQRebuildQuestionnaireTaskApi",
                 endpoint="bq_rebuild_questionnaire_task", methods=["GET"])

api.add_resource(GenerateBiobankSamplesTaskApi, TASK_PREFIX + "GenerateBiobankSamplesTaskApi",
                 endpoint="generate_bio_samples_task", methods=["GET"])


#
# Non-resource endpoints
#
api.add_resource(SpecDataGenApi, API_PREFIX + "SpecDataGen", endpoint="specdatagen", methods=["POST"])

app.add_url_rule(
    API_PREFIX + "PhysicalMeasurements/_history",
    endpoint="physicalMeasurementsSync",
    view_func=sync_physical_measurements,
    methods=["GET"],
)

app.add_url_rule(API_PREFIX + "CheckPpiData", endpoint="check_ppi_data", view_func=check_ppi_data, methods=["POST"])

app.add_url_rule(API_PREFIX + "ImportCodebook", endpoint="import_codebook", view_func=import_codebook,
                 methods=["POST"])


app.add_url_rule("/_ah/warmup", endpoint="warmup", view_func=_warmup, methods=["GET"])
app.add_url_rule("/_ah/start", endpoint="start", view_func=_start, methods=["GET"])
app.add_url_rule("/_ah/stop", endpoint="stop", view_func=_stop, methods=["GET"])

app.after_request(app_util.add_headers)
app.after_request(finalize_request_logging)
app.before_request(app_util.request_logging)
app.register_error_handler(DBAPIError, app_util.handle_database_disconnect)
