"""The main API definition file.

This defines the APIs and the handlers for the APIs. All responses are JSON.
"""
import logging

# pylint: disable=unused-import
import requests_toolbelt.adapters.appengine
from flask import Flask, got_request_exception
from flask_restful import Api
from sqlalchemy.exc import DBAPIError
from werkzeug.exceptions import HTTPException

from . import config_api
from . import version_api
from .json_encoder import RdrJsonEncoder
from rdr_service import app_util
from rdr_service.api import metrics_ehr_api
from rdr_service.api.awardee_api import AwardeeApi
from rdr_service.api.bigquery_participant_summary_api import BQParticipantSummaryApi
from rdr_service.api.bigquery_task_queue_api import BQRebuildTaskApi
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
from rdr_service.model.utils import ParticipantIdConverter

# Use the App Engine Requests adapter. This makes sure that Requests uses URLFetch.
requests_toolbelt.adapters.appengine.monkeypatch()

PREFIX = "/rdr/v1/"

app = Flask(__name__)
app.url_map.converters["participant_id"] = ParticipantIdConverter
app.config.setdefault("RESTFUL_JSON", {"cls": RdrJsonEncoder})


def _warmup():
    # Load configurations into the cache.
    get_config()
    get_db_config()
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
    PREFIX + "Participant/<participant_id:p_id>",
    PREFIX + "Participant",
    endpoint="participant",
    methods=["GET", "POST", "PUT"],
)

api.add_resource(
    ParticipantSummaryApi,
    PREFIX + "Participant/<participant_id:p_id>/Summary",
    PREFIX + "ParticipantSummary",
    endpoint="participant.summary",
    methods=["GET"],
)

# BigQuery version of Participant Summary API
api.add_resource(
    BQParticipantSummaryApi,
    PREFIX + "Participant/<participant_id:p_id>/TestSummary",
    endpoint="bq_participant.summary",
    methods=["GET"],
)

api.add_resource(
    ParticipantSummaryModifiedApi,
    PREFIX + "ParticipantSummary/Modified",
    endpoint="participant.summary.modified",
    methods=["GET"],
)

api.add_resource(
    PatientStatusApi,
    PREFIX + "PatientStatus/<participant_id:p_id>/Organization/<string:org_id>",
    endpoint="patient.status",
    methods=["GET", "POST", "PUT"],
)

api.add_resource(
    PatientStatusHistoryApi,
    PREFIX + "PatientStatus/<participant_id:p_id>/Organization/<string:org_id>/History",
    endpoint="patient.status.history",
    methods=["GET"],
)

api.add_resource(
    PhysicalMeasurementsApi,
    PREFIX + "Participant/<participant_id:p_id>/PhysicalMeasurements",
    PREFIX + "Participant/<participant_id:p_id>/PhysicalMeasurements/<string:id_>",
    endpoint="participant.physicalMeasurements",
    methods=["GET", "POST", "PATCH"],
)

api.add_resource(MetricsApi, PREFIX + "Metrics", endpoint="metrics", methods=["POST"])

api.add_resource(
    ParticipantCountsOverTimeApi,
    PREFIX + "ParticipantCountsOverTime",
    endpoint="participant_counts_over_time",
    methods=["GET"],
)

api.add_resource(MetricsFieldsApi, PREFIX + "MetricsFields", endpoint="metrics_fields", methods=["GET"])

api.add_resource(
    MetricSetsApi,
    PREFIX + "MetricSets",
    PREFIX + "MetricSets/<string:ms_id>/Metrics",
    endpoint="metric_sets",
    methods=["GET"],
)

api.add_resource(metrics_ehr_api.MetricsEhrApi, PREFIX + "MetricsEHR", endpoint="metrics_ehr", methods=["GET"])

api.add_resource(
    metrics_ehr_api.ParticipantEhrMetricsOverTimeApi,
    PREFIX + "MetricsEHR/ParticipantsOverTime",
    endpoint="metrics_ehr.participants_over_time",
    methods=["GET"],
)

api.add_resource(
    metrics_ehr_api.OrganizationMetricsApi,
    PREFIX + "MetricsEHR/Organizations",
    endpoint="metrics_ehr.sites",
    methods=["GET"],
)

api.add_resource(PublicMetricsApi, PREFIX + "PublicMetrics", endpoint="public_metrics", methods=["GET"])

api.add_resource(
    QuestionnaireApi,
    PREFIX + "Questionnaire",
    PREFIX + "Questionnaire/<string:id_>",
    endpoint="questionnaire",
    methods=["POST", "GET", "PUT"],
)

api.add_resource(
    QuestionnaireResponseApi,
    PREFIX + "Participant/<participant_id:p_id>/QuestionnaireResponse/<string:id_>",
    PREFIX + "Participant/<participant_id:p_id>/QuestionnaireResponse",
    endpoint="participant.questionnaire_response",
    methods=["POST", "GET"],
)

api.add_resource(
    ParticipantQuestionnaireAnswers,
    PREFIX + "Participant/<participant_id:p_id>/QuestionnaireAnswers/<string:module>",
    endpoint="participant.questionnaire_answers",
    methods=["GET"],
)

api.add_resource(
    BiobankOrderApi,
    PREFIX + "Participant/<participant_id:p_id>/BiobankOrder/<string:bo_id>",
    PREFIX + "Participant/<participant_id:p_id>/BiobankOrder",
    endpoint="participant.biobank_order",
    methods=["POST", "GET", "PUT", "PATCH"],
)

api.add_resource(
    DvOrderApi,
    PREFIX + "SupplyRequest/<string:bo_id>",
    PREFIX + "SupplyRequest",
    PREFIX + "SupplyDelivery",
    PREFIX + "SupplyDelivery/<string:bo_id>",
    endpoint="participant.dv_order",
    methods=["POST", "GET", "PUT"],
)

api.add_resource(AwardeeApi, PREFIX + "Awardee", PREFIX + "Awardee/<string:a_id>", endpoint="awardee", methods=["GET"])

# Configuration API for admin use.

api.add_resource(
    config_api.ConfigApi,
    PREFIX + "Config",
    PREFIX + "Config/<string:key>",
    endpoint="config",
    methods=["GET", "POST", "PUT"],
)

# Version API for prober and release management use.
api.add_resource(version_api.VersionApi, "/", PREFIX, endpoint="version", methods=["GET"])  # Default behavior

# Data generator API used to load fake data into the database.
api.add_resource(DataGenApi, PREFIX + "DataGen", endpoint="datagen", methods=["POST", "PUT"])


# Task Queue API endpoing to rebuild BQ participant summary records.
api.add_resource(BQRebuildTaskApi, PREFIX + "BQRebuildTaskApi", endpoint="bq_rebuilt_task", methods=["GET"])

#
# Non-resource endpoints
#

api.add_resource(SpecDataGenApi, PREFIX + "SpecDataGen", endpoint="specdatagen", methods=["POST"])

app.add_url_rule(
    PREFIX + "PhysicalMeasurements/_history",
    endpoint="physicalMeasurementsSync",
    view_func=sync_physical_measurements,
    methods=["GET"],
)

app.add_url_rule(PREFIX + "CheckPpiData", endpoint="check_ppi_data", view_func=check_ppi_data, methods=["POST"])

app.add_url_rule(PREFIX + "ImportCodebook", endpoint="import_codebook", view_func=import_codebook, methods=["POST"])

app.add_url_rule("/_ah/warmup", endpoint="warmup", view_func=_warmup, methods=["GET"])

app.after_request(app_util.add_headers)
app.before_request(app_util.request_logging)
app.register_error_handler(DBAPIError, app_util.handle_database_disconnect)
