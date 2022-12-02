"""The main API definition file.

This defines the APIs and the handlers for the APIs. All responses are JSON.
"""

import logging

from flask import got_request_exception
from flask_restful import Api
from sqlalchemy.exc import DBAPIError
from werkzeug.exceptions import HTTPException

from rdr_service import app_util, config_api, version_api
from rdr_service.api import metrics_ehr_api
from rdr_service.api.awardee_api import AwardeeApi
from rdr_service.api.bigquery_participant_summary_api import BQParticipantSummaryApi
from rdr_service.api.biobank_order_api import BiobankOrderApi
from rdr_service.api.biobank_specimen_api import BiobankAliquotApi, BiobankAliquotDatasetApi,\
    BiobankAliquotDisposalApi, BiobankAliquotStatusApi, BiobankSpecimenApi, BiobankSpecimenAttributeApi,\
    BiobankSpecimenDisposalApi, BiobankSpecimenStatusApi

from rdr_service.api.check_ppi_data_api import check_ppi_data
from rdr_service.api.data_gen_api import DataGenApi, SpecDataGenApi
from rdr_service.api.deceased_report_api import DeceasedReportApi, DeceasedReportReviewApi
from rdr_service.api.mail_kit_order_api import MailKitOrderApi
from rdr_service.api.genomic_api import GenomicPiiApi, GenomicOutreachApi, GenomicOutreachApiV2, GenomicSchedulingApi
from rdr_service.api.import_codebook_api import import_codebook
from rdr_service.api.metrics_fields_api import MetricsFieldsApi
from rdr_service.api.participant_api import ParticipantApi, ParticipantResearchIdApi
from rdr_service.api.participant_incentives import ParticipantIncentivesApi
from rdr_service.api.participant_summary_api import ParticipantSummaryApi, \
    ParticipantSummaryModifiedApi, ParticipantSummaryCheckLoginApi
from rdr_service.api.patient_status import PatientStatusApi, PatientStatusHistoryApi
from rdr_service.api.physical_measurements_api import PhysicalMeasurementsApi, sync_physical_measurements
from rdr_service.api.public_metrics_api import PublicMetricsApi
from rdr_service.api.questionnaire_api import QuestionnaireApi
from rdr_service.api.questionnaire_response_api import ParticipantQuestionnaireAnswers, QuestionnaireResponseApi
from rdr_service.api.organization_hierarchy_api import OrganizationHierarchyApi
from rdr_service.api.workbench_api import WorkbenchWorkspaceApi, WorkbenchResearcherApi
from rdr_service.api.research_projects_directory_api import ResearchProjectsDirectoryApi
from rdr_service.api.redcap_workbench_audit_api import RedcapResearcherAuditApi, RedcapWorkbenchAuditApi
from rdr_service.api.message_broker_api import MessageBrokerApi
from rdr_service.api.onsite_verification_api import OnsiteVerificationApi
from rdr_service.api.nph_participant_api import nph_participant

from rdr_service.services.flask import app, API_PREFIX, flask_warmup, flask_start, flask_stop
from rdr_service.services.gcp_logging import begin_request_logging, end_request_logging, \
    flask_restful_log_exception_error


def _log_request_exception(sender, exception, **extra):  # pylint: disable=unused-argument
    """Logs HTTPExceptions.

    flask_restful automatically returns exception messages for JSON endpoints, but forgoes logs
    for HTTPExceptions.
    """
    if isinstance(exception, HTTPException):
        # Log everything at error. This handles 400s which, since we have few/predefined clients,
        # we want to notice (and we don't see client-side logs); Stackdriver error reporting only
        # reports error logs with stack traces. (500s are logged with stacks by Flask automatically.)
        logging.error(f"{exception}: {exception.description}", exc_info=True)


got_request_exception.connect(_log_request_exception, app)


#
# The REST-ful resources that are the bulk of the API.
#

api = Api(app)
app_util.install_rate_limiting(app)


api.add_resource(
    ParticipantApi,
    API_PREFIX + "Participant/<participant_id:p_id>",
    API_PREFIX + "Participant",
    endpoint="participant",
    methods=["GET", "POST", "PUT"],
)

api.add_resource(
    ParticipantResearchIdApi,
    API_PREFIX + "ParticipantId/ResearchId/Mapping",
    endpoint="participant.researchId",
    methods=["GET"],
)


api.add_resource(
    ParticipantSummaryApi,
    API_PREFIX + "Participant/<participant_id:p_id>/Summary",
    API_PREFIX + "ParticipantSummary",
    endpoint="participant.summary",
    methods=["GET", "POST"],
)

api.add_resource(
    ParticipantIncentivesApi,
    API_PREFIX + "Participant/<participant_id:p_id>/Incentives",
    endpoint="participant.incentives",
    methods=["POST", "PUT"],
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
    ParticipantSummaryCheckLoginApi,
    API_PREFIX + "ParticipantSummary/CheckLogin",
    endpoint="participant.summary.check_login",
    methods=["POST"],
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

# Returns fields in metrics configs. Used in dashboards.
api.add_resource(MetricsFieldsApi, API_PREFIX + "MetricsFields", endpoint="metrics_fields", methods=["GET"])

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
    API_PREFIX + "BiobankOrder",
    endpoint="participant.biobank_order",
    methods=["POST", "GET", "PUT", "PATCH"],
)

api.add_resource(
    MailKitOrderApi,
    API_PREFIX + "SupplyRequest/<string:bo_id>",
    API_PREFIX + "SupplyRequest",
    API_PREFIX + "SupplyDelivery",
    API_PREFIX + "SupplyDelivery/<string:bo_id>",
    endpoint="participant.mail_kit_order",  # previously dv_order
    methods=["POST", "GET", "PUT"],
)

api.add_resource(
    BiobankSpecimenApi,
    API_PREFIX + "Biobank/specimens/<string:rlims_id>",
    API_PREFIX + "Biobank/specimens",
    endpoint="biobank.parent",
    methods=["PUT"],
    )

api.add_resource(
    BiobankSpecimenStatusApi,
    API_PREFIX + "Biobank/specimens/<string:rlims_id>/status",
    endpoint="biobank.parent_status",
    methods=["PUT"],
    )

api.add_resource(
    BiobankSpecimenDisposalApi,
    API_PREFIX + "Biobank/specimens/<string:rlims_id>/disposalStatus",
    endpoint="biobank.parent_disposal",
    methods=["PUT"],
    )

api.add_resource(
    BiobankSpecimenAttributeApi,
    API_PREFIX + "Biobank/specimens/<string:rlims_id>/attributes/<string:attribute_name>",
    endpoint="biobank.parent_attribute",
    methods=["PUT", "DELETE"],
    )

api.add_resource(
    BiobankAliquotApi,
    API_PREFIX + "Biobank/specimens/<string:parent_rlims_id>/aliquots/<string:rlims_id>",
    endpoint="biobank.parent_aliquot",
    methods=["PUT"],
    )

api.add_resource(
    BiobankAliquotStatusApi,
    API_PREFIX + "Biobank/aliquots/<string:rlims_id>/status",
    endpoint="biobank.aliquot_status",
    methods=["PUT"],
    )

api.add_resource(
    BiobankAliquotDisposalApi,
    API_PREFIX + "Biobank/aliquots/<string:rlims_id>/disposalStatus",
    endpoint="biobank.aliquot_disposal",
    methods=["PUT"],
    )

api.add_resource(
    BiobankAliquotDatasetApi,
    API_PREFIX + "Biobank/aliquots/<string:rlims_id>/datasets/<string:dataset_rlims_id>",
    endpoint="biobank.aliquot_dataset",
    methods=["PUT"],
    )

api.add_resource(AwardeeApi, API_PREFIX + "Awardee", API_PREFIX + "Awardee/<string:a_id>",
                        endpoint="awardee", methods=["GET"])

api.add_resource(OrganizationHierarchyApi,
                 API_PREFIX + 'organization/hierarchy',
                 endpoint='hierarchy_content.organizations',
                 methods=['PUT'])

api.add_resource(WorkbenchResearcherApi,
                 API_PREFIX + 'workbench/directory/researchers',
                 endpoint='workbench.researchers',
                 methods=['POST'])

api.add_resource(WorkbenchWorkspaceApi,
                 API_PREFIX + 'workbench/directory/workspaces',
                 endpoint='workbench.workspaces',
                 methods=['POST'])

api.add_resource(ResearchProjectsDirectoryApi,
                 API_PREFIX + 'researchHub/projectDirectory',
                 endpoint='research.projects.directory',
                 methods=['GET'])

api.add_resource(RedcapWorkbenchAuditApi,
                 API_PREFIX + 'workbench/audit/workspace/snapshots',
                 API_PREFIX + 'workbench/audit/workspace/results',
                 endpoint='workbench.audit',
                 methods=['GET', 'POST'])

api.add_resource(RedcapResearcherAuditApi,
                 API_PREFIX + 'workbench/audit/researcher/snapshots',
                 endpoint='researchers.audit',
                 methods=['GET'])

api.add_resource(GenomicPiiApi,
                 API_PREFIX + "GenomicPII/<string:mode>/<string:pii_id>",
                 endpoint='genomic.pii',
                 methods=['GET'])

api.add_resource(GenomicOutreachApi,
                 API_PREFIX + "GenomicOutreach/<string:mode>",
                 API_PREFIX + "GenomicOutreach/<string:mode>/Participant/<participant_id:p_id>",
                 endpoint='genomic.outreach',
                 methods=['GET', 'POST'])

api.add_resource(GenomicOutreachApiV2,
                 API_PREFIX + "GenomicOutreachV2",
                 endpoint='genomic.outreachv2',
                 methods=['GET', 'POST', 'PUT'])

api.add_resource(GenomicSchedulingApi,
                 API_PREFIX + "GenomicScheduling",
                 endpoint='genomic.scheduling',
                 methods=['GET'])

api.add_resource(
    DeceasedReportApi,
    API_PREFIX + 'Participant/<string:participant_id>/Observation',
    endpoint='observation',
    methods=['POST']
)

api.add_resource(
    DeceasedReportApi,
    API_PREFIX + 'DeceasedReports',
    API_PREFIX + 'Participant/<string:participant_id>/DeceasedReport',
    endpoint='deceased_report.list',
    methods=['GET']
)

api.add_resource(
    DeceasedReportReviewApi,
    API_PREFIX + "Participant/<string:participant_id>/Observation/<string:report_id>/Review",
    endpoint='observation.review',
    methods=['POST']
)

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

# Message broker API
api.add_resource(MessageBrokerApi, API_PREFIX + "MessageBroker", endpoint="message_broker", methods=["POST"])

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

api.add_resource(
    OnsiteVerificationApi,
    API_PREFIX + "Onsite/Id/Verification",
    API_PREFIX + "Onsite/Id/Verification/<participant_id:p_id>",
    endpoint="onsite_id_verification",
    methods=["POST", "GET"],
)

app.add_url_rule("/_ah/warmup", endpoint="warmup", view_func=flask_warmup, methods=["GET"])
app.add_url_rule("/_ah/start", endpoint="start", view_func=flask_start, methods=["GET"])
app.add_url_rule("/_ah/stop", endpoint="stop", view_func=flask_stop, methods=["GET"])


app.add_url_rule(API_PREFIX + '/nph_participant', view_func=nph_participant, methods=["POST"])

app.before_request(begin_request_logging)  # Must be first before_request() call.
app.before_request(app_util.request_logging)
app.after_request(app_util.add_headers)
app.after_request(end_request_logging)  # Must be last after_request() call.

app.register_error_handler(DBAPIError, app_util.handle_database_disconnect)

# https://github.com/flask-restful/flask-restful/issues/792
got_request_exception.connect(flask_restful_log_exception_error, app)
