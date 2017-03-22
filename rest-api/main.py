"""The main API definition file.

This defines the APIs and the handlers for the APIs. All responses are JSON.
"""
import app_util
import config_api
import version_api

from api.biobank_order_api import BiobankOrderApi
from api.metrics_api import MetricsApi
from api.metrics_fields_api import MetricsFieldsApi
from api.participant_api import ParticipantApi
from api.participant_summary_api import ParticipantSummaryApi
from api.physical_measurements_api import PhysicalMeasurementsApi, sync_physical_measurements
from api.questionnaire_api import QuestionnaireApi
from api.questionnaire_response_api import QuestionnaireResponseApi
from flask import Flask
from flask_restful import Api
from model.utils import ParticipantIdConverter

PREFIX = '/rdr/v1/'

app = Flask(__name__)
app.url_map.converters['participant_id'] = ParticipantIdConverter

#
# The REST-ful resources that are the bulk of the API.
#

api = Api(app)

api.add_resource(ParticipantApi,
                 PREFIX + 'Participant/<participant_id:p_id>',
                 PREFIX + 'Participant',
                 endpoint='participant',
                 # TODO(DA-216): remove PATCH once PTC migrates to PUT
                 methods=['GET', 'POST', 'PATCH', 'PUT'])

api.add_resource(ParticipantSummaryApi,
                 PREFIX + 'Participant/<participant_id:p_id>/Summary',
                 PREFIX + 'ParticipantSummary',
                 endpoint='participant.summary',
                 methods=['GET'])

api.add_resource(PhysicalMeasurementsApi,
                 PREFIX + 'Participant/<participant_id:p_id>/PhysicalMeasurements',
                 PREFIX + 'Participant/<participant_id:p_id>/PhysicalMeasurements/<string:id_>',
                 endpoint='participant.physicalMeasurements',
                 methods=['GET', 'POST'])

api.add_resource(MetricsApi,
                 PREFIX + 'Metrics',
                 endpoint='metrics',
                 methods=['POST'])

api.add_resource(MetricsFieldsApi,
                 PREFIX + 'MetricsFields',
                 endpoint='metrics_fields',
                 methods=['GET'])

api.add_resource(QuestionnaireApi,
                 PREFIX + 'Questionnaire',
                 PREFIX + 'Questionnaire/<string:id_>',
                 endpoint='questionnaire',
                 methods=['POST', 'GET', 'PUT'])

api.add_resource(QuestionnaireResponseApi,
                 PREFIX + 'Participant/<participant_id:p_id>/QuestionnaireResponse/<string:id_>',
                 PREFIX + 'Participant/<participant_id:p_id>/QuestionnaireResponse',
                 endpoint='participant.questionnaire_response',
                 methods=['POST', 'GET'])

api.add_resource(BiobankOrderApi,
                 PREFIX + 'Participant/<participant_id:p_id>/BiobankOrder/<string:bo_id>',
                 PREFIX + 'Participant/<participant_id:p_id>/BiobankOrder',
                 endpoint='participant.biobank_order',
                 methods=['POST', 'GET'])

# Configuration API for admin use.

api.add_resource(config_api.ConfigApi,
                 PREFIX + 'Config',
                 PREFIX + 'Config/<string:key>',
                 endpoint='config',
                 methods=['GET', 'POST', 'PUT'])

# Version API for prober and release management use.
api.add_resource(version_api.VersionApi,
                 '/',
                 PREFIX,  # Default behavior
                 endpoint='version',
                 methods=['GET'])

#
# Non-resource endpoints
#

app.add_url_rule(PREFIX + 'PhysicalMeasurements/_history',
                 endpoint='physicalMeasurementsSync',
                 view_func=sync_physical_measurements,
                 methods=['GET'])

app.after_request(app_util.add_headers)
app.before_request(app_util.request_logging)
