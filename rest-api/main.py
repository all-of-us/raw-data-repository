"""The main API definition file.

This defines the APIs and the handlers for the APIs. All responses are JSON.
"""
import app_util
import config_api
import biobank_orders_api
import metrics_api
import participant_summary_api
import physical_measurements_api
import ppi_api
import version_api

from api.participant_api import ParticipantApi
from api.questionnaire_api import QuestionnaireApi
from flask import Flask
from flask_restful import Api

PREFIX = '/rdr/v1/'

app = Flask(__name__)
#
# The REST-ful resources that are the bulk of the API.
#

api = Api(app)

api.add_resource(ParticipantApi,
                 PREFIX + 'Participant/<string:id_>',
                 PREFIX + 'Participant',
                 endpoint='participant',
                 # TODO(DA-216): remove PATCH once PTC migrates to PUT
                 methods=['GET', 'POST', 'PATCH', 'PUT'])

api.add_resource(participant_summary_api.ParticipantSummaryAPI,
                 PREFIX + 'Participant/<string:id_>/Summary',
                 PREFIX + 'ParticipantSummary',
                 endpoint='participant.summary',
                 methods=['GET',])

api.add_resource(physical_measurements_api.PhysicalMeasurementsAPI,
                 PREFIX + 'Participant/<string:a_id>/PhysicalMeasurements',
                 PREFIX + 'Participant/<string:a_id>/PhysicalMeasurements/<string:id_>',
                 endpoint='participant.physicalMeasurements',
                 methods=['GET', 'POST',])

api.add_resource(metrics_api.MetricsAPI,
                 PREFIX + 'Metrics',
                 endpoint='metrics',
                 methods=['POST'])

api.add_resource(metrics_api.MetricsFieldsAPI,
                 PREFIX + 'MetricsFields',
                 endpoint='metrics_fields',
                 methods=['GET'])

api.add_resource(QuestionnaireApi,
                 PREFIX + 'Questionnaire',
                 PREFIX + 'Questionnaire/<string:id_>',
                 endpoint='questionnaire',
                 methods=['POST', 'GET', 'PUT'])

api.add_resource(ppi_api.QuestionnaireResponseAPI,
                 PREFIX + 'Participant/<string:a_id>/QuestionnaireResponse/<string:id_>',
                 PREFIX + 'Participant/<string:a_id>/QuestionnaireResponse',
                 endpoint='participant.questionnaire_response',
                 methods=['POST', 'GET'])

api.add_resource(biobank_orders_api.BiobankOrderAPI,
                 PREFIX + 'Participant/<string:a_id>/BiobankOrder/<string:id_>',
                 PREFIX + 'Participant/<string:a_id>/BiobankOrder',
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
                 view_func=physical_measurements_api.sync_physical_measurements,
                 methods=['GET'])

app.after_request(app_util.add_headers)
app.before_request(app_util.request_logging)
