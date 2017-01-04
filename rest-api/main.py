"""The main API definition file.

This defines the APIs and the handlers for the APIs. All responses are JSON.
"""
import config_api
import biobank_orders_api
import biobank_samples_api
import logging
import metrics_api
import participants_api
import ppi_api
import version_api

from flask import Flask
from flask_restful import Api
from flask import request


PREFIX = '/rdr/v1/'

app = Flask(__name__)

#
# The REST-ful resources that are the bulk of the API.
#

api = Api(app)

api.add_resource(participants_api.ParticipantAPI,
                 PREFIX + 'Participant/<string:id_>',
                 PREFIX + 'Participant',
                 endpoint='participant',
                 methods=['GET', 'POST', 'PATCH'])

api.add_resource(
    participants_api.EvaluationAPI,
    PREFIX + 'Participant/<string:a_id>/PhysicalEvaluation',
    PREFIX + 'Participant/<string:a_id>/PhysicalEvaluation/<string:id_>',
    endpoint='participant.evaluation',
    methods=['GET', 'POST',])

api.add_resource(
    participants_api.ParticipantSummaryAPI,
    PREFIX + 'Participant/<string:id_>/Summary',
    PREFIX + 'ParticipantSummary',
    endpoint='participant.summary',
    methods=['GET',])

api.add_resource(metrics_api.MetricsAPI,
                 PREFIX + 'Metrics',
                 endpoint='metrics',
                 methods=['POST'])

api.add_resource(ppi_api.QuestionnaireAPI,
                 PREFIX + 'Questionnaire',
                 PREFIX + 'Questionnaire/<string:id_>',
                 endpoint='questionnaire',
                 methods=['POST', 'GET'])

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
                 methods=['GET', 'PUT'])

# Version API for prober and release management use.
api.add_resource(version_api.VersionApi,
                 '/',
                 PREFIX,  # Default behavior
                 endpoint='version',
                 methods=['GET'])

#
# Non-resource pipeline-trigger endpoints
#

app.add_url_rule(PREFIX + 'BiobankSamplesReload',
                 endpoint='biobankSamplesReload',
                 view_func=biobank_samples_api.get,
                 methods=['GET'])

app.add_url_rule(PREFIX + 'MetricsRecalculate',
                 endpoint='metrics_recalc',
                 view_func=metrics_api.get,
                 methods=['GET'])

# All responses are json, so we tag them as such at the app level to
# provide uniform protection against content-sniffing-based attacks.
def add_headers(response):
  response.headers['Content-Disposition'] = 'attachment'
  response.headers['X-Content-Type-Options'] = 'nosniff'
  response.headers['Content-Type'] = 'application/json'
  return response

app.after_request(add_headers)


# Some uniform logging of request characteristics before any checks are applied.
def request_logging():
  logging.info('Request protocol: HTTPS={}'.format(request.environ['HTTPS']))

app.before_request(request_logging)
