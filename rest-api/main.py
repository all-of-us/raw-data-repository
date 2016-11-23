"""The main API definition file.

This defines the APIs and the handlers for the APIs.
"""
import config_api
import biobank_orders_api
import biobank_samples_api
import logging
import metrics_api
import participants_api
import ppi_api

from flask import Flask
from flask_restful import Api
from flask import request


PREFIX = '/rdr/v1/'

app = Flask(__name__)

# The REST-ful resources that are the bulk of the API.

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
    endpoint='participant.summary',
    methods=['GET',])

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


# Some non-resource endpoints for triggering pipelines, and querying
# the metrics.

app.add_url_rule(PREFIX + 'BiobankSamplesReload',
                 endpoint='biobankSamplesReload',
                 view_func=biobank_samples_api.get,
                 methods=['GET'])

app.add_url_rule(PREFIX + 'MetricsRecalculate',
                 endpoint='metrics_recalc',
                 view_func=metrics_api.get,
                 methods=['GET'])

app.add_url_rule(PREFIX + 'Metrics',
                 endpoint='metrics',
                 view_func=metrics_api.post,
                 methods=['POST'])


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
