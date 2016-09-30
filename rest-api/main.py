"""The main API definition file.

This defines the APIs and the handlers for the APIs.
"""
import flask
import metrics_api
import participants_api
import ppi_api

from flask import Flask, jsonify
from flask_restful import Api

from werkzeug.exceptions import default_exceptions


app = Flask(__name__)
api = Api(app)

api.add_resource(participants_api.ParticipantAPI,
                 '/participant/v1/participants/<string:id_>',
                 '/participant/v1/participants',
                 endpoint='participants',
                 methods=['GET', 'POST', 'PATCH'])

api.add_resource(
    participants_api.EvaluationAPI,
    '/participant/v1/participants/<string:a_id>/evaluation',
    '/participant/v1/participants/<string:a_id>/evaluation/<string:id_>',
    endpoint='evaluations',
    methods=['GET', 'POST',])

api.add_resource(ppi_api.QuestionnaireAPI,
                 '/ppi/fhir/Questionnaire',
                 '/ppi/fhir/Questionnaire/<string:id_>',
                 endpoint='ppi.fhir.questionnaire',
                 methods=['POST', 'GET'])

api.add_resource(ppi_api.QuestionnaireResponseAPI,
                 '/ppi/fhir/QuestionnaireResponse/<string:id_>',
                 '/ppi/fhir/QuestionnaireResponse',
                 endpoint='ppi.fhir.questionnaire_response',
                 methods=['POST', 'GET'])

api.add_resource(metrics_api.MetricsApi,
                 '/metrics/v1/metrics',
                 endpoint='metrics',
                 methods=['POST', 'GET'])
