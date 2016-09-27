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

api.add_resource(participants_api.ParticipantListAPI,
                 '/participant/v1/participants',
                 endpoint='participants.list',
                 methods=['GET'])
api.add_resource(participants_api.ParticipantInsertAPI,
                 '/participant/v1/participants',
                 endpoint='participants.insert',
                 methods=['POST'])
api.add_resource(participants_api.ParticipantGetAPI,
                 '/participant/v1/participants/<string:p_id>',
                 methods=['GET'],
                 endpoint='participants.get')
api.add_resource(participants_api.ParticipantUpdateAPI,
                 '/participant/v1/participants/<string:p_id>',
                 methods=['PUT'],
                 endpoint='participants.update')

api.add_resource(participants_api.EvaluationListAPI,
                 '/participant/v1/participants/<string:p_id>/evaluation',
                 endpoint='evaluations.list',
                 methods=['GET'])
api.add_resource(participants_api.EvaluationInsertAPI,
                 '/participant/v1/participants/<string:p_id>/evaluation',
                 endpoint='evaluations.insert',
                 methods=['POST'])
api.add_resource(
    participants_api.EvaluationGetAPI,
    '/participant/v1/participants/<string:p_id>/evaluation/<string:e_id>',
    methods=['GET'],
    endpoint='evaluations.get')
api.add_resource(
    participants_api.EvaluationUpdateAPI,
    '/participant/v1/participants/<string:p_id>/evaluation/<string:e_id>',
    methods=['PUT'],
    endpoint='evaluations.update')

api.add_resource(ppi_api.QuestionnaireInsertAPI,
                 '/ppi/fhir/Questionnaire',
                 endpoint='ppi.fhir.questionnaire.insert',
                 methods=['POST'])
api.add_resource(ppi_api.QuestionnaireGetAPI,
                 '/ppi/fhir/Questionnaire/<string:q_id>',
                 endpoint='ppi.fhir.questionnaire.get',
                 methods=['GET'])

api.add_resource(ppi_api.QuestionnaireResponseGetAPI,
                 '/ppi/fhir/QuestionnaireResponse/<string:q_id>',
                 endpoint='ppi.fhir.questionnaire_response.insert',
                 methods=['GET'])
api.add_resource(ppi_api.QuestionnaireResponseInsertAPI,
                 '/ppi/fhir/QuestionnaireResponse',
                 endpoint='ppi.fhir.questionnaire_response.get',
                 methods=['POST'])

api.add_resource(metrics_api.MetricsApi,
                 '/metrics/v1/metrics',
                 endpoint='metrics.calculate',
                 methods=['POST'])
