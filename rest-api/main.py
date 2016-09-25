"""The main API definition file.

This defines the APIs and the handlers for the APIs.
"""

import endpoints
import participants_api
import metrics_api
import ppi_api

from flask import Flask
from flask_restful import Resource, Api


api = endpoints.api_server([participants_api.participants_api,
                            metrics_api.metrics_api])

flask_app = Flask(__name__)
flask_api = Api(flask_app)

flask_api.add_resource(ppi_api.Questionnaire,
                       '/ppi/fhir/Questionnaire',
                       '/ppi/fhir/Questionnaire/<string:q_id>',
                       endpoint='ppi.fhir.questionnaire.insert')
flask_api.add_resource(ppi_api.QuestionnaireResponse,
                       '/ppi/fhir/QuestionnaireResponse',
                       '/ppi/fhir/QuestionnaireResponse/<string:q_id>',
                       endpoint='ppi.fhir.questionnaire_response.insert')
