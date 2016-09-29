"""The API definition file for the ppi API.

This defines the APIs and the handlers for the APIs.
"""

import base_api

import participant
import questionnaire
import questionnaire_response
import fhirclient.models.questionnaire
from werkzeug.exceptions import BadRequest

from google.appengine.ext import ndb
from flask.ext.restful import Resource


class QuestionnaireAPI(base_api.BaseApi):
  def __init__(self):
    super(QuestionnaireAPI, self).__init__(questionnaire.DAO)


class QuestionnaireResponseAPI(base_api.BaseApi):
  def __init__(self):
    super(QuestionnaireResponseAPI, self).__init__(questionnaire_response.DAO)

  def validate_object(self, q):
    """Makes sure that the questionnaire response is for a valid participant"""
    model = fhirclient.models.questionnaireresponse.QuestionnaireResponse(
        q.resource)
    participant_id = model.subject.reference
    parts = participant_id.split('Patient/')
    if len(parts) != 2 or parts[0]:
      raise BadRequest('Participant id {} invalid.'.format(participant_id))

    # This will raise if the participant can't be found.  Loading for validation
    # only.
    participant.DAO.load(parts[1])
