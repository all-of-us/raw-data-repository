"""The API definition file for the ppi API.

This defines the APIs and the handlers for the APIs.
"""

import base_api

import participant
import questionnaire
import questionnaire_response
import fhirclient.models.questionnaire
from werkzeug.exceptions import BadRequest

class QuestionnaireAPI(base_api.BaseApi):
  def __init__(self):
    super(QuestionnaireAPI, self).__init__(questionnaire.DAO)

  def validate_object(self, q, a_id=None):
    """Makes sure that the questionnaire is valid."""
    fhirclient.models.questionnaire.Questionnaire(q.resource)


class QuestionnaireResponseAPI(base_api.BaseApi):
  def __init__(self):
    super(QuestionnaireResponseAPI, self).__init__(questionnaire_response.DAO)

  def validate_object(self, q, a_id=None):
    """Makes sure that the questionnaire response has valid references."""
    model = fhirclient.models.questionnaireresponse.QuestionnaireResponse(
        q.resource)

    # The participant id must match a_id and be present.
    participant_id = model.subject.reference
    if (participant_id != 'Patient/{}'.format(a_id) or
        not participant.DAO.load_if_present(a_id)):
      raise BadRequest(
          'Participant id {} invalid or missing.'.format(participant_id))

    # The questionnaire ID must be valid and present in the datastore.
    questionnaire_id = model.questionnaire.reference
    if not questionnaire_id.startswith('Questionnaire/'):
      raise BadRequest(
          'Questionnaire id {} invalid or missing.'.format(questionnaire_id))
    questionnaire_id = questionnaire_id.replace('Questionnaire/', '', 1)
    if not questionnaire.DAO.load_if_present(questionnaire_id):
      raise BadRequest(
          'Questionnaire id {} invalid or missing.'.format(questionnaire_id))
