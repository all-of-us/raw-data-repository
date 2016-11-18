"""The API definition file for the ppi API.

This defines the APIs and the handlers for the APIs.
"""

import api_util
import base_api

import participant
import questionnaire
import questionnaire_response
import fhirclient.models.questionnaire
from api_util import PTC
from werkzeug.exceptions import BadRequest

class QuestionnaireAPI(base_api.BaseApi):
  def __init__(self):
    super(QuestionnaireAPI, self).__init__(questionnaire.DAO)

  @api_util.auth_required(PTC)
  def get(self, id_=None, a_id=None):
    return super(QuestionnaireAPI, self).get(id_, a_id)

  @api_util.auth_required(PTC)
  def post(self, a_id=None):
    return super(QuestionnaireAPI, self).post(a_id)

  @api_util.auth_required(PTC)
  def put(self, id_, a_id=None):
    return super(QuestionnaireAPI, self).put(id_, a_id)

  @api_util.auth_required(PTC)
  def patch(self, id_, a_id=None):
    return super(QuestionnaireAPI, self).patch(id_, a_id)

  @api_util.auth_required(PTC)
  def list(self, a_id=None):
    return super(QuestionnaireAPI, self).list(a_id)

  def validate_object(self, q, a_id=None):
    """Makes sure that the questionnaire is valid."""
    fhirclient.models.questionnaire.Questionnaire(q.resource)


class QuestionnaireResponseAPI(base_api.BaseApi):
  def __init__(self):
    super(QuestionnaireResponseAPI, self).__init__(questionnaire_response.DAO)

  @api_util.auth_required(PTC)
  def get(self, id_=None, a_id=None):
    return super(QuestionnaireResponseAPI, self).get(id_, a_id)

  @api_util.auth_required(PTC)
  def post(self, a_id=None):
    return super(QuestionnaireResponseAPI, self).post(a_id)

  @api_util.auth_required(PTC)
  def put(self, id_, a_id=None):
    return super(QuestionnaireResponseAPI, self).put(id_, a_id)

  @api_util.auth_required(PTC)
  def patch(self, id_, a_id=None):
    return super(QuestionnaireResponseAPI, self).patch(id_, a_id)

  @api_util.auth_required(PTC)
  def list(self, a_id=None):
    return super(QuestionnaireResponseAPI, self).list(a_id)

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
