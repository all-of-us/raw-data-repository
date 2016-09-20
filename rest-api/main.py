"""The main API definition file.

This defines the APIs and the handlers for the APIs.
"""

import config
import datetime
import data_access_object
import endpoints
import evaluation
import metrics
import participant
import questionnaire
import questionnaire_response
import uuid

from protorpc import message_types
from protorpc import messages
from protorpc import protojson
from protorpc import remote


# ResourceContainers are used to encapsulate a request body and URL
# parameters. This one is used to represent the participant ID for the
# participant_get method.
GET_PARTICIPANT_RESOURCE = endpoints.ResourceContainer(
    # The request body should be empty.
    message_types.VoidMessage,
    # Accept one URL parameter: a string named 'id'
    drc_internal_id=messages.StringField(1, variant=messages.Variant.STRING))

LIST_PARTICIPANT_RESOURCE = endpoints.ResourceContainer(
    message_types.VoidMessage,
    first_name=messages.StringField(1, variant=messages.Variant.STRING),
    last_name=messages.StringField(2, variant=messages.Variant.STRING),
    date_of_birth=messages.StringField(3, variant=messages.Variant.STRING))

UPDATE_PARTICIPANT_RESOURCE = endpoints.ResourceContainer(
    participant.Participant,
    # Accept one URL parameter: a string named 'id'
    drc_internal_id=messages.StringField(1, variant=messages.Variant.STRING))

GET_EVALUATION_RESOURCE = endpoints.ResourceContainer(
    # The request body should be empty.
    message_types.VoidMessage,
    evaluation_id=messages.StringField(1, variant=messages.Variant.STRING),
    participant_drc_id=messages.StringField(2, variant=messages.Variant.STRING))

LIST_EVALUATION_RESOURCE = endpoints.ResourceContainer(
    # The request body should be empty.
    message_types.VoidMessage,
    participant_drc_id=messages.StringField(1, variant=messages.Variant.STRING))

UPDATE_EVALUATION_RESOURCE = endpoints.ResourceContainer(
    evaluation.Evaluation,
    evaluation_id=messages.StringField(1, variant=messages.Variant.STRING),
    participant_drc_id=messages.StringField(2, variant=messages.Variant.STRING))

GET_QUESTIONNAIRE_RESOURCE = endpoints.ResourceContainer(
    message_types.VoidMessage,
    id=messages.StringField(1, variant=messages.Variant.STRING))

GET_QUESTIONNAIRE_RESPONSE_RESOURCE = endpoints.ResourceContainer(
    message_types.VoidMessage,
    id=messages.StringField(1, variant=messages.Variant.STRING))



@endpoints.api(name='participant',
               version='v1',
               allowed_client_ids=config.ALLOWED_CLIENT_IDS,
               scopes=[endpoints.EMAIL_SCOPE])
class ParticipantApi(remote.Service):
  @endpoints.method(
      metrics.MetricsRequest,
      metrics.MetricsResponse,
      path='metrics',
      http_method='POST',
      name='metrics.calculate')
  def get_metric(self, request):
    _check_auth()
    return metrics.SERVICE.get_metrics(request)

  @endpoints.method(
      LIST_PARTICIPANT_RESOURCE,
      participant.ParticipantCollection,
      path='participants',
      http_method='GET',
      name='participants.list')
  def list_participants(self, request):
    _check_auth()

    # In order to do a query, at least the last name and the birthdate must be
    # specified.
    first_name = getattr(request, 'first_name', None)
    last_name = getattr(request, 'last_name', None)
    date_of_birth = getattr(request, 'date_of_birth', None)
    if (not last_name  or not date_of_birth):
      raise endpoints.ForbiddenException(
          'Last name and date of birth must be specified.')
    request_obj = participant.Participant(
        first_name=first_name, last_name=last_name,
        date_of_birth=_parse_date(date_of_birth))

    return participant.ParticipantCollection(
        items=participant.DAO.list(request_obj))

  @endpoints.method(
      participant.Participant,
      participant.Participant,
      path='participants',
      http_method='POST',
      name='participants.insert')
  def insert_participant(self, request):
    _check_auth()
    request.drc_internal_id = str(uuid.uuid4())
    return participant.DAO.insert(request)

  @endpoints.method(
      UPDATE_PARTICIPANT_RESOURCE,
      participant.Participant,
      path='participants/{drc_internal_id}',
      http_method='PUT',
      name='participants.update')
  def update_participant(self, request):
    _check_auth()
    return participant.DAO.update(request)

  @endpoints.method(
      # Use the ResourceContainer defined above to accept an empty body
      # but an ID in the query string.
      GET_PARTICIPANT_RESOURCE,
      # This method returns a participant.
      participant.Participant,
      # The path defines the source of the URL parameter 'id'. If not
      # specified here, it would need to be in the query string.
      path='participants/{drc_internal_id}',
      http_method='GET',
      name='participants.get')
  def get_participant(self, request):
    _check_auth()
    try:
      # request.drc_internal_id is used to access the URL parameter.
      return participant.DAO.get(request)
    except (IndexError, data_access_object.NotFoundException):
      raise endpoints.NotFoundException('Participant {} not found'.format(
          request.drc_internal_id))

  @endpoints.method(
      GET_EVALUATION_RESOURCE,
      # This method returns a ParticipantCollection message.
      evaluation.EvaluationCollection,
      path='participants/{participant_drc_id}/evaluations',
      http_method='GET',
      name='evaluations.list')
  def list_evaluations(self, request):
    _check_auth()
    return evaluation.EvaluationCollection(items=evaluation.DAO.list(request))

  @endpoints.method(
      UPDATE_EVALUATION_RESOURCE,
      evaluation.Evaluation,
      path='participants/{participant_drc_id}/evaluations',
      http_method='POST',
      name='evaluations.insert')
  def insert_evaluation(self, request):
    _check_auth()
    return evaluation.DAO.insert(request)

  @endpoints.method(
      UPDATE_EVALUATION_RESOURCE,
      evaluation.Evaluation,
      path='participants/{participant_drc_id}/evaluations/{evaluation_id}',
      http_method='PUT',
      name='evaluations.update')
  def update_evaluation(self, request):
    _check_auth()
    try:
      return evaluation.DAO.update(request)
    except data_access_object.NotFoundException:
      raise endpoints.NotFoundException(
          'Evaluation participant_drc_id: {} evaluation_id: not found'.format(
              request.participant_drc_id, request.evaluation_id))

  @endpoints.method(
      # Use the ResourceContainer defined above to accept an empty body
      # but an ID in the query string.
      GET_EVALUATION_RESOURCE,
      # This method returns a evaluation.
      evaluation.Evaluation,
      # The path defines the source of the URL parameter 'id'. If not
      # specified here, it would need to be in the query string.
      path='participants/{participant_drc_id}/evaluations/{evaluation_id}',
      http_method='GET',
      name='evaluations.get')
  def get_evaluation(self, request):
    _check_auth()
    try:
      return evaluation.DAO.get(request)
    except (IndexError, data_access_object.NotFoundException):
      raise endpoints.NotFoundException(
          'Evaluation participant_drc_id: {} evaluation_id: not found'.format(
              request.participant_drc_id, request.evaluation_id))

  @endpoints.method(
      questionnaire.Questionnaire,
      questionnaire.Questionnaire,
      path='ppi/fhir/Questionnaire',
      http_method='POST',
      name='ppi.fhir.questionnaire.insert')
  def insert_questionnaire(self, request):
    _check_auth()
    if not getattr(request, 'id', None):
      request.id = str(uuid.uuid4())
    return questionnaire.DAO.insert(request, strip=True)

  @endpoints.method(
      GET_QUESTIONNAIRE_RESOURCE,
      questionnaire.Questionnaire,
      path='ppi/fhir/Questionnaire/{id}',
      http_method='GET',
      name='ppi.fhir.questionnaire.get')
  def get_questionnaire(self, request):
    _check_auth()
    try:
      return questionnaire.DAO.get(request, strip=True)
    except (IndexError, data_access_object.NotFoundException):
      raise endpoints.NotFoundException(
          'Questionnaire questionnaire_id: {} not found'.format(
              request.id, request.evaluation_id))

  @endpoints.method(
      questionnaire_response.QuestionnaireResponse,
      questionnaire_response.QuestionnaireResponse,
      path='ppi/fhir/QuestionnaireResponse',
      http_method='POST',
      name='ppi.fhir.questionnaire_response.insert')
  def insert_questionnaire_response(self, request):
    _check_auth()
    if not getattr(request, 'id', None):
      request.id = str(uuid.uuid4())
    return questionnaire_response.DAO.insert(request, strip=True)


  @endpoints.method(
      GET_QUESTIONNAIRE_RESPONSE_RESOURCE,
      questionnaire_response.QuestionnaireResponse,
      path='ppi/fhir/QuestionnaireResponse/{id}',
      http_method='GET',
      name='ppi.fhir.questionnaire_response.get')
  def get_questionnaire_response(self, request):
    _check_auth()
    try:
      return questionnaire_response.DAO.get(request, strip=True)
    except (IndexError, data_access_object.NotFoundException):
      raise endpoints.NotFoundException(
          'Questionnaire questionnaire_id: {} not found'.format(
              request.id, request.evaluation_id))


def _check_auth():
  current_user = endpoints.get_current_user()
  if current_user is None or current_user.email() not in config.ALLOWED_USERS:
    raise endpoints.UnauthorizedException('Forbidden.')


class DateHolder(messages.Message):
  date = message_types.DateTimeField(1)

def _parse_date(date_str, date_only=True):
  """Parses JSON dates.

  Dates that come in as query params are strings.  Use the proto converter so
  they get the same handling as the rest of the dates in the system.
  """
  json_str = '{{"date": "{}"}}'.format(date_str)
  holder = protojson.decode_message(DateHolder, json_str)

  date_obj = holder.date
  if date_only:
    if (date_obj != datetime.datetime.combine(date_obj.date(),
                                              datetime.datetime.min.time())):
      raise endpoints.BadRequestException('Date contains non zero time fields')
  return date_obj


api = endpoints.api_server([ParticipantApi])
