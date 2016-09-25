"""The API definition file for the participants API.

This defines the APIs and the handlers for the APIs.
"""

import config
import datetime
import endpoints
import pprint
import uuid

import api_util
import data_access_object
import evaluation
import participant

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

participants_api = endpoints.api(
    name='participant',
    version='v1',
    allowed_client_ids=config.getSettingList(config.ALLOWED_CLIENT_ID),
    scopes=[endpoints.EMAIL_SCOPE])

# Note that auth_level is missing.  This makes sure that the user is
# authenticated before the endpoint is called.  This is unnecessary as this
# check is insufficient, and we we are doing a string account whitelisting in
# check_auth().  Ideally we would turn it on anyway, but at the moment, it
# causes errors saying that this API is not enabled for the service account's
# project... And there is no way to enable the API. This API enabling should
# work fine once we upgrade to Cloud Endpoints 2.0.
@participants_api
class ParticipantApi(remote.Service):
  # Using a query_method would make this quite a bit simpler, but it allows the
  # client to query anything.  We need to enforce that last_name and
  # date_of_birth are set.
  @participant.Participant.method(
      request_fields=('first_name', 'last_name', 'date_of_birth'),
      response_message=participant.Participant.ProtoCollection(),
      user_required=True,
      path='participants',
      http_method='GET',
      name='participants.list')
  def list_participants(self, model):
    api_util.check_auth()

    # In order to do a query, at least the last name and the birthdate must be
    # specified.
    last_name = model.last_name
    date_of_birth = model.date_of_birth
    if (not last_name or not date_of_birth):
      raise endpoints.ForbiddenException(
          'Last name and date of birth must be specified.')
    return participant.list(model)

  @participant.Participant.method(
      user_required=True,
      path='participants',
      http_method='POST',
      name='participants.insert')
  def insert_participant(self, model):
    api_util.check_auth()

    model.drc_internal_id = str(uuid.uuid4())
    if not model.sign_up_time:
      model.sign_up_time = datetime.datetime.now()

    model.put()
    return model

  @participant.Participant.method(
      user_required=True,
      path='participants/{drc_internal_id}',
      http_method='PUT',
      name='participants.update')
  def update_participant(self, model):
    api_util.check_auth()
    return participant.update(model)

  @participant.Participant.method(
      request_message=participant.Participant.ProtoModel(),
      path='participants/{drc_internal_id}',
      http_method='GET',
      name='participants.get')
  def get_participant(self, model):
    api_util.check_auth()
    return participant.get(model.drc_internal_id)

  @endpoints.method(
      GET_EVALUATION_RESOURCE,
      # This method returns a ParticipantCollection message.
      evaluation.EvaluationCollection,
      path='participants/{participant_drc_id}/evaluations',
      http_method='GET',
      name='evaluations.list')
  def list_evaluations(self, request):
    api_util.check_auth()
    return evaluation.EvaluationCollection(items=evaluation.DAO.list(request))

  @endpoints.method(
      UPDATE_EVALUATION_RESOURCE,
      evaluation.Evaluation,
      path='participants/{participant_drc_id}/evaluations',
      http_method='POST',
      name='evaluations.insert')
  def insert_evaluation(self, request):
    api_util.check_auth()
    return evaluation.DAO.insert(request)

  @endpoints.method(
      UPDATE_EVALUATION_RESOURCE,
      evaluation.Evaluation,
      path='participants/{participant_drc_id}/evaluations/{evaluation_id}',
      http_method='PUT',
      name='evaluations.update')
  def update_evaluation(self, request):
    api_util.check_auth()
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
    api_util.check_auth()
    try:
      return evaluation.DAO.get(request)
    except (IndexError, data_access_object.NotFoundException):
      raise endpoints.NotFoundException(
          'Evaluation participant_drc_id: {} evaluation_id: not found'.format(
              request.participant_drc_id, request.evaluation_id))

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
