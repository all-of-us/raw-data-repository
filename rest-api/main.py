import config
import datetime
import db
import endpoints
import participant

from protorpc import message_types
from protorpc import messages
from protorpc import remote

# ResourceContainers are used to encapsulate a request body and URL
# parameters. This one is used to represent the participant ID for the
# participant_get method.
GET_RESOURCE = endpoints.ResourceContainer(
    # The request body should be empty.
    message_types.VoidMessage,
    # Accept one URL parameter: a string named 'id'
    id=messages.StringField(1, variant=messages.Variant.STRING))

UPDATE_RESOURCE = endpoints.ResourceContainer(
    participant.Participant,
    # Accept one URL parameter: a string named 'id'
    id=messages.StringField(1, variant=messages.Variant.STRING))


@endpoints.api(name='participant',
               version='v1',
               allowed_client_ids=config.ALLOWED_CLIENT_IDS,
               scopes=[endpoints.EMAIL_SCOPE])
class ParticipantApi(remote.Service):

  @endpoints.method(
      # This method does not take a request message.
      message_types.VoidMessage,
      # This method returns a ParticipantCollection message.
      participant.ParticipantCollection,
      path='participants',
      http_method='GET',
      name='participants.list')
  def list_participants(self, unused_request):
    return participant.ParticipantCollection(
        items=participant.ListParticipants())

  @endpoints.method(
      participant.Participant,
      participant.Participant,
      path='participants',
      http_method='POST',
      name='participants.insert')
  def insert_participant(self, request):
    return participant.InsertParticipant(request)

  @endpoints.method(
      UPDATE_RESOURCE,
      participant.Participant,
      path='participants/{id}',
      http_method='PUT',
      name='participants.update')
  def update_participant(self, request):
    return participant.UpdateParticipant(request)

  @endpoints.method(
      # Use the ResourceContainer defined above to accept an empty body
      # but an ID in the query string.
      GET_RESOURCE,
      # This method returns a participant.
      participant.Participant,
      # The path defines the source of the URL parameter 'id'. If not
      # specified here, it would need to be in the query string.
      path='participants/{id}',
      http_method='GET',
      name='participants.get')
  def get_participant(self, request):
    try:
      # request.id is used to access the URL parameter.
      return participant.GetParticipant(request.id)
    except IndexError:
      raise endpoints.NotFoundException('Participant {} not found'.format(
          request.id))

api = endpoints.api_server([ParticipantApi])
