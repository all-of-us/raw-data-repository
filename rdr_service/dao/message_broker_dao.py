from rdr_service import clock
from dateutil.parser import parse
from werkzeug.exceptions import BadRequest
from rdr_service.dao.base_dao import BaseDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.message_broker import MessageBrokerRecord, MessageBrokerMetadata


class MessageBrokerDao(BaseDao):
    def __init__(self):
        super(MessageBrokerDao, self).__init__(MessageBrokerRecord)
        self.message_metadata_dao = MessageBrokerMetadataDao()

    def from_client_json(self, resource_json, client_id=None):
        self._validate(resource_json)
        participant_id = int(resource_json.get('participantId'))
        dest_name = self._get_message_dest_name(participant_id)
        message = MessageBrokerRecord(
            participantId=participant_id,
            eventType=resource_json.get('event'),
            messageOrigin=client_id,
            messageDest=dest_name,
            eventAuthoredTime=parse(resource_json.get('eventAuthoredTime')),
            requestTime=clock.CLOCK.now(),
            requestBody=resource_json.get('messageBody'),
            requestResource=resource_json
        )

        return message

    def _validate(self, resource_json):
        not_null_fields = ['event', 'eventAuthoredTime', 'participantId', 'messageBody']
        for field_name in not_null_fields:
            if resource_json.get(field_name) is None:
                raise BadRequest(f'{field_name} can not be NULL')

        if not resource_json.get('participantId').isnumeric():
            raise BadRequest('Invalid participant ID')

    def _get_message_dest_name(self, participant_id):
        p_dao = ParticipantDao()
        participant = p_dao.get(participant_id)
        if not participant:
            raise BadRequest(f'Participant not found with id {participant_id}')

        return participant.participantOrigin

    def _get_message_dest_url(self, event, dest):
        dest_url = self.message_metadata_dao.get_dest_url(event, dest)
        return dest_url

    def insert_with_session(self, session, message):
        response_code, response_body, response_error = self._send_message_to_dest(message)
        message.responseCode = response_code
        message.responseBody = response_body
        message.responseError = response_error
        message.responseTime = clock.CLOCK.now()
        super(MessageBrokerDao, self).insert_with_session(session, message)
        # TODO - store data to RDR table

        return message

    def to_client_json(self, message):
        response_json = {
            "event": message.eventType,
            "participantId": message.participantId,
            "responseCode": message.responseCode,
            "responseBody": message.responseBody,
            "errorMessage": message.responseError
        }
        return response_json

    def _send_message_to_dest(self, message):
        dest_url = self._get_message_dest_url(message.eventType, message.messageDest)  # pylint: disable=unused-variable
        access_token = self._get_access_token(message)
        request_body = message.requestBody  # pylint: disable=unused-variable


        # PTSC test env is not ready for this implementation
        # TODO - get access to dest endpoint and sent message to dest
        # mock response from PTSC

        response_code = '200'
        response_body = {'result': 'mocked result'}
        response_error = ''
        return response_code, response_body, response_error

    def _get_access_token(self, message):
        # TODO
        # 1. PTSC will configure an OpenID connect client in their identity provider.
        # 2. Calls from DRC will use a token obtained using the client credentials grant,
        # used for machine to machine authentication/authorization.
        # 3. Token should be included in the HTTP Authorization header using the Bearer scheme.
        return ''


class MessageBrokerMetadataDao(BaseDao):
    def __init__(self):
        super(MessageBrokerMetadataDao, self).__init__(MessageBrokerMetadata)

    def get_dest_url(self, event, dest):
        with self.session() as session:
            query = session.query(MessageBrokerMetadata).filter(MessageBrokerMetadata.eventType == event,
                                                                MessageBrokerMetadata.destination == dest)
            return query.first()
