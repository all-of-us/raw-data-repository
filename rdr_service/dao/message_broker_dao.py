from rdr_service import clock
from werkzeug.exceptions import BadRequest

from rdr_service.config import GAE_PROJECT
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.dao.database_utils import parse_datetime, format_datetime
from rdr_service.model.utils import from_client_participant_id, to_client_participant_id
from rdr_service.dao.base_dao import BaseDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.message_broker import MessageBrokerRecord, MessageBrokerEventData
from rdr_service.message_broker.message_broker import MessageBrokerFactory


class MessageBrokerDao(BaseDao):
    def __init__(self):
        super(MessageBrokerDao, self).__init__(MessageBrokerRecord)

    def from_client_json(self, resource_json, client_id=None):
        self._validate(resource_json)
        participant_id = from_client_participant_id(resource_json.get('participantId'))
        dest_name = self._get_message_dest_name(participant_id)
        message = MessageBrokerRecord(
            participantId=participant_id,
            eventType=resource_json.get('event'),
            messageOrigin=client_id,
            messageDest=dest_name,
            eventAuthoredTime=parse_datetime(resource_json.get('eventAuthoredTime')),
            requestTime=clock.CLOCK.now(),
            requestBody=resource_json.get('messageBody'),
            requestResource=resource_json
        )

        return message

    @staticmethod
    def _validate(resource_json):
        not_null_fields = ['event', 'eventAuthoredTime', 'participantId', 'messageBody']
        for field_name in not_null_fields:
            if resource_json.get(field_name) is None:
                raise BadRequest(f'{field_name} can not be NULL')

    @staticmethod
    def _get_message_dest_name(participant_id):
        p_dao = ParticipantDao()
        participant = p_dao.get(participant_id)
        if not participant:
            raise BadRequest(f'Participant not found with id {participant_id}')

        return participant.participantOrigin

    def insert(self, message):
        response_code, response_body, response_error = self.send_message(message)
        message.responseCode = response_code
        message.responseBody = response_body
        message.responseError = response_error
        message.responseTime = clock.CLOCK.now()
        super(MessageBrokerDao, self).insert(message)
        # store the data to RDR table asynchronous
        if GAE_PROJECT != 'localhost':
            payload = {
                'id': message.id,
                'eventType': message.eventType,
                'eventAuthoredTime': format_datetime(message.eventAuthoredTime),
                'participantId': message.participantId,
                'requestBody': message.requestBody
            }
            _task = GCPCloudTask()
            _task.execute('store_message_broker_event_data_task',
                          payload=payload,
                          queue='message-broker-tasks')

        return message

    def to_client_json(self, message):
        response_json = {
            "event": message.eventType,
            "participantId": to_client_participant_id(message.participantId),
            "responseCode": message.responseCode,
            "responseBody": message.responseBody,
            "errorMessage": message.responseError
        }
        return response_json

    @staticmethod
    def send_message(message):
        message_broker = MessageBrokerFactory.create(message)
        return message_broker.send_request()


class MessageBrokenEventDataDao(BaseDao):
    def __init__(self):
        super(MessageBrokenEventDataDao, self).__init__(MessageBrokerEventData, order_by_ending=['id'])

    def to_client_json(self, model):
        pass

    def from_client_json(self):
        pass

    def get_informing_loop(self, message_record_id, loop_type):
        with self.session() as session:
            return session.query(
                MessageBrokerEventData
            ).filter(
                MessageBrokerEventData.messageRecordId == message_record_id,
                MessageBrokerEventData.eventType == loop_type
            ).all()

    def get_result_viewed(self, message_record_id):
        with self.session() as session:
            return session.query(
                MessageBrokerEventData
            ).filter(
                MessageBrokerEventData.messageRecordId == message_record_id,
                MessageBrokerEventData.eventType == 'result_viewed'
            ).all()

    def get_result_ready(self, message_record_id):
        with self.session() as session:
            return session.query(
                MessageBrokerEventData
            ).filter(
                MessageBrokerEventData.messageRecordId == message_record_id,
                MessageBrokerEventData.eventType == 'result_ready'
            ).all()

    def get_appointment_event(self, message_record_id):
        with self.session() as session:
            return session.query(
                MessageBrokerEventData
            ).filter(
                MessageBrokerEventData.messageRecordId == message_record_id,
                MessageBrokerEventData.eventType.contains('appointment_')
            ).all()

