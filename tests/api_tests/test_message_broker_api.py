import http.client
import mock
from datetime import timedelta

from rdr_service import clock
from rdr_service.model.utils import to_client_participant_id
from rdr_service.dao.database_utils import format_datetime
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.message_broker.message_broker import MessageBrokerFactory
from rdr_service.model.message_broker import MessageBrokerRecord, MessageBrokerDestAuthInfo
from rdr_service.dao.message_broker_dest_auth_info_dao import MessageBrokerDestAuthInfoDao
from rdr_service.dao.message_broker_dao import MessageBrokerDao, MessageBrokenEventDataDao


class MockedTokenResponse(object):
    status_code = 200

    @staticmethod
    def json():
        return {
            'access_token': 'new_token',
            'expires_in': 600
        }


class MessageBrokerApiTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.record_dao = MessageBrokerDao()
        self.event_data_dao = MessageBrokenEventDataDao()

    @staticmethod
    def _create_auth_info_record(dest, token, expired_at):
        auth_info_record = MessageBrokerDestAuthInfo(
            destination=dest,
            accessToken=token,
            expiredAt=expired_at
        )
        auth_info_dao = MessageBrokerDestAuthInfoDao()
        auth_info_dao.insert(auth_info_record)

    def test_exist_token_not_expired(self):
        message = MessageBrokerRecord(messageDest='vibrent')
        message_broker = MessageBrokerFactory.create(message)
        # create a auth info record with token not expired
        now = clock.CLOCK.now()
        expired_at = now + timedelta(seconds=600)
        self._create_auth_info_record('vibrent', 'current_token', expired_at)

        token = message_broker.get_access_token()
        self.assertEqual(token, 'current_token')

    def test_exist_token_expired(self):
        requests_api_patcher = mock.patch(
            "rdr_service.message_broker.message_broker.requests",
            **{"post.return_value": MockedTokenResponse()}
        )
        requests_api_patcher.start()

        message = MessageBrokerRecord(messageDest='vibrent')
        message_broker = MessageBrokerFactory.create(message)

        # create a auth info record with expired token
        expired_at = clock.CLOCK.now()
        self._create_auth_info_record('vibrent', 'current_token', expired_at)

        token = message_broker.get_access_token()
        self.assertEqual(token, 'new_token')

        requests_api_patcher.stop()

    @mock.patch('rdr_service.dao.participant_dao.get_account_origin_id')
    @mock.patch('rdr_service.message_broker.message_broker.PtscMessageBroker.send_request')
    def test_send_valid_message(self, send_request, request_origin):
        send_request.return_value = 200, {'result': 'mocked result'}, ''
        request_origin.return_value = 'color'
        participant = self.data_generator.create_database_participant(participantOrigin='vibrent')
        request_json = {
            "event": "result_viewed",
            "eventAuthoredTime": "2021-05-19T21:05:41Z",
            "participantId": to_client_participant_id(participant.participantId),
            "messageBody": {
                "test_str": "str",
                "test_int": 0,
                "test_datatime": "2020-01-01T21:05:41Z",
                "test_bool": True,
                "test_json": {'name': 'value'}
            }
        }
        result = self.send_post("MessageBroker", request_json)
        self.assertEqual(result, {'event': 'result_viewed',
                                  'participantId': to_client_participant_id(participant.participantId),
                                  'responseCode': 200,
                                  'responseBody': {'result': 'mocked result'},
                                  'errorMessage': ''})

        # test cloud task API
        from rdr_service.resource import main as resource_main
        records = self.record_dao.get_all()
        record = records[0]
        payload = {
            'id': record.id,
            'eventType': record.eventType,
            'eventAuthoredTime': record.eventAuthoredTime.strftime("%Y-%m-%dT%H:%M:%SZ"),
            'participantId': record.participantId,
            'requestBody': record.requestBody
        }
        self.send_post(
            local_path='StoreMessageBrokerEventDataTaskApi',
            request_data=payload,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        event_data = self.event_data_dao.get_all()
        self.assertEqual(5, len(event_data))
        count = 0
        for item in event_data:
            if item.fieldName == 'test_bool':
                self.assertEqual(item.valueBool, True)
                count = count + 1
            if item.fieldName == 'test_json':
                self.assertEqual(item.valueJson, {'name': 'value'})
                count = count + 1
            if item.fieldName == 'test_str':
                self.assertEqual(item.valueString, 'str')
                count = count + 1
            if item.fieldName == 'test_datatime':
                self.assertEqual(item.valueDatetime.strftime("%Y-%m-%dT%H:%M:%SZ"), "2020-01-01T21:05:41Z")
                count = count + 1
            if item.fieldName == 'test_int':
                self.assertEqual(item.valueInteger, 0)
                count = count + 1
        self.assertEqual(count, 5)

    def test_send_invalid_message(self):
        # request without participant id
        request_json = {
            "event": "result_viewed",
            "eventAuthoredTime": format_datetime(clock.CLOCK.now()),
            "messageBody": {
                "result_type": "hdr_v1",
                "report_revision_number": 0
            }
        }
        self.send_post("MessageBroker", request_json, expected_status=http.client.BAD_REQUEST)

        # participant not exist
        request_json = {
            "event": "result_viewed",
            "participantId": "P111",
            "eventAuthoredTime": format_datetime(clock.CLOCK.now()),
            "messageBody": {
                "result_type": "hdr_v1",
                "report_revision_number": 0
            }
        }
        self.send_post("MessageBroker", request_json, expected_status=http.client.BAD_REQUEST)

    @mock.patch('rdr_service.dao.participant_dao.get_account_origin_id')
    @mock.patch('rdr_service.message_broker.message_broker.PtscMessageBroker.send_request')
    def test_informing_loop(self, send_request, request_origin):
        send_request.return_value = 200, {'result': 'mocked result'}, ''
        request_origin.return_value = 'color'

        participant_one = self.data_generator.create_database_participant(participantOrigin='vibrent')
        participant_two = self.data_generator.create_database_participant(participantOrigin='vibrent')

        loop_decision = 'informing_loop_decision'
        loop_started = 'informing_loop_started'

        from rdr_service.resource import main as resource_main

        request_json_decision = {
            "event": loop_decision,
            "eventAuthoredTime": format_datetime(clock.CLOCK.now()),
            "participantId": to_client_participant_id(participant_one.participantId),
            "messageBody": {
                'module_type': 'hdr',
                'decision_value': 'yes'
            }
        }

        self.send_post("MessageBroker", request_json_decision)

        records = self.record_dao.get_all()
        record = records[0]
        event_time = format_datetime(record.eventAuthoredTime)

        payload = {
            'id': record.id,
            'eventType': record.eventType,
            'eventAuthoredTime': event_time,
            'participantId': record.participantId,
            'requestBody': record.requestBody
        }

        self.send_post(
            local_path='StoreMessageBrokerEventDataTaskApi',
            request_data=payload,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        loop_decision_records = self.event_data_dao.get_informing_loop(
            record.id,
            loop_decision
        )

        self.assertIsNotNone(loop_decision_records)
        self.assertEqual(len(loop_decision_records), 2)

        for loop_record in loop_decision_records:
            self.assertIsNotNone(loop_record.valueString)
            self.assertEqual(format_datetime(loop_record.eventAuthoredTime), event_time)
            self.assertTrue(any(obj for obj in loop_decision_records if obj.valueString == 'hdr'))
            self.assertTrue(any(obj for obj in loop_decision_records if obj.valueString == 'yes'))

        request_json_started = {
            "event": loop_started,
            "eventAuthoredTime": format_datetime(clock.CLOCK.now()),
            "participantId": to_client_participant_id(participant_two.participantId),
            "messageBody": {
                'module_type': 'hdr',
            }
        }

        self.send_post("MessageBroker", request_json_started)

        records = self.record_dao.get_all()
        record = records[1]
        event_time = format_datetime(record.eventAuthoredTime)

        payload = {
            'id': record.id,
            'eventType': record.eventType,
            'eventAuthoredTime': event_time,
            'participantId': record.participantId,
            'requestBody': record.requestBody
        }

        self.send_post(
            local_path='StoreMessageBrokerEventDataTaskApi',
            request_data=payload,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        loop_started_records = self.event_data_dao.get_informing_loop(
            record.id,
            loop_started
        )

        self.assertIsNotNone(loop_started_records)
        self.assertEqual(len(loop_started_records), 1)

        for loop_record in loop_started_records:
            self.assertIsNotNone(loop_record.valueString)
            self.assertEqual(format_datetime(loop_record.eventAuthoredTime), event_time)
            self.assertTrue(any(obj for obj in loop_decision_records if obj.valueString == 'hdr'))

    @mock.patch('rdr_service.dao.participant_dao.get_account_origin_id')
    @mock.patch('rdr_service.message_broker.message_broker.PtscMessageBroker.send_request')
    def test_result_viewed(self, send_request, request_origin):
        send_request.return_value = 200, {'result': 'mocked result'}, ''
        request_origin.return_value = 'color'

        participant_one = self.data_generator.create_database_participant(participantOrigin='vibrent')
        event_type = 'result_viewed'

        from rdr_service.resource import main as resource_main

        # result_viewed events use result_type instead of module_type
        request_json_decision = {
            "event": event_type,
            "eventAuthoredTime": format_datetime(clock.CLOCK.now()),
            "participantId": to_client_participant_id(participant_one.participantId),
            "messageBody": {
                'result_type': 'gem',
            }
        }

        self.send_post("MessageBroker", request_json_decision)

        records = self.record_dao.get_all()
        record = records[0]
        event_time = format_datetime(record.eventAuthoredTime)

        payload = {
            'id': record.id,
            'eventType': record.eventType,
            'eventAuthoredTime': event_time,
            'participantId': record.participantId,
            'requestBody': record.requestBody
        }

        self.send_post(
            local_path='StoreMessageBrokerEventDataTaskApi',
            request_data=payload,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        result_viewed_records = self.event_data_dao.get_result_viewed(
            record.id
        )

        self.assertIsNotNone(result_viewed_records)
        self.assertEqual(len(result_viewed_records), 1)

        for result in result_viewed_records:
            self.assertIsNotNone(result.valueString)
            self.assertEqual(format_datetime(result.eventAuthoredTime), event_time)
            self.assertEqual(result.valueString, 'gem')
            self.assertEqual(result.fieldName, 'result_type')


