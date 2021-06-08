import http.client
import mock

from rdr_service import clock
from datetime import timedelta
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.message_broker.message_broker import MessageBrokerFactory
from rdr_service.model.message_broker import MessageBrokerRecord, MessageBrokerDestAuthInfo
from rdr_service.dao.message_broker_dest_auth_info_dao import MessageBrokerDestAuthInfoDao


class MessageBrokerApiTest(BaseTestCase):
    def setUp(self):
        super().setUp()

    def _create_auth_info_record(self, dest, token, expired_at):
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
    def test_send_valid_message(self, request_origin):
        request_origin.return_value = 'color'
        participant = self.data_generator.create_database_participant(participantOrigin='vibrent')
        request_json = {
            "event": "result_viewed",
            "eventAuthoredTime": "2021-05-19T21:05:41Z",
            "participantId": str(participant.participantId),
            "messageBody": {
                "result_type": "hdr_v1",
                "report_revision_number": 0
            }
        }
        result = self.send_post("MessageBroker", request_json)
        self.assertEqual(result, {'event': 'result_viewed',
                                  'participantId': participant.participantId,
                                  'responseCode': '200',
                                  'responseBody': {'result': 'mocked result'},
                                  'errorMessage': ''})

    def test_send_invalid_message(self):
        # request without participant id
        request_json = {
            "event": "result_viewed",
            "eventAuthoredTime": str(clock.CLOCK.now()),
            "messageBody": {
                "result_type": "hdr_v1",
                "report_revision_number": 0
            }
        }
        self.send_post("MessageBroker", request_json, expected_status=http.client.BAD_REQUEST)

        # participant not exist
        request_json = {
            "event": "result_viewed",
            "participantId": "111",
            "eventAuthoredTime": str(clock.CLOCK.now()),
            "messageBody": {
                "result_type": "hdr_v1",
                "report_revision_number": 0
            }
        }
        self.send_post("MessageBroker", request_json, expected_status=http.client.BAD_REQUEST)


class MockedTokenResponse(object):
    status_code = 200

    def json(self):
        return {
            'access_token': 'new_token',
            'expires_in': 600
        }
