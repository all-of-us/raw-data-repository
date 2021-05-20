import http.client

from rdr_service import clock
from tests.helpers.unittest_base import BaseTestCase


class MessageBrokerApiTest(BaseTestCase):
    def setUp(self):
        super().setUp()

    def test_send_valid_message(self):
        participant = self.data_generator.create_database_participant()
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
