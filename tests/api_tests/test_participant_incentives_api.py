
from rdr_service.dao.participant_incentives_dao import ParticipantIncentivesDao
from tests.helpers.unittest_base import BaseTestCase


class ParticipantIncentivesApiTest(BaseTestCase):
    def setUp(self):
        super(ParticipantIncentivesApiTest, self).setUp()
        self.incentive_dao = ParticipantIncentivesDao()

    def test_post_pid_validation(self):
        bad_pid = 121213232
        response = self.send_post(
            f"Participant/P{bad_pid}/Incentives",
            request_data={},
            expected_status=404
        )

        self.assertEqual(response.status_code, 404)
        message = response.json['message']
        self.assertEqual(message, f'Participant with ID {bad_pid} not found')

        response = self.send_put(
            f"Participant/P{bad_pid}/Incentives",
            request_data={},
            expected_status=404
        )

        self.assertEqual(response.status_code, 404)
        message = response.json['message']
        self.assertEqual(message, f'Participant with ID {bad_pid} not found')

    def test_response_with_missing_keys(self):
        participant = self.data_generator.create_database_participant_summary()

        data = {'createdBy': ''}

        response = self.send_post(
            f"Participant/P{participant.participantId}/Incentives",
            request_data=data,
            expected_status=400
        )

        self.assertEqual(response.status_code, 400)
        message = response.json['message']
        self.assertEqual(
            message,
            'Missing required key/values in request, required: '
            'createdBy,site,dateGiven,occurrence,incentiveType,amount'
        )

        response = self.send_put(
            f"Participant/P{participant.participantId}/Incentives",
            request_data=data,
            expected_status=400
        )

        self.assertEqual(response.status_code, 400)
        message = response.json['message']
        self.assertEqual(
            message,
            'Missing required key/values in request, required: '
            'createdBy,site,dateGiven,occurrence,incentiveType,amount'
        )

        data = {
            'cancel': 'True'
        }

        response = self.send_put(
            f"Participant/P{participant.participantId}/Incentives",
            request_data=data,
            expected_status=400
        )

        self.assertEqual(response.status_code, 400)
        message = response.json['message']
        self.assertEqual(
            message,
            'Missing required key/values in request, required: incentiveId,cancelledBy,cancelledDate'
        )
