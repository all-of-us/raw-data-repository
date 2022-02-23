
from rdr_service.dao.participant_incentives_dao import ParticipantIncentivesDao
from rdr_service.dao.site_dao import SiteDao
from tests.helpers.unittest_base import BaseTestCase


class ParticipantIncentivesApiTest(BaseTestCase):
    def setUp(self):
        super(ParticipantIncentivesApiTest, self).setUp()
        self.incentive_dao = ParticipantIncentivesDao()
        self.site_dao = SiteDao()
        self.participant = self.data_generator.create_database_participant_summary()

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
        data = {'createdBy': ''}
        response = self.send_post(
            f"Participant/P{self.participant.participantId}/Incentives",
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
            f"Participant/P{self.participant.participantId}/Incentives",
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
            f"Participant/P{self.participant.participantId}/Incentives",
            request_data=data,
            expected_status=400
        )

        self.assertEqual(response.status_code, 400)
        message = response.json['message']
        self.assertEqual(
            message,
            'Missing required key/values in request, required: incentiveId,cancelledBy,cancelledDate'
        )

    def test_bad_site_in_payload(self):
        bad_group = 'bad-group'
        data = {
            'createdBy': 'Test User',
            'site': bad_group,
            'dateGiven': '2022-02-07 21:15:35',
            'occurrence': 'one_time',
            'incentiveType': 'cash',
            'amount': '25'
        }

        response = self.send_post(
            f"Participant/P{self.participant.participantId}/Incentives",
            request_data=data,
            expected_status=400
        )

        self.assertEqual(response.status_code, 400)
        message = response.json['message']
        self.assertEqual(
            message,
            f'Site for group {bad_group} is invalid'
        )

    def test_post_inserts_record_returns(self):
        participant = self.data_generator.create_database_participant_summary()
        site = self.site_dao.get(1)

        test_user = 'Test User'
        date_given = '2022-02-07 21:15:35'
        occurrence = 'one_time'
        incentive_type = 'cash'
        amount = 25

        data = {
            'createdBy': test_user,
            'site': site.googleGroup,
            'dateGiven': date_given,
            'occurrence': occurrence,
            'incentiveType': incentive_type,
            'amount': amount
        }

        response = self.send_post(
            f"Participant/P{participant.participantId}/Incentives",
            request_data=data
        )

        incentives = self.incentive_dao.get_all()
        self.assertEqual(len(incentives), 1)

        self.assertEqual(response['createdBy'], test_user)
        self.assertEqual(response['participantId'], f'P{participant.participantId}')
        self.assertEqual(response['site'], site.googleGroup)
        self.assertEqual(response['incentiveType'], incentive_type)
        self.assertEqual(response['occurrence'], occurrence)
        self.assertEqual(response['amount'], amount)
        self.assertEqual(response['dateGiven'], date_given)

        self.assertEqual(response['giftcardType'], 'UNSET')
        self.assertEqual(response['notes'], 'UNSET')
        self.assertEqual(response['cancelled'], False)
        self.assertEqual(response['cancelledBy'], 'UNSET')
        self.assertEqual(response['cancelledDate'], 'UNSET')

        incentive_type = 'gift_card'
        giftcard_type = 'target'
        notes = 'This is an example note'

        data['incentiveType'] = incentive_type
        data['notes'] = notes
        data['giftcardType'] = giftcard_type

        response = self.send_post(
            f"Participant/P{participant.participantId}/Incentives",
            request_data=data
        )

        incentives = self.incentive_dao.get_all()
        self.assertEqual(len(incentives), 2)

        self.assertEqual(response['createdBy'], test_user)
        self.assertEqual(response['participantId'], f'P{participant.participantId}')
        self.assertEqual(response['site'], site.googleGroup)
        self.assertEqual(response['incentiveType'], incentive_type)
        self.assertEqual(response['occurrence'], occurrence)
        self.assertEqual(response['amount'], amount)
        self.assertEqual(response['dateGiven'], date_given)
        self.assertEqual(response['giftcardType'], giftcard_type)
        self.assertEqual(response['notes'], notes)

        self.assertEqual(response['cancelled'], False)
        self.assertEqual(response['cancelledBy'], 'UNSET')
        self.assertEqual(response['cancelledDate'], 'UNSET')

