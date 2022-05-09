
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
            'Missing required key/values in request, required: site | dateGiven'
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
            'Missing required key/values in request, required: site | dateGiven'
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
            'Missing required key/values in request, required: incentiveId | cancelledBy | cancelledDate'
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

    def test_put_not_found_incentive(self):
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

        self.send_post(
            f"Participant/P{self.participant.participantId}/Incentives",
            request_data=data
        )

        incentives = self.incentive_dao.get_all()
        self.assertEqual(len(incentives), 1)
        self.assertEqual(incentives[0].id, 1)

        bad_incentive_id = 2
        data['incentiveId'] = bad_incentive_id

        response = self.send_put(
            f"Participant/P{self.participant.participantId}/Incentives",
            request_data=data,
            expected_status=400
        )

        self.assertEqual(response.status_code, 400)
        message = response.json['message']
        self.assertEqual(
            message,
            f'Incentive with id {bad_incentive_id} was not found'
        )

    def test_post_inserts_record_returns_obj(self):
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

        response_one = self.send_post(
            f"Participant/P{self.participant.participantId}/Incentives",
            request_data=data
        )

        incentives = self.incentive_dao.get_all()
        self.assertEqual(len(incentives), 1)

        incentive_created = self.incentive_dao.get(1)

        self.assertEqual(response_one['incentiveId'], incentive_created.id)
        self.assertEqual(response_one['createdBy'], test_user)
        self.assertEqual(response_one['participantId'], f'P{self.participant.participantId}')
        self.assertEqual(response_one['site'], site.googleGroup)
        self.assertEqual(response_one['incentiveType'], incentive_type)
        self.assertEqual(response_one['occurrence'], occurrence)
        self.assertEqual(response_one['amount'], amount)
        self.assertEqual(response_one['dateGiven'], date_given)

        self.assertEqual(response_one['giftcardType'], 'UNSET')
        self.assertEqual(response_one['notes'], 'UNSET')
        self.assertEqual(response_one['cancelled'], False)
        self.assertEqual(response_one['cancelledBy'], 'UNSET')
        self.assertEqual(response_one['cancelledDate'], 'UNSET')

        incentive_type = 'gift_card'
        giftcard_type = 'target'
        notes = 'This is an example note'

        data['incentiveType'] = incentive_type
        data['notes'] = notes
        data['giftcardType'] = giftcard_type

        response_two = self.send_post(
            f"Participant/P{self.participant.participantId}/Incentives",
            request_data=data
        )

        incentives = self.incentive_dao.get_all()
        self.assertEqual(len(incentives), 2)

        incentive_created = self.incentive_dao.get(2)

        self.assertEqual(response_two['incentiveId'], incentive_created.id)
        self.assertEqual(response_two['createdBy'], test_user)
        self.assertEqual(response_two['participantId'], f'P{self.participant.participantId}')
        self.assertEqual(response_two['site'], site.googleGroup)
        self.assertEqual(response_two['incentiveType'], incentive_type)
        self.assertEqual(response_two['occurrence'], occurrence)
        self.assertEqual(response_two['amount'], amount)
        self.assertEqual(response_two['dateGiven'], date_given)
        self.assertEqual(response_two['giftcardType'], giftcard_type)
        self.assertEqual(response_two['notes'], notes)

        self.assertEqual(response_two['cancelled'], False)
        self.assertEqual(response_two['cancelledBy'], 'UNSET')
        self.assertEqual(response_two['cancelledDate'], 'UNSET')

    def test_put_cancel_incentive(self):
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

        # create initial incentive
        response = self.send_post(
            f"Participant/P{self.participant.participantId}/Incentives",
            request_data=data
        )

        incentives = self.incentive_dao.get_all()
        self.assertEqual(len(incentives), 1)

        incentive_created = self.incentive_dao.get(1)

        self.assertEqual(response['incentiveId'], incentive_created.id)
        self.assertEqual(response['createdBy'], test_user)
        self.assertEqual(response['participantId'], f'P{self.participant.participantId}')
        self.assertEqual(response['site'], site.googleGroup)
        self.assertEqual(response['incentiveType'], incentive_type)
        self.assertEqual(response['occurrence'], occurrence)
        self.assertEqual(response['amount'], amount)
        self.assertEqual(response['dateGiven'], date_given)

        self.assertEqual(response['cancelled'], False)
        self.assertEqual(response['cancelledBy'], 'UNSET')
        self.assertEqual(response['cancelledDate'], 'UNSET')

        cancel_date = '2022-02-08 21:15:35'

        data = {
            'incentiveId': response['incentiveId'],
            'cancelledBy': test_user,
            'cancelledDate': cancel_date,
            'cancel': True
        }

        cancel_response = self.send_put(
            f"Participant/P{self.participant.participantId}/Incentives",
            request_data=data
        )

        incentives = self.incentive_dao.get_all()
        self.assertEqual(len(incentives), 1)

        incentive_cancelled = self.incentive_dao.get(1)

        self.assertEqual(cancel_response['incentiveId'], incentive_cancelled.id)
        self.assertEqual(cancel_response['createdBy'], test_user)
        self.assertEqual(cancel_response['participantId'], f'P{self.participant.participantId}')
        self.assertEqual(cancel_response['site'], site.googleGroup)
        self.assertEqual(cancel_response['incentiveType'], incentive_type)
        self.assertEqual(cancel_response['occurrence'], occurrence)
        self.assertEqual(cancel_response['amount'], amount)
        self.assertEqual(cancel_response['dateGiven'], date_given)

        self.assertEqual(cancel_response['cancelled'], True)
        self.assertEqual(cancel_response['cancelledBy'], test_user)
        self.assertEqual(cancel_response['cancelledDate'], cancel_date)

    def test_put_update_incentive(self):
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

        # create initial incentive
        response = self.send_post(
            f"Participant/P{self.participant.participantId}/Incentives",
            request_data=data
        )

        incentives = self.incentive_dao.get_all()
        self.assertEqual(len(incentives), 1)

        incentive_created = self.incentive_dao.get(1)

        self.assertEqual(response['incentiveId'], incentive_created.id)
        self.assertEqual(response['createdBy'], test_user)
        self.assertEqual(response['participantId'], f'P{self.participant.participantId}')
        self.assertEqual(response['site'], site.googleGroup)
        self.assertEqual(response['incentiveType'], incentive_type)
        self.assertEqual(response['occurrence'], occurrence)
        self.assertEqual(response['amount'], amount)
        self.assertEqual(response['dateGiven'], date_given)

        updated_test_user = 'Test User_1'
        updated_date_given = '2022-02-09 21:15:35'
        updated_occurrence = 'monthly'
        updated_incentive_type = 'giftcard'
        updated_amount = 30
        added_giftcard_type = 'target'
        added_notes = 'This is an example note'

        data = {
            'incentiveId': incentives[0].id,
            'createdBy': updated_test_user,
            'site': site.googleGroup,
            'dateGiven': updated_date_given,
            'occurrence': updated_occurrence,
            'incentiveType': updated_incentive_type,
            'amount': updated_amount,
            'giftcardType': added_giftcard_type,
            'notes': added_notes
        }

        updated_response = self.send_put(
            f"Participant/P{self.participant.participantId}/Incentives",
            request_data=data
        )

        incentives = self.incentive_dao.get_all()
        self.assertEqual(len(incentives), 1)

        incentive_updated = self.incentive_dao.get(1)

        # updated fields in PUT
        self.assertEqual(updated_response['incentiveId'], incentive_updated.id)
        self.assertEqual(updated_response['createdBy'], updated_test_user)
        self.assertEqual(updated_response['participantId'], f'P{self.participant.participantId}')
        self.assertEqual(updated_response['site'], site.googleGroup)
        self.assertEqual(updated_response['incentiveType'], updated_incentive_type)
        self.assertEqual(updated_response['occurrence'], updated_occurrence)
        self.assertEqual(updated_response['amount'], updated_amount)
        self.assertEqual(updated_response['dateGiven'], updated_date_given)

        # added fields in PUT
        self.assertEqual(updated_response['giftcardType'], added_giftcard_type)
        self.assertEqual(updated_response['notes'], added_notes)
