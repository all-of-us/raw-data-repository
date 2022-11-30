from datetime import date
import mock

from tests.helpers.unittest_base import BaseTestCase


class ProfileUpdateApiTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ProfileUpdateApiTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs):
        super(ProfileUpdateApiTest, self).setUp(*args, **kwargs)

        dao_patch = mock.patch('rdr_service.api.profile_update_api.ParticipantSummaryDao')
        dao_mock = dao_patch.start()
        self.update_mock = dao_mock.update_profile_data
        self.addCleanup(dao_patch.stop)

    def test_first_name_update(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'name': [{
                    'given': [
                        'Peter'
                    ]
                }]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            first_name='Peter'
        )

    def test_middle_name_update(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'name': [{
                    'given': [
                        'Peter',
                        'Joshua'
                    ]
                }]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            first_name='Peter',
            middle_name='Joshua'
        )

    def test_last_name_update(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'name': [{
                    'family': 'Bishop',
                }]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            last_name='Bishop'
        )

    def test_clearing_middle_name(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'name': [{
                    'given': [
                        'Peter',
                        ''
                    ]
                }]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            first_name='Peter',
            middle_name=None
        )

    def test_clearing_family_name(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'name': [{
                    'family': None
                }]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            last_name=None
        )

    def test_clearing_full_name(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'name': []
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            first_name=None,
            middle_name=None,
            last_name=None
        )

    def test_update_phone_number(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'telecom': [
                    {
                        'system': 'phone',
                        'value': '1234567890'
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            phone_number='1234567890'
        )

    def test_clear_phone_number(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'telecom': [
                    {
                        'system': 'phone',
                        'value': ''
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            phone_number=None
        )

    def test_update_email(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'telecom': [
                    {
                        'system': 'email',
                        'value': 'test@example.org'
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            email='test@example.org'
        )

    def test_clear_email(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'telecom': [
                    {
                        'system': 'email',
                        'value': None
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            email=None
        )

    def test_update_birthdate(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'birthDate': '2017-01-01'
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            birthdate=date(2017, 1, 1)
        )

    def test_clear_birthdate(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'birthDate': ''
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            birthdate=None
        )

    def test_update_address_line1(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'address': [
                    {
                        'line': [
                            '123 Main St.'
                        ]
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            address_line1='123 Main St.',
            address_line2=None
        )

    def test_clear_address_line1(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'address': [
                    {
                        'line': [
                            ''
                        ]
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            address_line1=None,
            address_line2=None
        )

    def test_update_address_line2(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'address': [
                    {
                        'line': [
                            '123 Main St.',
                            'Apt C'
                        ]
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            address_line1='123 Main St.',
            address_line2='Apt C'
        )

    def test_clear_address_line2(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'address': [
                    {
                        'line': [
                            '123 Main St.'
                        ]
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            address_line1='123 Main St.',
            address_line2=None
        )

    def test_update_address_city(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'address': [
                    {
                        'city': 'New Haven'
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            address_city='New Haven'
        )

    def test_clear_address_city(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'address': [
                    {
                        'city': ''
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            address_city=None
        )

    def test_update_address_state(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'address': [
                    {
                        'state': 'CA'
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            address_state='CA'
        )

    def test_clear_address_state(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'address': [
                    {
                        'state': None
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            address_state=None
        )

    def test_update_address_zip_code(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'address': [
                    {
                        'postalCode': '12345'
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            address_zip_code='12345'
        )

    def test_clear_address_zip_code(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'address': [
                    {
                        'postalCode': ''
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            address_zip_code=None
        )

    def test_update_language(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'communication': [
                    {
                        'preferred': True,
                        'language': {
                            'coding': [
                                {
                                    'code': 'es'
                                }
                            ]
                        }
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            preferred_language='es'
        )

    def test_clear_language(self):
        self.send_post(
            'Participant/ProfileUpdate',
            request_data={
                'id': 'P123123123',
                'communication': []
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            preferred_language=None
        )
