from datetime import date
import mock

from rdr_service.model.pediatric_data_log import PediatricDataType
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
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
            'Patient',
            request_data={
                'id': 'P123123123',
                'communication': []
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            preferred_language=None
        )

    @mock.patch('rdr_service.api.profile_update_api.ProfileUpdateRepository.store_update_json')
    def test_recording_update(self, store_update_mock):
        participant_id = 123123123
        update_json = {
            'id': f'P{participant_id}',
            'name': [{
                'given': [
                    'John'
                ]
            }]
        }
        self.send_post('Patient', request_data=update_json)
        store_update_mock.assert_called_with(
            participant_id=participant_id,
            json=update_json
        )

    @mock.patch(
        'rdr_service.services.ancillary_studies.study_enrollment.EnrollmentInterface.create_study_participant'
    )
    def test_nph_participant_id(self, study_mock):
        payload = {
            "id": "P123123123",
            "contained": [
                {
                    "resourceType": "Provenance",
                    "id": "PMIParticipantProvenanceExample",
                    "target": [
                        {
                            "reference": "Patient/P123123123"
                        }
                    ],
                    "recorded": "2015-02-07T13:28:17.239+02:00",
                    "agent": [
                        {
                            "who": {
                                "reference": "Patient/P123123123"
                            }
                        }
                    ]
                }
              ],
            "identifier": [
                {
                    "use": "official",
                    "type": {
                        "coding": [
                            {
                                "system": "https://pmi-fhir-ig.github.io/pmi-fhir-ig/CodeSystem/PMIIdentifierTypeCS",
                                "code": "NPH-1000"
                            }
                        ]
                    },
                    "value": "1000578448930"
                }
            ]
        }
        response = self.send_post(
            'Patient',
            request_data=payload
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
        )
        self.assertEqual(response, payload)

        study_mock.assert_called_with(
            aou_pid=123123123,
            ancillary_pid='1000578448930',
            event_authored_time="2015-02-07T13:28:17.239+02:00"
        )

    def test_setting_login_phone(self):
        self.send_post(
            'Patient',
            request_data={
                'id': 'P123123123',
                'telecom': [
                    {
                        'system': 'phone',
                        'value': '1234567890',
                        'extension': [
                            {
                                'url': 'https://pmi-fhir-ig.github.io/pmi-fhir-ig/StructureDefinition/pmi-verified',
                                'valueBoolean': True
                            }
                        ]
                    }
                ]
            }
        )
        self.update_mock.assert_called_with(
            participant_id=123123123,
            login_phone_number='1234567890'
        )

    @mock.patch('rdr_service.dao.pediatric_data_log_dao.PediatricDataLogDao.insert')
    def test_reading_pediatric_age_range(self, pediatric_data_insert_mock):
        participant_id = 123123123
        self.send_post(
            'Patient',
            request_data={
                'id': f'P{participant_id}',
                'extension': [
                    {
                        'url': 'https://pmi-fhir-ig.github.io/pmi-fhir-ig/StructureDefinition/child-account-type',
                        'valueCode': 'SIX_AND_BELOW'
                    }
                ]
            }
        )
        data_to_insert = pediatric_data_insert_mock.call_args.kwargs['data']
        self.assertEqual(participant_id, data_to_insert.participant_id)
        self.assertEqual(PediatricDataType.AGE_RANGE, data_to_insert.data_type)
        self.assertEqual('SIX_AND_BELOW', data_to_insert.value)

    @mock.patch('rdr_service.dao.pediatric_data_log_dao.logging')
    def test_unrecognized_range(self, logging_mock):
        """Verify that we don't crash if we get an age range we don't recognize."""
        self.send_post(
            'Patient',
            request_data={
                'id': 'P123123123',
                'extension': [
                    {
                        'url': 'https://pmi-fhir-ig.github.io/pmi-fhir-ig/StructureDefinition/child-account-type',
                        'valueCode': 'not_valid'
                    }
                ]
            }
        )
        logging_mock.error.assert_called_with('Unrecognized age range value "not_valid"')

    @mock.patch('rdr_service.dao.pediatric_data_log_dao.PediatricDataLogDao.insert')
    @mock.patch('rdr_service.dao.pediatric_data_log_dao.logging')
    def test_unset_age_range(self, logging_mock, pediatric_data_insert_mock):
        """
        The API receives 'UNSET' for participants that are not pediatric.
        Verify that we don't store a pediatric record for them if that's the case.
        """
        self.send_post(
            'Patient',
            request_data={
                'id': 'P123123123',
                'extension': [
                    {
                        'url': 'https://pmi-fhir-ig.github.io/pmi-fhir-ig/StructureDefinition/child-account-type',
                        'valueCode': 'UNSET'
                    }
                ]
            }
        )
        pediatric_data_insert_mock.assert_not_called()
        logging_mock.error.assert_not_called()

    @mock.patch("rdr_service.api.base_api.log_api_request")
    def test_participant_id_in_log_record(self, mock_log_api_request):
        self.send_post(
            "Patient", request_data={"id": "P123123123", "name": [{"given": ["Peter"]}]}
        )
        mock_log_api_request.assert_called_once()
        self.assertEqual(mock_log_api_request.return_value.participantId, 123123123)




class ProfileUpdateIntegrationTest(BaseTestCase):
    def test_error_when_removing_login(self):
        """
        Ensure the API gracefully handles a request trying to clear any "login" data
        (leaving a summary without an email or phone number)
        """
        summary = self.data_generator.create_database_participant_summary(
            email='test@foo.com'
        )
        self.send_post(
            'Patient',
            request_data={
                'id': f'P{summary.participantId}',
                'telecom': [
                    {
                        'system': 'email',
                        'value': ''
                    }
                ]
            },
            expected_status=400
        )
