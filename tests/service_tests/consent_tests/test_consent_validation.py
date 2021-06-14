from datetime import datetime, timedelta
import json
import mock
from typing import List, Type

from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.model.hpo import HPO
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.consent import files
from rdr_service.services.consent.validation import ConsentValidator
from tests.helpers.unittest_base import BaseTestCase


class ConsentValidationTesting(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ConsentValidationTesting, self).__init__(*args, **kwargs)
        self.uses_database = False

        self.va_hpo = HPO(hpoId=4)
        self.another_hpo = HPO(hpoId=8)

        self._default_signature = 'Test'
        default_consent_timestamp = datetime(2019, 8, 27, 17, 9)
        self._default_signing_date = default_consent_timestamp.date()

        self.participant_summary = ParticipantSummary(
            consentForStudyEnrollmentFirstYesAuthored=default_consent_timestamp
        )
        self.consent_factory_mock = mock.MagicMock(spec=files.ConsentFileAbstractFactory)

        self.validator = ConsentValidator(
            consent_factory=self.consent_factory_mock,
            participant_summary=self.participant_summary,
            va_hpo_id=self.va_hpo.hpoId
        )

    def test_primary_file_ready_for_sync(self):
        """Test the defaults give a consent file ready for syncing"""
        self.consent_factory_mock.get_primary_consents.return_value = [
            self._mock_consent(consent_class=files.PrimaryConsentFile)
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'file_exists': True,
                    'type': ConsentType.PRIMARY,
                    'is_signature_valid': True,
                    'signature_str': self._default_signature,
                    'is_signing_date_valid': True,
                    'signing_date': self._default_signing_date,
                    'sync_status': ConsentSyncStatus.READY_FOR_SYNC
                }
            ],
            self.validator.get_primary_validation_results()
        )

    def test_primary_with_incorrect_date(self):
        incorrect_date_on_file = self._default_signing_date - timedelta(days=300)
        self.consent_factory_mock.get_primary_consents.return_value = [
            self._mock_consent(
                consent_class=files.PrimaryConsentFile,
                get_signature_on_file='signed with wrong date',
                get_date_signed=incorrect_date_on_file
            ),
            self._mock_consent(
                consent_class=files.PrimaryConsentFile,
                get_signature_on_file='signed with no date',
                get_date_signed=None
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'type': ConsentType.PRIMARY,
                    'signature_str': 'signed with wrong date',
                    'is_signing_date_valid': False,
                    'signing_date': incorrect_date_on_file,
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                },
                {
                    'type': ConsentType.PRIMARY,
                    'signature_str': 'signed with no date',
                    'is_signing_date_valid': False,
                    'signing_date': None,
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_primary_validation_results()
        )

    def test_primary_with_signature_image(self):
        self.consent_factory_mock.get_primary_consents.return_value = [
            self._mock_consent(
                consent_class=files.PrimaryConsentFile,
                get_signature_on_file=True
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'type': ConsentType.PRIMARY,
                    'is_signature_valid': True,
                    'signature_str': None,
                    'is_signature_image': True,
                    'sync_status': ConsentSyncStatus.READY_FOR_SYNC
                }
            ],
            self.validator.get_primary_validation_results()
        )

    def test_va_primary_for_non_veteran(self):
        self.participant_summary.hpoId = self.another_hpo.hpoId
        self.consent_factory_mock.get_primary_consents.return_value = [
            self._mock_consent(
                consent_class=files.PrimaryConsentFile,
                get_is_va_consent=True
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'type': ConsentType.PRIMARY,
                    'other_errors': 'veteran consent for non-veteran participant',
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_primary_validation_results()
        )

    def test_non_va_primary_for_veteran(self):
        consent_timestamp = datetime(2020, 1, 17, 13, 7)
        self.participant_summary.consentForStudyEnrollmentFirstYesAuthored = consent_timestamp
        self.participant_summary.hpoId = self.va_hpo.hpoId
        self.consent_factory_mock.get_primary_consents.return_value = [
            self._mock_consent(
                consent_class=files.PrimaryConsentFile,
                get_is_va_consent=False,
                get_signature_on_file=True,
                get_date_signed=consent_timestamp.date()
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'type': ConsentType.PRIMARY,
                    'other_errors': 'non-veteran consent for veteran participant',
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_primary_validation_results()
        )

    def test_ehr_file_ready_for_sync(self):
        ehr_consent_timestamp = datetime(2020, 2, 5, 13, 9)
        self.participant_summary.consentForElectronicHealthRecordsAuthored = ehr_consent_timestamp
        self.consent_factory_mock.get_ehr_consents.return_value = [
            self._mock_consent(
                consent_class=files.EhrConsentFile,
                get_date_signed=ehr_consent_timestamp.date()
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.EHR,
                    'is_signing_date_valid': True,
                    'signing_date': ehr_consent_timestamp.date(),
                    'sync_status': ConsentSyncStatus.READY_FOR_SYNC
                }
            ],
            self.validator.get_ehr_validation_results()
        )

    def test_cabor_file_ready_for_sync(self):
        cabor_consent_timestamp = datetime(2020, 4, 21, 13, 9)
        self.participant_summary.consentForCABoRAuthored = cabor_consent_timestamp
        self.consent_factory_mock.get_cabor_consents.return_value = [
            self._mock_consent(
                consent_class=files.CaborConsentFile,
                get_date_signed=cabor_consent_timestamp.date()
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.CABOR,
                    'is_signing_date_valid': True,
                    'signing_date': cabor_consent_timestamp.date(),
                    'sync_status': ConsentSyncStatus.READY_FOR_SYNC
                }
            ],
            self.validator.get_cabor_validation_results()
        )

    def test_gror_file_ready_for_sync(self):
        gror_consent_timestamp = datetime(2020, 10, 21, 13, 9)
        self.participant_summary.consentForGenomicsRORAuthored = gror_consent_timestamp
        self.consent_factory_mock.get_gror_consents.return_value = [
            self._mock_consent(
                consent_class=files.GrorConsentFile,
                get_date_signed=gror_consent_timestamp.date()
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.GROR,
                    'is_signing_date_valid': True,
                    'signing_date': gror_consent_timestamp.date(),
                    'sync_status': ConsentSyncStatus.READY_FOR_SYNC
                }
            ],
            self.validator.get_gror_validation_results()
        )

    def test_gror_without_checkmark(self):
        self.participant_summary.consentForGenomicsRORAuthored = datetime.combine(
            self._default_signing_date,
            datetime.now().time()
        )
        self.consent_factory_mock.get_gror_consents.return_value = [
            self._mock_consent(
                consent_class=files.GrorConsentFile,
                is_confirmation_selected=False
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.GROR,
                    'other_errors': 'missing consent check mark',
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_gror_validation_results()
        )

    def _mock_consent(self, consent_class: Type[files.ConsentFile], **kwargs):
        consent_args = {
            'get_signature_on_file': self._default_signature,
            'get_date_signed': self._default_signing_date,
            'get_is_va_consent': False
        }
        consent_args.update(kwargs)

        consent_mock = mock.MagicMock(spec=consent_class)
        consent_mock.upload_time = datetime.now()
        for method_name, return_value in consent_args.items():
            if hasattr(consent_mock, method_name):
                getattr(consent_mock, method_name).return_value = return_value
        return consent_mock

    def assertMatchesExpectedResults(self, expected_list, actual_list: List[ConsentFile]):
        self.assertEqual(len(expected_list), len(actual_list))

        def expected_data_found_in_results(expected_result):
            for actual_result in actual_list:
                if all([getattr(actual_result, attr_name) == value for attr_name, value in expected_result.items()]):
                    return True

            return False

        def json_print(data):
            return json.dumps(data, default=str, indent=4)

        for expected in expected_list:
            if not expected_data_found_in_results(expected):
                self.fail(
                    f'{json_print(expected)} not found in results: '
                    f'{json_print([actual.asdict() for actual in actual_list])}'
                )
