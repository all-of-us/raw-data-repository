from datetime import date, datetime, timedelta
import json
import mock
from typing import List, Type

from rdr_service import config
from rdr_service.code_constants import SENSITIVE_EHR_STATES
from rdr_service.dao import database_factory
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType, ConsentOtherErrors
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.model.hpo import HPO
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.services.consent import files
from rdr_service.services.consent.validation import ConsentValidator, EhrStatusUpdater, StoreResultStrategy
from tests.helpers.unittest_base import BaseTestCase


class ConsentValidationTesting(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ConsentValidationTesting, self).__init__(*args, **kwargs)
        self.uses_database = False

        self.va_hpo = HPO(hpoId=4)
        self.another_hpo = HPO(hpoId=8)

        self._default_signature = 'Test'
        self._default_consent_timestamp = datetime(2019, 8, 27, 17, 9)
        self._default_signing_date = self._default_consent_timestamp.date()

        self.participant_summary = ParticipantSummary(
            consentForStudyEnrollmentFirstYesAuthored=self._default_consent_timestamp
        )
        self.consent_factory_mock = mock.MagicMock(spec=files.ConsentFileAbstractFactory)

        self.validator = ConsentValidator(
            consent_factory=self.consent_factory_mock,
            participant_summary=self.participant_summary,
            va_hpo_id=self.va_hpo.hpoId,
            session=mock.MagicMock()
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

    def test_primary_with_slightly_off_date(self):
        shifted_date_on_file = self._default_signing_date - timedelta(days=3)
        self.consent_factory_mock.get_primary_consents.return_value = [
            self._mock_consent(
                consent_class=files.PrimaryConsentFile,
                get_signature_on_file='signed with slightly off date',
                get_date_signed=shifted_date_on_file
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'type': ConsentType.PRIMARY,
                    'signature_str': 'signed with slightly off date',
                    'is_signing_date_valid': True,
                    'signing_date': shifted_date_on_file,
                    'sync_status': ConsentSyncStatus.READY_FOR_SYNC
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
                    'other_errors': ConsentOtherErrors.VETERAN_CONSENT_FOR_NON_VETERAN,
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_primary_validation_results()
        )

    def test_non_va_primary_for_veteran(self):
        self.participant_summary.hpoId = self.va_hpo.hpoId
        self.consent_factory_mock.get_primary_consents.return_value = [
            self._mock_consent(
                consent_class=files.PrimaryConsentFile,
                get_is_va_consent=False,
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'type': ConsentType.PRIMARY,
                    'other_errors': ConsentOtherErrors.NON_VETERAN_CONSENT_FOR_VETERAN,
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
                    'other_errors': ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK,
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_gror_validation_results()
        )

    def test_gror_missing(self):
        self.participant_summary.consentForGenomicsRORAuthored = datetime.combine(
            self._default_signing_date,
            datetime.now().time()
        )
        self.consent_factory_mock.get_gror_consents.return_value = []
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.GROR,
                    'file_exists': False,
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_gror_validation_results()
        )

    def test_missing_file_validation_storage(self):
        """
        A bug was found with the validation storage strategies. This ensures that any records that indicate
        missing files don't interfere with each other and all the needed records get stored.
        """
        # mock a consent DAO so we can isolate the output strategy instance
        consent_dao_mock = mock.MagicMock()

        # Create two participant ids with missing files
        # one will have a missing PRIMARY and will get a new validation for a missing GROR
        # the other will have a missing EHR and will get a new validation for a missing PRIMARY
        new_gror_participant_id = 1234
        new_primary_participant_id = 5678
        consent_dao_mock.get_validation_results_for_participants.return_value = [
            ConsentFile(id=1, participant_id=new_gror_participant_id, type=ConsentType.PRIMARY, file_exists=False),
            ConsentFile(id=2, participant_id=new_primary_participant_id, type=ConsentType.EHR, file_exists=False)
        ]

        # Create some results to provide to the output strategy for each participant
        new_primary_result = ConsentFile(
            id=3,
            participant_id=new_primary_participant_id,
            type=ConsentType.PRIMARY,
            file_exists=False,
            consent_response=ConsentResponse(id=1)
        )
        previous_ehr_result = ConsentFile(
            id=2,
            participant_id=new_primary_participant_id,
            type=ConsentType.EHR,
            file_exists=False,
            consent_response=ConsentResponse(id=2)
        )
        new_gror_result = ConsentFile(
            id=4,
            participant_id=new_gror_participant_id,
            type=ConsentType.GROR,
            file_exists=False,
            consent_response=ConsentResponse(id=3)
        )

        # Create a new storage validation strategy and provide the new validation results for each participant
        with StoreResultStrategy(
            session=mock.MagicMock(),
            consent_dao=consent_dao_mock
        ) as output_strategy:
            output_strategy.add_all([new_primary_result, previous_ehr_result, new_gror_result])

        # Verify that both records that provide new validation information were stored
        consent_dao_mock.batch_update_consent_files.assert_called_with([new_primary_result, new_gror_result], mock.ANY)

    def test_multiple_distinct_consents(self):
        """
        It's possible to have multiple consents that need to be processed independently
        rather than ignored as duplicates. For example, when a new EHR consent is submitted, the participant's
        status is set to NOT_VALIDATED, and the new consent file needs to continue through processing to
        have their status updated.
        """
        # mock a consent DAO so we can isolate the output strategy instance
        consent_dao_mock = mock.MagicMock()

        # Create two existing consent validation records:
        #   one that should appear as an older record, but not prevent a new file from being saved
        #   and another that would be found as a duplication of a new record and prevent it from being saved
        participant_id = 1234
        consent_dao_mock.get_validation_results_for_participants.return_value = [
            ConsentFile(
                participant_id=participant_id,
                type=ConsentType.PRIMARY,
                expected_sign_date=date(2021, 10, 17),
                file_path='duplicate_primary',
                sync_status=ConsentSyncStatus.SYNC_COMPLETE,
                consent_response=ConsentResponse(id=1)
            ),
            ConsentFile(
                participant_id=participant_id,
                type=ConsentType.EHR,
                expected_sign_date=date(2022, 2, 4),
                file_path='older_ehr',
                sync_status=ConsentSyncStatus.SYNC_COMPLETE,
                consent_response=ConsentResponse(id=2)
            )
        ]

        # Create two new validations: one that would be a duplication, and another that's not
        duplicate_primary_result = ConsentFile(  # A re-play of the primary consent
            participant_id=participant_id,
            type=ConsentType.PRIMARY,
            expected_sign_date=date(2021, 10, 17),
            file_exists=True,
            file_path='replay_primary',
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            consent_response=ConsentResponse(id=3)
        )
        new_ehr_result = ConsentFile(  # Another EHR file, but signed later than the one we've already seen
            participant_id=participant_id,
            type=ConsentType.EHR,
            expected_sign_date=date(2023, 1, 23),
            file_exists=True,
            file_path='newer_ehr',
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            consent_response=ConsentResponse(id=4)
        )

        # Create a new storage validation strategy and provide the new validation results for each participant
        with StoreResultStrategy(
            session=mock.MagicMock(),
            consent_dao=consent_dao_mock
        ) as output_strategy:
            output_strategy.add_all([duplicate_primary_result, new_ehr_result])

        # Verify that the EHR validation was fully processed and the duplicate Primary was ignored
        consent_dao_mock.batch_update_consent_files.assert_called_with([new_ehr_result], mock.ANY)

    def test_primary_update_agreement_check(self):
        self.participant_summary.consentForStudyEnrollmentAuthored = datetime.combine(
            self._default_signing_date,
            datetime.now().time()
        ) + timedelta(days=100)
        self.consent_factory_mock.get_primary_update_consents.return_value = [
            self._mock_consent(
                consent_class=files.PrimaryConsentUpdateFile,
                is_agreement_selected=False
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.PRIMARY_UPDATE,
                    'other_errors': ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK,
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_primary_update_validation_results()
        )

    def test_primary_update_missing_check_and_non_va(self):
        self.participant_summary.consentForStudyEnrollmentAuthored = datetime.combine(
            self._default_signing_date,
            datetime.now().time()
        ) + timedelta(days=100)
        self.participant_summary.hpoId = self.va_hpo.hpoId
        self.consent_factory_mock.get_primary_update_consents.return_value = [
            self._mock_consent(
                consent_class=files.PrimaryConsentUpdateFile,
                is_agreement_selected=False,
                get_is_va_consent=False
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.PRIMARY_UPDATE,
                    'other_errors': ", ".join([ConsentOtherErrors.NON_VETERAN_CONSENT_FOR_VETERAN,
                                               ConsentOtherErrors.MISSING_CONSENT_CHECK_MARK]),
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_primary_update_validation_results()
        )

    @mock.patch('rdr_service.services.consent.validation.QuestionnaireResponseDao')
    def test_sensitive_state_of_residence(self, response_dao_mock):
        """Check that state of residence is used to determine if a PDF should be the sensitive version"""
        self.temporarily_override_config_setting(config.SENSITIVE_EHR_RELEASE_DATE, '1990-1-1')
        response_dao_mock.get_latest_answer_for_state_receiving_care.return_value = None
        response_dao_mock.get_latest_answer_for_state_of_residence.return_value = SENSITIVE_EHR_STATES[0]

        self.participant_summary.consentForElectronicHealthRecordsAuthored = self._default_consent_timestamp
        self.consent_factory_mock.get_ehr_consents.return_value = [
            self._mock_consent(
                consent_class=files.EhrConsentFile,
                is_sensitive_form=False
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.EHR,
                    'other_errors': ConsentOtherErrors.SENSITIVE_EHR_EXPECTED,
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_ehr_validation_results()
        )

    @mock.patch('rdr_service.services.consent.validation.QuestionnaireResponseDao')
    def test_sensitive_state_of_care(self, response_dao_mock):
        """Check that state of care is used over state of residence when it is available"""
        self.temporarily_override_config_setting(config.SENSITIVE_EHR_RELEASE_DATE, '1990-1-1')
        response_dao_mock.get_latest_answer_for_state_receiving_care.return_value = SENSITIVE_EHR_STATES[0]
        response_dao_mock.get_latest_answer_for_state_of_residence.return_value = 'otherstate'

        self.participant_summary.consentForElectronicHealthRecordsAuthored = self._default_consent_timestamp
        self.consent_factory_mock.get_ehr_consents.return_value = [
            self._mock_consent(
                consent_class=files.EhrConsentFile,
                is_sensitive_form=False
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.EHR,
                    'other_errors': ConsentOtherErrors.SENSITIVE_EHR_EXPECTED,
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_ehr_validation_results()
        )

    def test_long_signature_gets_truncated(self):
        """Some signatures are abnormally long, and we shouldn't have the validation process fail because of that"""

        # Create a mock of a file with a long signature
        consent_file = mock.MagicMock()
        consent_file.get_signature_on_file.return_value = "test" * 100

        # Load the signature using the validator
        parsing_result = ConsentFile()
        ConsentValidator._store_signature(
            result=parsing_result,
            consent_file=consent_file
        )

        # Make sure the signature got truncated if it was too large
        self.assertEqual(200, len(parsing_result.signature_str))

    def test_etm_file_ready_for_sync(self):
        etm_consent_timestamp = datetime(2020, 10, 21, 13, 9)
        self.participant_summary.consentForEtMAuthored = etm_consent_timestamp
        self.consent_factory_mock.get_etm_consents.return_value = [
            self._mock_consent(
                consent_class=files.EtmConsentFile,
                get_date_signed=etm_consent_timestamp.date()
            )
        ]
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.ETM,
                    'is_signing_date_valid': True,
                    'signing_date': etm_consent_timestamp.date(),
                    'sync_status': ConsentSyncStatus.READY_FOR_SYNC
                }
            ],
            self.validator.get_etm_validation_results()
        )

    def test_etm_missing(self):
        self.participant_summary.consentForEtMAuthored = datetime.combine(
            self._default_signing_date,
            datetime.now().time()
        )
        self.consent_factory_mock.get_etm_consents.return_value = []
        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.ETM,
                    'file_exists': False,
                    'sync_status': ConsentSyncStatus.NEEDS_CORRECTING
                }
            ],
            self.validator.get_etm_validation_results()
        )

    @mock.patch('rdr_service.services.consent.validation.AccountLinkDao')
    def test_pediatric_guardian_name(self, _):
        """Check that the guardian's name matching is flexible (case-insensitive and optional middle name"""

        self.consent_factory_mock.get_pediatric_primary_consents.return_value = [
            self._mock_consent(
                consent_class=files.PediatricPrimaryConsentFile,
                get_guardian_name='TEST AL MCTESTERSON'
            )
        ]
        guardian_participant = ParticipantSummary(
            firstName='Test',
            lastName='McTesterson'
        )
        self.validator._session.query.return_value.filter.return_value.one_or_none.return_value = guardian_participant

        self.assertMatchesExpectedResults(
            [
                {
                    'participant_id': self.participant_summary.participantId,
                    'type': ConsentType.PEDIATRIC_PRIMARY,
                    'sync_status': ConsentSyncStatus.READY_FOR_SYNC
                }
            ],
            self.validator.get_pediatric_primary_validation_results()
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
        consent_mock.file_path = '/test'
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


class ValidationDbTesting(BaseTestCase):

    @mock.patch('rdr_service.services.consent.validation.GCPCloudTask')
    def test_reading_current_ehr_status(self, _):
        """
        The EhrStatusUpdater should only move statuses from ...NOT_VALIDATED, rather than updating any other status based
        on a recent validation result. We've seen some cases where the validation starts running and caches a
        ...NOT_VALIDATED result, but before setting the new status we get a no-consent response from the participant.
        The EhrStatusUpdater doesn't see this new status, because we still have previous status in the cache.
        """

        # make a summary with a NOT_VALIDATED status in the current session
        participant_summary = self.data_generator.create_database_participant_summary(
            consentForElectronicHealthRecords=QuestionnaireStatus.SUBMITTED_NOT_VALIDATED
        )

        # set the status to NO in the db in another session (to avoid applying it to the cache of the current session)
        with database_factory.get_database().session() as new_session:
            copied_summary: ParticipantSummary = new_session.query(ParticipantSummary).filter(
                ParticipantSummary.participantId == participant_summary.participantId
            ).one()
            copied_summary.consentForElectronicHealthRecords = QuestionnaireStatus.SUBMITTED_NO_CONSENT

        # make sure the cached version still has NOT_VALIDATED (to make sure the test still functions as intended)
        original_summary: ParticipantSummary = self.session.query(ParticipantSummary).get(participant_summary.participantId)
        self.assertEqual(QuestionnaireStatus.SUBMITTED_NOT_VALIDATED, original_summary.consentForElectronicHealthRecords)

        # run the EHR updater and check that it doesn't clobber the NO by using the cached NEEDS_VALIDATION
        updater = EhrStatusUpdater(session=self.session)
        updater._update_status(
            participant_id=participant_summary.participantId,
            has_valid_file=True,
            authored_time=datetime.utcnow()
        )

        self.session.refresh(participant_summary)
        self.assertEqual(QuestionnaireStatus.SUBMITTED_NO_CONSENT, participant_summary.consentForElectronicHealthRecords)
