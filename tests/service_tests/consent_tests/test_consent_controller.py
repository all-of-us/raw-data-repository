from datetime import datetime, timedelta
import mock
from typing import List

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.consent_file import ConsentFile, ConsentType, ConsentSyncStatus
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.services.consent.validation import ConsentValidationController, StoreResultStrategy
from tests.helpers.unittest_base import BaseTestCase


class ConsentControllerTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ConsentControllerTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super(ConsentControllerTest, self).setUp(*args, **kwargs)

        self.consent_dao_mock = mock.MagicMock(spec=ConsentDao)
        self.hpo_dao_mock = mock.MagicMock(spec=HPODao)
        self.participant_summary_dao_mock = mock.MagicMock(spec=ParticipantSummaryDao)
        consent_validator_patch = mock.patch('rdr_service.services.consent.validation.ConsentValidator')
        self.consent_validator_mock = consent_validator_patch.start().return_value
        self.addCleanup(consent_validator_patch.stop)
        consent_factory_patch = mock.patch(
            'rdr_service.services.consent.validation.files.ConsentFileAbstractFactory.get_file_factory'
        )
        consent_factory_patch.start()
        self.addCleanup(consent_factory_patch.stop)
        consent_metrics_dispatch_rebuild_patch = mock.patch(
            'rdr_service.services.consent.validation.dispatch_rebuild_consent_metrics_tasks'
        )
        self.dispatch_rebuild_consent_metrics_mock = consent_metrics_dispatch_rebuild_patch.start()
        self.addCleanup(consent_metrics_dispatch_rebuild_patch.stop)

        self.consent_controller = ConsentValidationController(
            consent_dao=self.consent_dao_mock,
            hpo_dao=self.hpo_dao_mock,
            participant_summary_dao=self.participant_summary_dao_mock,
            storage_provider=mock.MagicMock()
        )
        self.store_strategy = StoreResultStrategy(session=mock.MagicMock(), consent_dao=self.consent_dao_mock)

    def test_correction_check(self):
        """
        The controller should load all files that need correction and update their state if they've been
        replaced by new files.
        """
        self.consent_dao_mock.get_files_needing_correction.return_value = [
            ConsentFile(id=1, type=ConsentType.GROR, file_path='/invalid_gror_1'),
            ConsentFile(id=2, type=ConsentType.GROR, file_path='/invalid_gror_2'),
            ConsentFile(id=3, type=ConsentType.CABOR, file_path='/invalid_cabor_1'),
            ConsentFile(id=4, type=ConsentType.PRIMARY, file_path='/invalid_primary_1'),
            ConsentFile(id=5, type=ConsentType.PRIMARY, file_path='/invalid_primary_2')
        ]

        self.consent_validator_mock.get_primary_validation_results.return_value = [
            ConsentFile(id=4, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_primary_1'),
            ConsentFile(id=5, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_primary_2'),
            ConsentFile(id=6, sync_status=ConsentSyncStatus.READY_FOR_SYNC, file_path='/valid_primary_1')
        ]
        self.consent_validator_mock.get_cabor_validation_results.return_value = [
            ConsentFile(id=3, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_cabor_1'),
            ConsentFile(id=7, sync_status=ConsentSyncStatus.READY_FOR_SYNC, file_path='/valid_cabor_1')
        ]
        self.consent_validator_mock.get_gror_validation_results.return_value = [
            ConsentFile(id=1, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_gror_1'),
            ConsentFile(id=2, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_gror_2'),
            ConsentFile(id=8, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_gror_3')
        ]

        self.consent_controller.check_for_corrections(session=mock.MagicMock())
        self.assertConsentValidationResultsUpdated(
            expected_updates=[
                ConsentFile(id=4, file_path='/invalid_primary_1', sync_status=ConsentSyncStatus.OBSOLETE),
                ConsentFile(id=5, file_path='/invalid_primary_2', sync_status=ConsentSyncStatus.OBSOLETE),
                ConsentFile(id=6, file_path='/valid_primary_1', sync_status=ConsentSyncStatus.READY_FOR_SYNC),
                ConsentFile(id=3, file_path='/invalid_cabor_1', sync_status=ConsentSyncStatus.OBSOLETE),
                ConsentFile(id=7, file_path='/valid_cabor_1', sync_status=ConsentSyncStatus.READY_FOR_SYNC),
                ConsentFile(id=8, file_path='/invalid_gror_3', sync_status=ConsentSyncStatus.NEEDS_CORRECTING)
            ]
        )
        # Confirm a call to the dispatcher to rebuild the consent metrics resource data, with the ConsentFile.id
        # values from the expected_updates list
        self.assertDispatchRebuildConsentMetricsCalled([4, 5, 6, 3, 7, 8])

    def test_new_consent_validation(self):
        """The controller should find all recent participant summary consents authored and validate files for them"""
        self.consent_validator_mock.get_primary_validation_results.return_value = [
            ConsentFile(id=1, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_primary_1'),
            ConsentFile(id=2, sync_status=ConsentSyncStatus.READY_FOR_SYNC, file_path='/valid_primary_2'),
            ConsentFile(id=3, sync_status=ConsentSyncStatus.READY_FOR_SYNC, file_path='/valid_primary_3')
        ]
        self.consent_validator_mock.get_cabor_validation_results.return_value = [
            ConsentFile(id=4, sync_status=ConsentSyncStatus.READY_FOR_SYNC, file_path='/valid_cabor_1')
        ]
        self.consent_validator_mock.get_ehr_validation_results.return_value = [
            ConsentFile(id=5, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_ehr_1'),
            ConsentFile(id=6, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_ehr_2')
        ]
        self.consent_validator_mock.get_gror_validation_results.return_value = [
            ConsentFile(id=7, sync_status=ConsentSyncStatus.READY_FOR_SYNC, file_path='/gror_not_checked')
        ]

        min_consent_date_checked = datetime(2020, 4, 1)
        self.consent_dao_mock.get_participants_with_unvalidated_files.return_value = [
            ParticipantSummary(
                consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
                consentForStudyEnrollmentAuthored=min_consent_date_checked,  # Needs to be set for PrimaryUpdate check
                consentForStudyEnrollmentFirstYesAuthored=min_consent_date_checked + timedelta(days=5),
                consentForElectronicHealthRecords=QuestionnaireStatus.SUBMITTED,
                consentForElectronicHealthRecordsAuthored=min_consent_date_checked + timedelta(days=10)
            ),
            ParticipantSummary(
                consentForCABoR=QuestionnaireStatus.SUBMITTED,
                consentForStudyEnrollmentAuthored=min_consent_date_checked,  # Needs to be set for PrimaryUpdate check
                consentForCABoRAuthored=min_consent_date_checked + timedelta(days=5)
            ),
            ParticipantSummary(
                consentForGenomicsROR=QuestionnaireStatus.SUBMITTED,
                consentForStudyEnrollmentAuthored=min_consent_date_checked,  # Needs to be set for PrimaryUpdate check
                consentForGenomicsRORAuthored=min_consent_date_checked - timedelta(days=5)
            ),
            ParticipantSummary(
                consentForGenomicsROR=QuestionnaireStatus.SUBMITTED_NOT_SURE,
                consentForStudyEnrollmentAuthored=min_consent_date_checked,  # Needs to be set for PrimaryUpdate check
                consentForGenomicsRORAuthored=min_consent_date_checked + timedelta(days=20)
            )
        ]

        self.consent_controller.validate_recent_uploads(
            session=mock.MagicMock(),
            output_strategy=self.store_strategy,
            min_consent_date=min_consent_date_checked
        )
        self.store_strategy.process_results()
        self.assertConsentValidationResultsUpdated(
            expected_updates=[
                ConsentFile(id=2, file_path='/valid_primary_2', sync_status=ConsentSyncStatus.READY_FOR_SYNC),
                ConsentFile(id=5, file_path='/invalid_ehr_1', sync_status=ConsentSyncStatus.NEEDS_CORRECTING),
                ConsentFile(id=6, file_path='/invalid_ehr_2', sync_status=ConsentSyncStatus.NEEDS_CORRECTING),
                ConsentFile(id=4, file_path='/valid_cabor_1', sync_status=ConsentSyncStatus.READY_FOR_SYNC),
            ]
        )
        # Confirm a call to the dispatcher to rebuild the consent metrics resource data, with the ConsentFile.id
        # values from the expected_updates list
        self.assertDispatchRebuildConsentMetricsCalled([2, 5, 6, 4])

    def test_no_duplication_in_validation(self):
        """
        Check to make sure the validation check for recent consents doesn't create
        new validation records for consents that have already been checked
        """
        self.consent_dao_mock.get_validation_results_for_participants.return_value = [
            ConsentFile(id=1, file_path='/previous_1', file_exists=True),
            ConsentFile(id=2, file_path='/previous_2', file_exists=True),
        ]
        self.consent_validator_mock.get_primary_validation_results.return_value = [
            ConsentFile(id=1, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/previous_1', file_exists=True),
            ConsentFile(id=2, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/previous_2', file_exists=True),
            ConsentFile(id=3, sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/new_file_1', file_exists=True)
        ]

        min_consent_date_checked = datetime(2020, 4, 1)
        self.consent_dao_mock.get_participants_with_unvalidated_files.return_value = [
            ParticipantSummary(
                consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
                consentForStudyEnrollmentAuthored=datetime(2020, 5, 1),  # Needs to be set for PrimaryUpdate check
                consentForStudyEnrollmentFirstYesAuthored=min_consent_date_checked + timedelta(days=5)
            )
        ]

        self.consent_controller.validate_recent_uploads(
            session=mock.MagicMock(),
            output_strategy=self.store_strategy,
            min_consent_date=min_consent_date_checked
        )
        self.store_strategy.process_results()
        self.assertConsentValidationResultsUpdated(
            expected_updates=[
                ConsentFile(file_path='/new_file_1', sync_status=ConsentSyncStatus.NEEDS_CORRECTING)
            ]
        )
        # Confirm a call to the dispatcher to rebuild the consent metrics resource data, with the ConsentFile.id
        # values from the expected_updates list
        self.assertDispatchRebuildConsentMetricsCalled([3])

    def test_validating_specific_consents(self):
        """Make sure only the provided consent types are validated when specified"""
        # Create a participant that has consented to the primary, ehr, and gror consents
        summary = ParticipantSummary(
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
            consentForElectronicHealthRecords=QuestionnaireStatus.SUBMITTED,
            consentForGenomicsROR=QuestionnaireStatus.SUBMITTED
        )

        # Mock out the consent files for the participant
        primary_file = ConsentFile(id=1, sync_status=ConsentSyncStatus.READY_FOR_SYNC,
                                   file_path='/primary', file_exists=True)
        self.consent_validator_mock.get_primary_validation_results.return_value = [primary_file]
        ehr_file = ConsentFile(id=2, sync_status=ConsentSyncStatus.READY_FOR_SYNC, file_path='/ehr', file_exists=True)
        self.consent_validator_mock.get_ehr_validation_results.return_value = [ehr_file]
        gror_file = ConsentFile(id=3, sync_status=ConsentSyncStatus.READY_FOR_SYNC, file_path='/gror', file_exists=True)
        self.consent_validator_mock.get_gror_validation_results.return_value = [gror_file]

        # Make sure that only specific consent types are validated
        self.consent_controller.validate_participant_consents(
            summary=summary,
            output_strategy=self.store_strategy,
            types_to_validate=[ConsentType.GROR, ConsentType.EHR]
        )
        self.store_strategy.process_results()
        self.assertConsentValidationResultsUpdated(expected_updates=[ehr_file, gror_file])
        # Confirm a call to the dispatcher to rebuild the consent metrics resource data, with the ConsentFile.id
        # values from the expected_updates list
        self.assertDispatchRebuildConsentMetricsCalled([ehr_file.id, gror_file.id])

    def assertDispatchRebuildConsentMetricsCalled(self, expected_id_list, call_count=1, call_number=1):
        """
        Confirm the mocked dispatch_rebuild_consent_metrics_tasks method was called with the expected id list
        Most test cases should only expect a single call to the dispatch method, but a different expected call_count
        and a different (1-based) call_number whose id_list argument should be validated can be specified
        """
        self.assertEqual(call_count, self.dispatch_rebuild_consent_metrics_mock.call_count)
        # Testing first (0th) arg: dispatch_rebuild_consent_metrics_task(id_list, <more kwargs>). kwargs ignored (_)
        # Adjust call_number for 0-based indexing.  assertCountEqual tests list equivalence regardless of the order of
        # the values in the compared lists
        args, _ = self.dispatch_rebuild_consent_metrics_mock.call_args_list[call_number - 1]
        self.assertCountEqual(args[0], expected_id_list)

    def assertConsentValidationResultsUpdated(self, expected_updates: List[ConsentFile]):
        """Make sure the validation results are sent to the dao"""
        actual_updates: List[ConsentFile] = self.consent_dao_mock.batch_update_consent_files.call_args.args[0]
        self.assertEqual(len(expected_updates), len(actual_updates))

        for expected in expected_updates:
            found_expected = False
            for actual in actual_updates:
                if expected.file_path == actual.file_path and expected.sync_status == actual.sync_status:
                    found_expected = True
                    break

            if not found_expected:
                self.fail('Unable to find an expected update in the updated validation results')
