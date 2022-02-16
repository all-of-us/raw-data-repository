import mock
from typing import List

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.consent_file import ConsentFile, ConsentType, ConsentSyncStatus
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire_response import QuestionnaireResponse
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
        consent_metrics_dispatch_check_errors_patch = mock.patch(
            'rdr_service.services.consent.validation.dispatch_check_consent_errors_task'
        )
        self.dispatch_check_consent_errors_mock = consent_metrics_dispatch_check_errors_patch.start()
        self.addCleanup(consent_metrics_dispatch_check_errors_patch.stop)
        self.consent_controller = ConsentValidationController(
            consent_dao=self.consent_dao_mock,
            hpo_dao=self.hpo_dao_mock,
            participant_summary_dao=self.participant_summary_dao_mock,
            storage_provider=mock.MagicMock()
        )
        self.store_strategy = StoreResultStrategy(session=mock.MagicMock(), consent_dao=self.consent_dao_mock)

    def test_new_consent_validation(self):
        """The controller should find all recent participant summary consents authored and validate files for them"""
        primary_and_ehr_participant_id = 123
        cabor_participant_id = 456
        self.consent_dao_mock.get_consent_responses_to_validate.return_value = {
            primary_and_ehr_participant_id: [
                ConsentResponse(
                    response=QuestionnaireResponse(participantId=primary_and_ehr_participant_id),
                    type=ConsentType.PRIMARY
                ),
                ConsentResponse(
                    response=QuestionnaireResponse(participantId=primary_and_ehr_participant_id),
                    type=ConsentType.EHR
                )
            ],
            cabor_participant_id: [
                ConsentResponse(
                    response=QuestionnaireResponse(participantId=cabor_participant_id),
                    type=ConsentType.CABOR
                ),
            ]
        }

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

        self.participant_summary_dao_mock.get_by_ids_with_session.return_value = [
            ParticipantSummary(participantId=primary_and_ehr_participant_id),
            ParticipantSummary(participantId=cabor_participant_id)
        ]

        self.consent_controller.validate_consent_uploads(
            session=mock.MagicMock(),
            output_strategy=self.store_strategy
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
        # Confirm call to dispatcher for task that checks for newly detected validation errors
        self.assertEqual(1, self.dispatch_check_consent_errors_mock.call_count)

        # Confirm a call to the dispatcher to rebuild the consent metrics resource data, with the ConsentFile.id
        # values from the expected_updates list
        self.assertDispatchRebuildConsentMetricsCalled([2, 5, 6, 4])

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
