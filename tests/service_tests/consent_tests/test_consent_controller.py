import mock
from typing import List

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.consent_file import ConsentFile, ConsentType, ConsentSyncStatus
from rdr_service.services.consent.validation import ConsentValidationController
from tests.helpers.unittest_base import BaseTestCase


class ConsentControllerTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ConsentControllerTest, self).__init__(*args, **kwargs)
        self.uses_database = False

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

        self.consent_controller = ConsentValidationController(
            consent_dao=self.consent_dao_mock,
            hpo_dao=self.hpo_dao_mock,
            participant_summary_dao=self.participant_summary_dao_mock,
            storage_provider=mock.MagicMock()
        )

    def test_correction_check(self):
        """
        The controller should load all files that need correction and update their state if they've been
        replaced by new files.
        """
        self.consent_dao_mock.get_files_needing_correction.return_value = [
            ConsentFile(type=ConsentType.GROR, file_path='/invalid_gror_1'),
            ConsentFile(type=ConsentType.GROR, file_path='/invalid_gror_2'),
            ConsentFile(type=ConsentType.CABOR, file_path='/invalid_cabor_1'),
            ConsentFile(type=ConsentType.PRIMARY, file_path='/invalid_primary_1'),
            ConsentFile(type=ConsentType.PRIMARY, file_path='/invalid_primary_2')
        ]

        self.consent_validator_mock.get_primary_validation_results.return_value = [
            ConsentFile(sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_primary_1'),
            ConsentFile(sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_primary_2'),
            ConsentFile(sync_status=ConsentSyncStatus.READY_FOR_SYNC, file_path='/valid_primary_1')
        ]
        self.consent_validator_mock.get_cabor_validation_results.return_value = [
            ConsentFile(sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_cabor_1'),
            ConsentFile(sync_status=ConsentSyncStatus.READY_FOR_SYNC, file_path='/valid_cabor_1')
        ]
        self.consent_validator_mock.get_gror_validation_results.return_value = [
            ConsentFile(sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_gror_1'),
            ConsentFile(sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_gror_2'),
            ConsentFile(sync_status=ConsentSyncStatus.NEEDS_CORRECTING, file_path='/invalid_gror_3')
        ]

        self.consent_controller.check_for_corrections()
        self.assertConsentValidationResultsUpdated(
            expected_updates=[
                ConsentFile(file_path='/invalid_primary_1', sync_status=ConsentSyncStatus.OBSOLETE),
                ConsentFile(file_path='/invalid_primary_2', sync_status=ConsentSyncStatus.OBSOLETE),
                ConsentFile(file_path='/valid_primary_1', sync_status=ConsentSyncStatus.READY_FOR_SYNC),
                ConsentFile(file_path='/invalid_cabor_1', sync_status=ConsentSyncStatus.OBSOLETE),
                ConsentFile(file_path='/valid_cabor_1', sync_status=ConsentSyncStatus.READY_FOR_SYNC),
                ConsentFile(file_path='/invalid_gror_3', sync_status=ConsentSyncStatus.NEEDS_CORRECTING)
            ]
        )

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
