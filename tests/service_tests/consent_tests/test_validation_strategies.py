from datetime import date
import mock.mock

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.services.consent.validation import StoreResultStrategy
from tests.helpers.unittest_base import BaseTestCase


class ValidationOutputStrategyIntegrationTest(BaseTestCase):

    def test_store_one_result_per_response(self):
        """
        Validation should only save one valid result per type consent response.
        """
        participant_id = self.data_generator.create_database_participant_summary().participantId

        # out of several successful validation results of the same response,
        # only one successful one should save
        consent_response_a = self.data_generator.create_database_consent_response()
        result_a1 = ConsentFile(
            participant_id=participant_id,
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            file_exists=True,
            file_path='new_file_a1',
            consent_response=consent_response_a
        )
        result_a2 = ConsentFile(
            participant_id=participant_id,
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            file_exists=True,
            file_path='new_file_a2',
            consent_response=consent_response_a
        )

        # out of several unsuccessful results for a response,
        # all of them should be saved
        consent_response_b = self.data_generator.create_database_consent_response()
        result_b1 = ConsentFile(
            participant_id=participant_id,
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            file_exists=True,
            file_path='new_file_b1',
            consent_response=consent_response_b
        )
        result_b2 = ConsentFile(
            participant_id=participant_id,
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            file_exists=True,
            file_path='new_file_b2',
            consent_response=consent_response_b
        )

        with StoreResultStrategy(
            self.session, ConsentDao()
        ) as strategy:
            strategy.add_all([
                result_a1, result_a2,
                result_b1, result_b2
            ])

        stored_results = self.session.query(ConsentFile).filter(
            ConsentFile.participant_id == participant_id
        ).order_by(
            ConsentFile.file_path
        ).all()
        self.assertEqual(
            [result_a1, result_b1, result_b2],
            stored_results
        )

    @mock.patch('rdr_service.services.consent.validation.GCPCloudTask')
    def test_store_updates_ehr_status_correctly(self, _):
        # Initialize the data
        participant_id = self.data_generator.create_database_participant_summary(
            consentForElectronicHealthRecords=QuestionnaireStatus.SUBMITTED_NOT_VALIDATED
        ).participantId
        primary_result = ConsentFile(
            participant_id=participant_id,
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            file_exists=True,
            file_path='new_primary_file',
            consent_response=mock.MagicMock(id=1),
            expected_sign_date=date.today()
        )
        ehr_result = ConsentFile(
            participant_id=participant_id,
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            file_exists=True,
            file_path='new_ehr_file',
            consent_response=mock.MagicMock(id=2),
            expected_sign_date=date.today()
        )

        consent_dao = mock.MagicMock()
        with StoreResultStrategy(self.session, consent_dao) as strategy:
            strategy.add_all([primary_result, ehr_result])

        participant_summary = self.session.query(ParticipantSummary).get(participant_id)
        self.assertEqual(
            QuestionnaireStatus.SUBMITTED_INVALID, participant_summary.consentForElectronicHealthRecords
        )
