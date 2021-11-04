import mock.mock

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.services.consent.validation import ReplacementStoringStrategy
from tests.helpers.unittest_base import BaseTestCase


class ValidationOutputStrategyIntegrationTest(BaseTestCase):
    def test_store_strategy_updates_pdr(self):
        # Initialize the data
        participant = self.data_generator.create_database_participant()
        existing_result = self.data_generator.create_database_consent_file(
            participant_id=participant.participantId,
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING,
            file_exists=False
        )
        new_result = ConsentFile(
            participant_id=participant.participantId,
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC,
            file_exists=True,
            file_path='new_file'
        )

        with mock.patch('rdr_service.services.consent.validation.dispatch_rebuild_consent_metrics_tasks') \
                as rebuild_mock:
            # Make a change and add the results to the output strategy
            consent_dao = ConsentDao()
            with consent_dao.session() as session, ReplacementStoringStrategy(session, consent_dao) as strategy:
                existing_result.sync_status = ConsentSyncStatus.OBSOLETE
                strategy.add_all([existing_result, new_result])

            # Make sure the rebuild notification was sent to PDR with non-null ids
            sent_ids = rebuild_mock.call_args.args[0]
            self.assertFalse(any([file_id is None for file_id in sent_ids]))
