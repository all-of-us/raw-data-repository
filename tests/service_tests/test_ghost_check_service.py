from datetime import datetime
import mock

from rdr_service.model.participant import Participant
from rdr_service.services.ghost_check_service import GhostCheckService, GhostFlagModification
from tests.helpers.unittest_base import BaseTestCase


class GhostCheckServiceTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(GhostCheckServiceTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super(GhostCheckServiceTest, self).setUp(*args, **kwargs)
        self.logger_mock = mock.MagicMock()

        client_class_patch = mock.patch('rdr_service.services.ghost_check_service.PtscClient')
        self.client_mock = client_class_patch.start().return_value
        self.client_mock.request_next_page.return_value = None  # Keeping tests from looping on next pages
        self.addCleanup(client_class_patch.stop)

        participant_dao_class_patch = mock.patch('rdr_service.services.ghost_check_service.ParticipantDao')
        self.participant_dao_mock = participant_dao_class_patch.start().return_value
        self.addCleanup(participant_dao_class_patch.stop)

        dao_patch = mock.patch('rdr_service.services.ghost_check_service.GhostCheckDao')
        self.ghost_dao_mock = dao_patch.start()
        self.addCleanup(dao_patch.stop)

        pdr_rebuild_patch = mock.patch('rdr_service.services.ghost_check_service.dispatch_participant_rebuild_tasks')
        self.pdr_rebuild_mock = pdr_rebuild_patch.start()
        self.addCleanup(pdr_rebuild_patch.stop)

        self.service = GhostCheckService(
            session=mock.MagicMock(),
            ptsc_config=mock.MagicMock(),
            logger=self.logger_mock
        )

    def test_no_participants_are_ghosts(self):
        """If no participants are ghosts, we should still record that they were checked"""
        test_participants = [
            Participant(participantId=1),
            Participant(participantId=2),
            Participant(participantId=3),
            Participant(participantId=4)
        ]
        self.ghost_dao_mock.get_participants_needing_checked.return_value = test_participants
        self.client_mock.get_participant_lookup.return_value = {
            'participants': [{'drcId': f'P{participant.participantId}'} for participant in test_participants]
        }

        self.service.run_ghost_check(start_date=datetime.now())
        self.ghost_dao_mock.record_ghost_check.assert_has_calls([
            mock.call(participant_id=participant.participantId, modification_performed=None, session=mock.ANY)
            for participant in test_participants
        ])

        self.participant_dao_mock.update_ghost_participant.assert_not_called()
        self.assertEqual(self.pdr_rebuild_mock.call_count, 0)

    def test_logging_for_vibrent_server_anomalies(self):
        """Vibrent could have ids we don't know about, or participants with missing DRC ids"""
        self.ghost_dao_mock.get_participants_needing_checked.return_value = []
        self.client_mock.get_participant_lookup.return_value = {
            'participants': [
                {'drcId': 'P1234'},
                {'drcId': None, 'vibrentId': 45}
            ]
        }

        self.service.run_ghost_check(start_date=datetime.now())
        self.logger_mock.error.assert_has_calls([
            mock.call("Vibrent has missing drc id: {'drcId': None, 'vibrentId': 45}"),
            mock.call('Vibrent had unknown id: 1234')
        ], any_order=True)

    def test_changing_ghost_flags(self):
        """Test that participant ghost flags are updated where needed."""
        # Have two participants come from the database, but only one exist on the API
        self.ghost_dao_mock.get_participants_needing_checked.return_value = [
            Participant(participantId=1123, isGhostId=True),
            Participant(participantId=4567, isGhostId=False),
        ]

        def api_response(**kwargs):
            if 'participant_id' in kwargs:
                return None  # Give that they don't exist when looking for the specific participant from the API
            else:
                return {
                    'participants': [{'drcId': 'P1123'}]
                }
        self.client_mock.get_participant_lookup.side_effect = api_response

        self.service.run_ghost_check(start_date=datetime.now())
        self.ghost_dao_mock.record_ghost_check.assert_has_calls([
            mock.call(
                participant_id=1123,
                modification_performed=GhostFlagModification.GHOST_FLAG_REMOVED,
                session=mock.ANY
            ),
            mock.call(
                participant_id=4567,
                modification_performed=GhostFlagModification.GHOST_FLAG_SET,
                session=mock.ANY
            )
        ])
        self.participant_dao_mock.update_ghost_participant.assert_has_calls([
            mock.call(
                pid=1123,
                is_ghost=False,
                session=mock.ANY
            ),
            mock.call(
                pid=4567,
                is_ghost=True,
                session=mock.ANY
            )
        ])
        # Assert the PDR data rebuild was invoked with both pids in the batch list
        self.assertEqual(1, self.pdr_rebuild_mock.call_count)
        self.assertListEqual(self.pdr_rebuild_mock.call_args[0][0], [1123, 4567])

