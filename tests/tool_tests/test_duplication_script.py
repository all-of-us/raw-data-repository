from datetime import datetime

import mock

from rdr_service.dao.duplicate_account_dao import DuplicateExistsException
from rdr_service.model.duplicate_account import DuplicationSource, DuplicationStatus, PrimaryParticipantIndication
from rdr_service.tools.tool_libs.duplicate_accounts import DuplicateAccountScript
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.tool_test_mixin import ToolTestMixin


class DuplicationScriptTest(ToolTestMixin, BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
        self.dao_mock = self.mock('rdr_service.tools.tool_libs.duplicate_accounts.DuplicateAccountDao')

    def test_adding_duplicate(self):
        self.run_tool(DuplicateAccountScript, tool_args={
            'pids': 'P123,P456',
            'status': 'APPROVED',
            'timestamp': '2022-10-09',
            'first_is_primary': True,
            'clear_primary': False,
            'update': False
        }, mock_session=True)
        self.dao_mock.store_duplication.assert_called_with(
            participant_a_id=123,
            participant_b_id=456,
            authored=datetime(2022, 10, 9),
            source=DuplicationSource.SUPPORT_TICKET,
            status=DuplicationStatus.APPROVED,
            primary_account=PrimaryParticipantIndication.PARTICIPANT_A,
            session=mock.ANY
        )

    def test_updating_duplicate(self):
        self.dao_mock.store_duplication.side_effect = DuplicateExistsException(mock.MagicMock())
        duplication_args = {
            'pids': 'P1234,P4321',
            'status': 'POTENTIAL',
            'timestamp': '2022-10-09',
            'first_is_primary': False,
            'clear_primary': False,
            'update': False
        }

        # If a duplicate pair exists, attempting to insert it should fail
        with self.assertRaises(DuplicateExistsException):
            self.run_tool(DuplicateAccountScript, tool_args=duplication_args, mock_session=True)

        # Update should be used instead
        duplication_args['update'] = True
        self.run_tool(DuplicateAccountScript, tool_args=duplication_args, mock_session=True)
        self.dao_mock.update_duplication.assert_called_with(
            participant_a_id=1234,
            participant_b_id=4321,
            authored=datetime(2022, 10, 9),
            status=DuplicationStatus.POTENTIAL,
            session=mock.ANY
        )
