from datetime import datetime, timedelta

from rdr_service.participant_enums import WithdrawalAIANCeremonyStatus
from tests.helpers.unittest_base import BaseTestCase, PDRGeneratorTestMixin


class ParticipantGeneratorTest(BaseTestCase, PDRGeneratorTestMixin):
    """Tests misc fields generated for participant resource data"""

    def test_ceremony_decision_fields(self):
        # Set up data for different scenarios of withdrawn participants
        # Clearing microseconds to avoid rounding time up in database and causing test to fail
        two_days_ago = datetime.today().replace(microsecond=0) - timedelta(days=2)
        withdrawal_reason_justification = 'testing withdrawal'
        no_ceremony_native_american_participant = self.data_generator.create_withdrawn_participant(
            withdrawal_reason_justification=withdrawal_reason_justification,
            is_native_american=True,
            requests_ceremony=WithdrawalAIANCeremonyStatus.DECLINED,
            withdrawal_time=two_days_ago
        )
        ceremony_native_american_participant = self.data_generator.create_withdrawn_participant(
            withdrawal_reason_justification=withdrawal_reason_justification,
            is_native_american=True,
            requests_ceremony=WithdrawalAIANCeremonyStatus.REQUESTED,
            withdrawal_time=two_days_ago
        )
        # Non-AIAN should not have been presented with a ceremony choice
        non_native_american_participant = self.data_generator.create_withdrawn_participant(
            withdrawal_reason_justification=withdrawal_reason_justification,
            is_native_american=False,
            requests_ceremony=None,
            withdrawal_time=two_days_ago
        )

        p_id = no_ceremony_native_american_participant.participantId
        ps_rsrc_data = self.make_participant_resource(p_id)
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.DECLINED))
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.DECLINED))

        p_id = ceremony_native_american_participant.participantId
        ps_rsrc_data = self.make_participant_resource(p_id)
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.REQUESTED))
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.REQUESTED))

        p_id = non_native_american_participant.participantId
        ps_rsrc_data = self.make_participant_resource(p_id)
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.UNSET))
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.UNSET))
