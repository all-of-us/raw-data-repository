from datetime import datetime, timedelta
import mock

from rdr_service import config
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.participant_data_validation import ParticipantDataValidation
from tests.helpers.unittest_base import BaseTestCase


class ParticipantDataValidationTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs):
        super(ParticipantDataValidationTest, self).setUp(*args, **kwargs)

        mock_summary_dao_class = self.mock('rdr_service.services.participant_data_validation.ParticipantSummaryDao')
        self.summary_dao_instance = mock_summary_dao_class.return_value

        mock_slack_client_class = self.mock('rdr_service.services.participant_data_validation.SlackMessageHandler')
        self.slack_client_instance = mock_slack_client_class.return_value

        self.logging_mock = self.mock('rdr_service.services.participant_data_validation.logging')

        self.temporarily_override_config_setting(config.RDR_SLACK_WEBHOOKS, {
            'rdr_validation_webhook': 'aoeu1234'
        })

    def test_valid_date_of_birth_gives_no_errors(self):
        self._set_consent_datetime(datetime.utcnow())
        date_of_birth = datetime.utcnow() - timedelta(weeks=52 * 23)  # about 23 years ago

        ParticipantDataValidation.analyze_date_of_birth(
            date_of_birth=date_of_birth,
            participant_id=1234
        )

        self.slack_client_instance.send_message_to_webhook.assert_not_called()
        self.logging_mock.warning.assert_not_called()

    def test_invalid_date_of_birth_gives_errors(self):
        self._set_consent_datetime(datetime.utcnow())
        date_of_birth = datetime.utcnow() - timedelta(days=10)

        ParticipantDataValidation.analyze_date_of_birth(
            date_of_birth=date_of_birth,
            participant_id=1234
        )

        self.slack_client_instance.send_message_to_webhook.assert_called_with(
            message_data={'text': 'Invalid date of birth detected'}
        )
        self.assertEqual(2, self.logging_mock.warning.call_count)
        self.logging_mock.warning.assert_has_calls(
            calls=[
                mock.call(
                    'Unexpected date of birth for P1234: date of birth means age at consent was invalid for program'
                ),
                mock.call(
                    'Unexpected date of birth for P1234: date of birth means current age is invalid for program'
                )
            ],
            any_order=True
        )

    def test_invalid_dob_found_with_strange_authored_date(self):
        """If the authored date is at a strange time, make sure invalid dob is still detected"""
        self._set_consent_datetime(datetime(1940, 1, 1))
        ParticipantDataValidation.analyze_date_of_birth(
            date_of_birth=datetime(1870, 1, 1),
            participant_id=1234
        )

        self.slack_client_instance.send_message_to_webhook.assert_called_with(
            message_data={'text': 'Invalid date of birth detected'}
        )
        self.assertEqual(1, self.logging_mock.warning.call_count)
        self.logging_mock.warning.assert_has_calls(
            calls=[
                mock.call(
                    'Unexpected date of birth for P1234: date of birth means current age is invalid for program'
                )
            ],
            any_order=True
        )

    def _set_consent_datetime(self, consent_datetime: datetime):
        self.summary_dao_instance.get_by_participant_id.return_value = ParticipantSummary(
            consentForStudyEnrollmentFirstYesAuthored=consent_datetime
        )

