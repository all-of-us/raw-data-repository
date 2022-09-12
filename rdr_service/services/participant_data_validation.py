from datetime import datetime
import logging

from dateutil.relativedelta import relativedelta

from rdr_service import config
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.slack_utils import SlackMessageHandler


class ParticipantDataValidation:
    @classmethod
    def analyze_date_of_birth(
        cls,
        participant_id: int,
        date_of_birth: datetime
    ):
        is_age_at_consent_valid = cls._is_age_at_consent_valid(
            date_of_birth=date_of_birth,
            participant_id=participant_id
        )
        is_current_age_valid = cls._is_current_age_valid(
            date_of_birth=date_of_birth,
            participant_id=participant_id
        )

        if not (is_age_at_consent_valid and is_current_age_valid):
            cls._send_notification()

    @classmethod
    def _is_current_age_valid(cls, date_of_birth: datetime, participant_id: int):
        current_age_years = relativedelta(datetime.utcnow(), date_of_birth).years
        if cls._is_outside_expected_bounds(current_age_years):
            logging.warning(
                f'Unexpected date of birth for P{participant_id}: '
                f'date of birth means current age is invalid for program'
            )
            return False

        return True

    @classmethod
    def _is_age_at_consent_valid(cls, date_of_birth: datetime, participant_id: int):
        summary_dao = ParticipantSummaryDao()
        participant_summary: ParticipantSummary = summary_dao.get_by_participant_id(participant_id)

        if not participant_summary:
            logging.error(f'Unable to find participant summary for {participant_id}')
            return True

        age_at_consent_years = relativedelta(
            participant_summary.consentForStudyEnrollmentFirstYesAuthored,
            date_of_birth
        ).years
        if cls._is_outside_expected_bounds(age_at_consent_years):
            logging.warning(
                f'Unexpected date of birth for P{participant_id}: '
                f'date of birth means age at consent was invalid for program'
            )
            return False

        return True

    @classmethod
    def _is_outside_expected_bounds(cls, age_years: int):
        return age_years < 18 or age_years > 125

    @classmethod
    def _send_notification(cls):
        validation_webhook = cls._get_slack_webhook_url()
        if validation_webhook is None:
            logging.warning('Webhook not found. Skipping slack notification for validation error.')

        handler = SlackMessageHandler(webhook_url=validation_webhook)
        handler.send_message_to_webhook(message_data={
            'text': 'Invalid date of birth detected'
        })

    @classmethod
    def _get_slack_webhook_url(cls):
        webhook_config = config.getSettingJson(config.RDR_SLACK_WEBHOOKS, None)
        if webhook_config is None:
            return None

        return webhook_config.get(config.RDR_VALIDATION_WEBHOOK)
