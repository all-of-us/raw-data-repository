from datetime import datetime, timedelta
import mock

from rdr_service.services.data_quality import DataQualityChecker
from tests.helpers.unittest_base import BaseTestCase


@mock.patch('rdr_service.services.data_quality.logging')
class DataQualityCheckerTest(BaseTestCase):
    def test_questionnaire_response_checks(self, mock_logging):
        participant = self.data_generator.create_database_participant(signUpTime=datetime(2020, 4, 10))
        response_authored_before_signup = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            authored=participant.signUpTime - timedelta(weeks=5)
        )

        now = datetime.now().replace(microsecond=0)
        response_authored_in_the_future = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            authored=now + timedelta(weeks=1),
            created=now
        )

        response_without_answers = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId
        )
        # None of the responses created have answers, but I'm relying on this one not getting flagged for anything else

        checker = DataQualityChecker(self.session)
        checker.run_data_quality_checks()

        mock_logging.warning.assert_has_calls([
            mock.call(
                f'Response {response_authored_before_signup.questionnaireResponseId} authored at '
                f'{response_authored_before_signup.authored} but participant signed up at {participant.signUpTime}'),
            mock.call(
                f'Response {response_authored_in_the_future.questionnaireResponseId} authored with future date '
                f'of {response_authored_in_the_future.authored} (received at {response_authored_in_the_future.created})'
            ),
            mock.call(
                f'Response {response_without_answers.questionnaireResponseId} has no answers'
            )
        ])

    def test_only_recent_responses_checked(self, mock_logging):
        """Make sure that the checks only apply to responses after the date given"""

        participant = self.data_generator.create_database_participant(signUpTime=datetime(2020, 4, 10))
        response_authored_before_signup = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            authored=participant.signUpTime - timedelta(weeks=5),
            created=participant.signUpTime
        )

        checker = DataQualityChecker(self.session)
        checker.run_data_quality_checks(for_data_since=response_authored_before_signup.created + timedelta(weeks=5))

        mock_logging.warning.assert_not_called()
