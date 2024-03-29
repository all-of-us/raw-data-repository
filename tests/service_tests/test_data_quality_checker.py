from datetime import datetime, timedelta
import mock

from rdr_service.services.data_quality import DataQualityChecker
from tests.helpers.unittest_base import BaseTestCase


@mock.patch('rdr_service.services.data_quality.logging')
class DataQualityCheckerTest(BaseTestCase):
    def setUp(self, **kwargs) -> None:
        super(DataQualityCheckerTest, self).setUp(**kwargs)

        self.checker = DataQualityChecker(self.session)

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

        self.checker.run_data_quality_checks()

        mock_logging.error.assert_any_call(
            f'Response {response_authored_before_signup.questionnaireResponseId} authored at '
            f'{response_authored_before_signup.authored} but participant signed up at {participant.signUpTime}'
        )
        mock_logging.error.assert_any_call(
            f'Response {response_authored_in_the_future.questionnaireResponseId} authored with future date '
            f'of {response_authored_in_the_future.authored} (received at {response_authored_in_the_future.created})'
        )

    def test_response_fuzzy_future_check(self, mock_logging):
        participant = self.data_generator.create_database_participant(signUpTime=datetime(2020, 4, 10))
        now = datetime.now().replace(microsecond=0)

        response_authored_in_the_future = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            authored=now + timedelta(weeks=1),
            created=now
        )
        # Create another questionnaire response that shouldn't get logged
        self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            authored=now + timedelta(seconds=40),
            created=now
        )

        self.checker.run_data_quality_checks()
        mock_logging.error.assert_called_once_with(
            f'Response {response_authored_in_the_future.questionnaireResponseId} authored with future date '
            f'of {response_authored_in_the_future.authored} (received at {response_authored_in_the_future.created})'
        )

    def test_response_after_suspension(self, mock_logging):
        suspended_participant = self.data_generator.create_database_participant(
            signUpTime=datetime(2020, 4, 10),
            suspensionTime=datetime(2020, 8, 4)
        )
        now = datetime.now().replace(microsecond=0)

        module_code = self.data_generator.create_database_code(value='test_module')
        response_authored_after_suspension = self.data_generator.create_database_questionnaire_response(
            participantId=suspended_participant.participantId,
            authored=now,
            created=now
        )
        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=response_authored_after_suspension.questionnaireId,
            questionnaireVersion=response_authored_after_suspension.questionnaireVersion,
            codeId=module_code.codeId
        )

        self.checker.run_data_quality_checks()
        mock_logging.error.assert_any_call(
            f'Response {response_authored_after_suspension.questionnaireResponseId} '
            f'authored for suspended participant (module: test_module)'
        )

    def test_response_after_withdrawal(self, mock_logging):
        withdrawn_participant = self.data_generator.create_database_participant(
            signUpTime=datetime(2020, 4, 10),
            withdrawalAuthored=datetime(2020, 8, 4)
        )
        now = datetime.now().replace(microsecond=0)

        response_authored_after_withdraw = self.data_generator.create_database_questionnaire_response(
            participantId=withdrawn_participant.participantId,
            authored=now,
            created=now
        )

        self.checker.run_data_quality_checks()
        mock_logging.error.assert_any_call(
            f'Response {response_authored_after_withdraw.questionnaireResponseId} authored for withdrawn participant'
        )

    def test_response_before_release(self, mock_logging):
        """
        We should get alerted if a response was authored before the first questionnaire for that survey was released
        """
        participant = self.data_generator.create_database_participant(signUpTime=datetime(2020, 4, 10))
        now = datetime.now().replace(microsecond=0)

        # Generate a pair of questionnaires and a pair of responses. Both responses should be for the latest
        # questionnaire, but one should be authored before the first questionnaire was created.
        module_code_value = 'TEST_CODE'
        module_code = self.data_generator.create_database_code(value=module_code_value)
        first_questionnaire = self.data_generator.create_database_questionnaire_history(
            created=now - timedelta(weeks=10)
        )
        updated_questionnaire = self.data_generator.create_database_questionnaire_history(
            created=now - timedelta(weeks=2)
        )
        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=first_questionnaire.questionnaireId,
            questionnaireVersion=first_questionnaire.version,
            codeId=module_code.codeId
        )
        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=updated_questionnaire.questionnaireId,
            questionnaireVersion=updated_questionnaire.version,
            codeId=module_code.codeId
        )
        response_authored_before_release = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            questionnaireId=updated_questionnaire.questionnaireId,
            questionnaireVersion=updated_questionnaire.version,
            authored=now - timedelta(weeks=12)
        )
        self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            questionnaireId=updated_questionnaire.questionnaireId,
            questionnaireVersion=updated_questionnaire.version,
            authored=now - timedelta(weeks=8)
        )

        self.checker.run_data_quality_checks()
        mock_logging.error.assert_called_once_with(
            f'Response {response_authored_before_release.questionnaireResponseId} to {module_code_value} '
            f'authored before survey released'
        )

    def test_only_recent_responses_checked(self, mock_logging):
        """Make sure that the checks only apply to responses after the date given"""

        participant = self.data_generator.create_database_participant(signUpTime=datetime(2020, 4, 10))
        questionnaire = self.data_generator.create_database_questionnaire_history(
            questions=[self.data_generator._questionnaire_question()]
        )
        response_authored_before_signup = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            authored=participant.signUpTime - timedelta(weeks=5),
            created=participant.signUpTime,
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version
        )

        self.checker.run_data_quality_checks(for_data_since=response_authored_before_signup.created + timedelta(weeks=5))

        mock_logging.warning.assert_not_called()

    def test_questionnaire_question_check(self, mock_logging):
        """Check warning for questionnaires that have no questions"""
        questionnaire_with_no_questions = self.data_generator.create_database_questionnaire()
        self.checker.run_data_quality_checks()
        mock_logging.warning.assert_called_with(
            f'Questionnaire with id {questionnaire_with_no_questions.questionnaireId} and '
            f'version {questionnaire_with_no_questions.version} was found with no questions.'
        )

    def test_patient_status_checks(self, mock_logging):
        """Check warnings that PatientStatus was authored with a future date or before the participant's sign up"""
        participant = self.data_generator.create_database_participant(signUpTime=datetime(2020, 10, 31))
        status_with_future_authored_date = self.data_generator.create_database_patient_status(
            participantId=participant.participantId,
            authored=datetime.now() + timedelta(days=5)
        )
        status_authored_before_signup = self.data_generator.create_database_patient_status(
            participantId=participant.participantId,
            authored=participant.signUpTime - timedelta(weeks=2)
        )

        self.checker.run_data_quality_checks()

        mock_logging.warning.assert_has_calls([
            mock.call(
                f'PatientStatus {status_with_future_authored_date.id} was authored with a future date'
            ),
            mock.call(
                f'PatientStatus {status_authored_before_signup.id} was authored before the participant signed up'
            )
        ])

    def test_patient_status_date_fuzzing(self, mock_logging):
        """Check warnings are not logged for patient statuses authored close to the created or signup times"""
        participant = self.data_generator.create_database_participant(signUpTime=datetime(2020, 10, 31))
        self.data_generator.create_database_patient_status(
            participantId=participant.participantId,
            authored=participant.signUpTime - timedelta(minutes=37)
        )
        self.data_generator.create_database_patient_status(
            participantId=participant.participantId,
            authored=datetime.utcnow() + timedelta(minutes=48)
        )

        self.checker.run_data_quality_checks()
        mock_logging.warning.assert_not_called()

    def test_deceased_report_checks(self, mock_logging):
        """Check warnings that DeceasedReport was authored with a future date or before the participant's sign up"""
        participant = self.data_generator.create_database_participant(signUpTime=datetime(2019, 10, 31))
        report_authored_with_future_date = self.data_generator.create_database_deceased_report(
            participantId=participant.participantId,
            authored=datetime.now() + timedelta(weeks=3)
        )
        report_authored_before_signup = self.data_generator.create_database_deceased_report(
            participantId=participant.participantId,
            authored=participant.signUpTime - timedelta(weeks=5)
        )
        report_effective_after_authored = self.data_generator.create_database_deceased_report(
            participantId=participant.participantId,
            authored=datetime(2020, 10, 1),
            dateOfDeath=datetime(2020, 10, 31)
        )
        report_with_multiple_issues = self.data_generator.create_database_deceased_report(
            participantId=participant.participantId,
            authored=participant.signUpTime - timedelta(weeks=5),
            dateOfDeath=participant.signUpTime
        )

        self.checker.run_data_quality_checks()

        mock_logging.warning.assert_has_calls([
            mock.call(
                f'Issues found with DeceasedReport {report_authored_with_future_date.id}: '
                f'was authored with a future date'
            ),
            mock.call(
                f'Issues found with DeceasedReport {report_authored_before_signup.id}: '
                f'was authored before participant signup'
            ),
            mock.call(
                f'Issues found with DeceasedReport {report_effective_after_authored.id}: '
                f'has an effective date after the authored date'
            ),
            mock.call(
                f'Issues found with DeceasedReport {report_with_multiple_issues.id}: '
                f'was authored before participant signup, has an effective date after the authored date'
            ),
        ])
