from datetime import datetime
import mock

from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.services.response_duplication_detector import ResponseDuplicationDetector
from tests.helpers.unittest_base import BaseTestCase


class ResponseDuplicationDetectorTests(BaseTestCase):
    def _make_duplicate_of(self, response: QuestionnaireResponse, **kwargs):
        response_params = {
            'participantId': response.participantId,
            'externalId': response.externalId,
            'answerHash': response.answerHash,
            'authored': response.authored
        }
        response_params.update(**kwargs)
        return self.data_generator.create_database_questionnaire_response(**response_params)

    def test_duplicates_are_marked(self):
        """Any responses found to be a duplicate of another should have isDuplicate set (except the most recent one)"""

        # Create some responses, some that are duplicates of each other
        participant = self.data_generator.create_database_participant()
        one = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            externalId='one',
            answerHash='badbeef',
            authored=datetime(2021, 2, 1),
            created=datetime(2021, 3, 1)
        )
        duplicate_of_one = self._make_duplicate_of(
            response=one,
            created=datetime(2021, 3, 2)
        )
        last_duplicate_of_one = self._make_duplicate_of(
            response=one,
            authored=datetime.now(),  # Sometimes the authored date can shift when duplications happen,
            created=datetime(2021, 3, 3)
        )
        # Some repetitions should be accepted, such as updates to contact information through the ConsentPII.
        # These updates will come as responses that share the identifier of the original, but have different answers.
        two = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            externalId=one.externalId,
            answerHash='0ddba11',
            authored=datetime.now(),
            created=datetime(2021, 3, 4)
        )

        detector = ResponseDuplicationDetector(duplication_threshold=2)
        detector.flag_duplicate_responses()

        # Reload responses in the current session so we can get the updated info on the isDuplicate field
        self.session.refresh(one)
        self.session.refresh(duplicate_of_one)
        self.session.refresh(last_duplicate_of_one)
        self.session.refresh(two)

        # Check that the isDuplicate flags were set correctly
        self.assertTrue(one.isDuplicate)
        self.assertTrue(duplicate_of_one.isDuplicate)
        self.assertFalse(last_duplicate_of_one.isDuplicate)
        self.assertFalse(two.isDuplicate)

    @mock.patch('rdr_service.services.response_duplication_detector.logging')
    def test_duplicates_are_not_reprocessed(self, mock_logging):
        """Responses that are already known to be duplicates shouldn't be updated every time"""

        participant = self.data_generator.create_database_participant()
        first_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            externalId='one',
            answerHash='badbeef',
            created=datetime(2021, 3, 1),
            isDuplicate=True
        )
        another_response = self._make_duplicate_of(
            response=first_response,
            created=datetime(2021, 3, 2)
        )
        new_response = self._make_duplicate_of(
            response=first_response,
            created=datetime(2021, 3, 3)
        )

        detector = ResponseDuplicationDetector(duplication_threshold=2)
        detector.flag_duplicate_responses()

        # Check that only one response was marked as a duplicate
        mock_logging.warning.assert_called_with(
            f"['{another_response.questionnaireResponseId}'] "
            f"found as duplicates of {new_response.questionnaireResponseId}"
        )

    @mock.patch('rdr_service.services.response_duplication_detector.logging')
    def test_detection_threshold(self, mock_logging):
        """Responses should only be marked as duplicates if there are a specific number of them"""

        # Create a detector that will only mark responses as duplicates if there are 8 of them
        detector = ResponseDuplicationDetector(duplication_threshold=8)

        # Create a response and 6 more instances of it (totalling 7 duplicates)
        participant = self.data_generator.create_database_participant()
        first_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            externalId='one',
            answerHash='badbeef',
            created=datetime(2021, 3, 1)
        )
        self._make_duplicate_of(response=first_response, created=datetime(2021, 3, 2))
        self._make_duplicate_of(response=first_response, created=datetime(2021, 3, 3))
        self._make_duplicate_of(response=first_response, created=datetime(2021, 3, 4))
        self._make_duplicate_of(response=first_response, created=datetime(2021, 3, 5))
        self._make_duplicate_of(response=first_response, created=datetime(2021, 3, 6))
        self._make_duplicate_of(response=first_response, created=datetime(2021, 3, 7))

        # Check that nothing is marked as a duplicate of anything else
        detector.flag_duplicate_responses()
        duplicate_response = self.session.query(QuestionnaireResponse).filter(
            QuestionnaireResponse.isDuplicate.is_(True)
        ).first()
        self.assertIsNone(duplicate_response)

        # Make one more duplicate and make sure the duplication is found
        latest_duplicate = self._make_duplicate_of(response=first_response, created=datetime(2021, 3, 8))
        detector.flag_duplicate_responses()
        warning_log = mock_logging.warning.call_args[0][0]
        self.assertTrue(warning_log.endswith(f'] found as duplicates of {latest_duplicate.questionnaireResponseId}'))

    def test_checking_recent_responses(self):
        """
        Make sure that only recent responses are loaded
        (to avoid unnecessary processing that causes slow_sql alerts)
        """

        # Create some responses, some that are duplicates of each other
        participant = self.data_generator.create_database_participant()
        one = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            externalId='one',
            answerHash='badbeef',
            authored=datetime(2020, 2, 1),
            created=datetime(2020, 3, 1)
        )
        self._make_duplicate_of(
            response=one,
            created=datetime(2020, 3, 2)
        )

        detector = ResponseDuplicationDetector()
        from tests.helpers.diagnostics import LoggingDatabaseActivity
        with LoggingDatabaseActivity():
            responses_detected = detector._get_duplicate_responses(self.session, datetime(2021, 1, 1))

        self.assertEmpty(responses_detected)
