from datetime import datetime
import mock

from rdr_service.services.response_duplication_detector import ResponseDuplicationDetector
from tests.helpers.unittest_base import BaseTestCase


class ResponseDuplicationDetectorTests(BaseTestCase):
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
        duplicate_of_one = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            externalId=one.externalId,
            answerHash=one.answerHash,
            authored=one.authored,
            created=datetime(2021, 3, 2)
        )
        another_duplicate_of_one = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            externalId=one.externalId,
            answerHash=one.answerHash,
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

        from tests.helpers.diagnostics import LoggingDatabaseActivity
        with LoggingDatabaseActivity():
            ResponseDuplicationDetector.flag_duplicate_responses()

        # Reload responses in the current session so we can get the updated info on the isDuplicate field
        self.session.refresh(one)
        self.session.refresh(duplicate_of_one)
        self.session.refresh(another_duplicate_of_one)
        self.session.refresh(two)

        # Check that the isDuplicate flags were set correctly
        self.assertTrue(one.isDuplicate)
        self.assertTrue(duplicate_of_one.isDuplicate)
        self.assertFalse(another_duplicate_of_one.isDuplicate)
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
        another_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            externalId=first_response.externalId,
            answerHash=first_response.answerHash,
            created=datetime(2021, 3, 2)
        )
        new_response = self.data_generator.create_database_questionnaire_response(
            participantId=participant.participantId,
            externalId=first_response.externalId,
            answerHash=first_response.answerHash,
            created=datetime(2021, 3, 3)
        )

        ResponseDuplicationDetector.flag_duplicate_responses()

        # Check that only one response was marked as a duplicate
        mock_logging.warning.assert_called_with(
            f"['{another_response.questionnaireResponseId}'] "
            f"found as duplicates of {new_response.questionnaireResponseId}"
        )
