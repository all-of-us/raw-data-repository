from datetime import datetime

from rdr_service.dao.ppi_validation_errors_dao import PpiValidationErrorsDao
from rdr_service.model.ppi_validation_errors import PpiValidationErrors, ValidationErrorType
from tests.helpers.unittest_base import BaseTestCase


class PpiValidationErrorsDaoTest(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.ppi_validation_dao = PpiValidationErrorsDao()

        # Create test validation errors
        participant_1 = self.data_generator.create_database_participant()
        participant_2 = self.data_generator.create_database_participant()
        questionnaire_question = self.data_generator.create_database_questionnaire_question()
        code = self.data_generator.create_database_code()
        response = self.data_generator.create_database_questionnaire_response()
        response_answer = self.data_generator.create_database_questionnaire_response_answer(
            questionnaireResponseId=response.questionnaireResponseId,
            questionId=questionnaire_question.questionnaireQuestionId
        )

        self.error_1 = PpiValidationErrors(
            id=1,
            created=datetime(2023, 10, 1),
            survey_code_value="TheBasics",
            question_code="test_code",
            error_str="Error: Test error",
            error_type=ValidationErrorType.BRANCHING_ERROR,
            participant_id=participant_1.participantId,
            survey_code_id=code.codeId,
            questionnaire_response_id=response.questionnaireResponseId,
            questionnaire_response_answer_id=response_answer.questionnaireResponseAnswerId
        )
        self.error_2 = PpiValidationErrors(
            id=2,
            created=datetime(2023, 10, 2),
            survey_code_value="GROR",
            question_code="test_code",
            error_str="Error: Test error",
            error_type=ValidationErrorType.INVALID_DATA_TYPE,
            participant_id=participant_2.participantId,
            survey_code_id=code.codeId,
            questionnaire_response_id=response.questionnaireResponseId,
            questionnaire_response_answer_id=response_answer.questionnaireResponseAnswerId
        )
        self.error_3 = PpiValidationErrors(
            id=3,
            created=datetime(2023, 10, 4),
            survey_code_value="TestSurvey",
            question_code="test_code",
            error_str="Error: Test error",
            error_type=ValidationErrorType.INVALID_VALUE,
            participant_id=participant_1.participantId,
            survey_code_id=code.codeId,
            questionnaire_response_id=response.questionnaireResponseId,
            questionnaire_response_answer_id=response_answer.questionnaireResponseAnswerId
        )

    def test_get_before_insert(self):
        self.assertIsNone(self.ppi_validation_dao.get(1))

    def test_insert_validation_errors(self):
        # Insert errors in table
        self.session.add(self.error_1)
        self.session.add(self.error_2)
        self.session.add(self.error_3)
        self.session.commit()

        self.assertEqual("TheBasics", self.ppi_validation_dao.get(1).survey_code_value)
        self.assertEqual("GROR", self.ppi_validation_dao.get(2).survey_code_value)
        self.assertEqual("TestSurvey", self.ppi_validation_dao.get(3).survey_code_value)
        self.assertEqual(ValidationErrorType.BRANCHING_ERROR, self.ppi_validation_dao.get(1).error_type)
        self.assertEqual(ValidationErrorType.INVALID_DATA_TYPE, self.ppi_validation_dao.get(2).error_type)
        self.assertEqual(ValidationErrorType.INVALID_VALUE, self.ppi_validation_dao.get(3).error_type)

    def test_get_errors_since(self):
        # Insert errors in table
        self.session.add(self.error_1)
        self.session.add(self.error_2)
        self.session.add(self.error_3)
        self.session.commit()

        since_date_none = datetime(2023, 10, 10)
        since_date_some = datetime(2023, 10, 2)
        since_date_all = datetime(2023, 10, 1)

        error_list_none = self.ppi_validation_dao.get_errors_since(since_date_none)
        error_list_some = self.ppi_validation_dao.get_errors_since(since_date_some)
        error_list_all = self.ppi_validation_dao.get_errors_since(since_date_all)

        self.assertEqual(0, len(error_list_none))
        self.assertEqual(2, len(error_list_some))
        self.assertEqual(3, len(error_list_all))

    def test_get_errors_within_range(self):
        # Insert errors in table
        self.session.add(self.error_1)
        self.session.add(self.error_2)
        self.session.add(self.error_3)
        self.session.commit()

        since_date_none = datetime(2023, 9, 1)
        since_date_some = datetime(2023, 10, 3)
        since_date_all = datetime(2023, 9, 1)

        end_date_none = datetime(2023, 9, 30)
        end_date_some = datetime(2023, 10, 10)
        end_date_all = datetime(2023, 10, 10)

        error_list_none = self.ppi_validation_dao.get_errors_within_range(since_date_none, end_date_none)
        error_list_some = self.ppi_validation_dao.get_errors_within_range(since_date_some, end_date_some)
        error_list_all = self.ppi_validation_dao.get_errors_within_range(since_date_all, end_date_all)

        self.assertEqual(0, len(error_list_none))
        self.assertEqual(1, len(error_list_some))
        self.assertEqual(3, len(error_list_all))
