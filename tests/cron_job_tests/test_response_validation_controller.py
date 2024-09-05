from datetime import datetime, timedelta
import mock

from rdr_service import clock, code_constants
from rdr_service.offline.response_validation import ResponseValidationController
from rdr_service.model.ppi_validation_result import PpiValidationResults
from rdr_service.model.survey import SurveyQuestionType
from tests.helpers.unittest_base import BaseTestCase


class ResponseValidationControllerTest(BaseTestCase):
    def setUp(self, *args, **kwargs) -> None:
        super().setUp(*args, **kwargs)
        self.data_generator.create_database_code(value=code_constants.PMI_SKIP_CODE)

    def test_simple_case(self):
        start_date = datetime(2024, 9, 1)

        first_questionnaire, first_question = self._generate_questionnaire('first')
        second_questionnaire, second_question = self._generate_questionnaire('another')

        first_invalid_response = self._generate_response(
            created=start_date + timedelta(weeks=1),
            questionnaire=first_questionnaire,
            question=first_question,
            answer=2
        )
        second_invalid_response = self._generate_response(
            created=start_date + timedelta(weeks=2),
            questionnaire=second_questionnaire,
            question=second_question,
            answer=8
        )
        successful_response = self._generate_response(
            created=start_date + timedelta(weeks=2),
            questionnaire=second_questionnaire,
            question=second_question,
            answer=80
        )

        controller = ResponseValidationController(
            session=self.session,
            validation_errors_dao=mock.MagicMock(),
            since_date=start_date,
            slack_webhook={}
        )
        controller.run_validation()
        self.session.commit()

        results = self.session.query(PpiValidationResults).all()
        result_map = {result.questionnaire_response_id: result for result in results}
        self.assertEqual(
            [
                first_invalid_response.questionnaireResponseId,
                second_invalid_response.questionnaireResponseId,
                successful_response.questionnaireResponseId
            ],
            list(result_map.keys())
        )
        self.assertEqual(1, len(result_map[first_invalid_response.questionnaireResponseId].errors))
        self.assertEqual(1, len(result_map[second_invalid_response.questionnaireResponseId].errors))
        self.assertEqual(0, len(result_map[successful_response.questionnaireResponseId].errors))

    def test_update_results(self):
        start_date = datetime(2024, 9, 1)

        first_questionnaire, first_question = self._generate_questionnaire('first')

        self._generate_response(
            created=start_date + timedelta(weeks=1),
            questionnaire=first_questionnaire,
            question=first_question,
            answer=2
        )

        controller = ResponseValidationController(
            session=self.session,
            validation_errors_dao=mock.MagicMock(),
            since_date=start_date,
            slack_webhook={}
        )
        first_run_time = datetime(2021, 10, 1)
        with clock.FakeClock(first_run_time):
            controller.run_validation()
            self.session.commit()

        # re-run to update validation results
        second_run_time = datetime(2021, 10, 15)
        with clock.FakeClock(second_run_time):
            controller.run_validation()
            self.session.commit()

        results = self.session.query(PpiValidationResults).all()
        obsolete_result = results[0]
        self.assertEqual(second_run_time, obsolete_result.obsoletion_timestamp)
        self.assertEqual('replaced by revalidation', obsolete_result.obsoletion_reason)
        newer_result = results[1]
        self.assertIsNone(newer_result.obsoletion_timestamp)
        self.assertIsNone(newer_result.obsoletion_reason)

    def _generate_questionnaire(self, code_value_str):
        questionnaire = self.data_generator.create_database_questionnaire_history(
            version='1'
        )
        module_code = self.data_generator.create_database_code(
            value=code_value_str
        )
        self.data_generator.create_database_questionnaire_concept(
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            codeId=module_code.codeId
        )
        question = self.data_generator.create_database_questionnaire_question(
            questionnaireId=questionnaire.questionnaireId
        )
        survey = self.data_generator.create_database_survey(
            code=module_code,
            redcapProjectId=self.fake.pyint()
        )
        self.data_generator.create_database_survey_question(
            survey=survey,
            code=question.code,
            questionType=SurveyQuestionType.TEXT,
            validation='integer',
            validation_min=10
        )

        return questionnaire, question

    def _generate_response(self, created, questionnaire, question, answer):

        response = self.data_generator.create_database_questionnaire_response(
            created=created,
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version
        )
        return self.data_generator.create_database_questionnaire_response_answer(
            questionnaireResponseId=response.questionnaireResponseId,
            questionId=question.questionnaireQuestionId,
            valueInteger=answer
        )
