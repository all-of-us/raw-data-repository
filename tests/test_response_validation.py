from datetime import datetime
from typing import Dict, List, Tuple

from rdr_service.model.questionnaire_response import QuestionnaireResponseStatus
from rdr_service.domain_model.response import Answer, Response
from rdr_service.services.response_validation.validation import And, CanOnlyBeAnsweredIf, ResponseRequirements, \
    InAnySurvey, Or, Question, ValidationError
from tests.helpers.unittest_base import BaseTestCase


class TestValidation(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(TestValidation, self).__init__(*args, **kwargs)
        self.uses_database = False

    def test_question_answer_dependency(self):
        """Check that we can validate a need for a specific answer to another question"""

        # define a rule that says question_b should only be answered if question_a was answered with ans_1
        response_validation = ResponseRequirements({
            'question_b': CanOnlyBeAnsweredIf(
                Question('question_a').is_answered_with('ans_1')
            )
        })

        response_not_answering_a = self._build_response({
            'question_b': [(3, 'anything')]
        })
        self.assertEqual(
            [ValidationError(answer_id=[3], question_code='question_b')],
            response_validation.check_for_errors(response_not_answering_a)
        )

        response_wrong_answer_to_a = self._build_response({
            'question_b': [(4, 'anything')],
            'question_a': [(5, 'something')]
        })
        self.assertEqual(
            [ValidationError(answer_id=[4], question_code='question_b')],
            response_validation.check_for_errors(response_wrong_answer_to_a)
        )

        valid_response = self._build_response({
            'question_a': [(3, 'ans_1')],
            'question_b': [(3, 'anything')]
        })
        self.assertEqual(
            [],
            response_validation.check_for_errors(valid_response)
        )

    def test_and_conditions(self):
        """Verify that AND-ing conditions requires that they all pass"""
        response_validation = ResponseRequirements({
            'question_d': CanOnlyBeAnsweredIf(
                And([
                    Question('question_a').is_answered_with('ans_a'),
                    Question('question_b').is_answered_with('ans_b'),
                    Question('question_c').is_answered_with('ans_c')
                ])
            )
        })

        response_skipped_a = self._build_response({
            'question_b': [(3, 'ans_b')],
            'question_c': [(4, 'ans_c')],
            'question_d': [(5, 'ans_d')]
        })
        self.assertEqual(
            [ValidationError(answer_id=[5], question_code='question_d')],
            response_validation.check_for_errors(response_skipped_a)
        )

        response_wrong_answer_a = self._build_response({
            'question_a': [(2, 'something')],
            'question_b': [(3, 'ans_b')],
            'question_c': [(4, 'ans_c')],
            'question_d': [(5, 'ans_d')]
        })
        self.assertEqual(
            [ValidationError(answer_id=[5], question_code='question_d')],
            response_validation.check_for_errors(response_wrong_answer_a)
        )

        valid_response = self._build_response({
            'question_a': [(2, 'ans_a')],
            'question_b': [(3, 'ans_b')],
            'question_c': [(4, 'ans_c')],
            'question_d': [(5, 'ans_d')]
        })
        self.assertEqual(
            [],
            response_validation.check_for_errors(valid_response)
        )

    def test_or_conditions(self):
        """Verify that OR-ing conditions only requires that one passes"""
        response_validation = ResponseRequirements({
            'question_d': CanOnlyBeAnsweredIf(
                Or([
                    Question('question_a').is_answered_with('ans_a'),
                    Question('question_b').is_answered_with('ans_b'),
                    Question('question_c').is_answered_with('ans_c')
                ])
            )
        })

        valid_only_b = self._build_response({
            'question_b': [(3, 'ans_b')],
            'question_d': [(5, 'ans_d')]
        })
        self.assertEqual(
            [],
            response_validation.check_for_errors(valid_only_b)
        )

        response_wrong_answer_all = self._build_response({
            'question_a': [(2, 'something')],
            'question_b': [(3, 'something')],
            'question_c': [(4, 'something')],
            'question_d': [(5, 'ans_d')]
        })
        self.assertEqual(
            [ValidationError(answer_id=[5], question_code='question_d')],
            response_validation.check_for_errors(response_wrong_answer_all)
        )

        valid_response_to_c = self._build_response({
            'question_a': [(2, 'something')],
            'question_b': [(3, 'something')],
            'question_c': [(4, 'ans_c')],
            'question_d': [(5, 'ans_d')]
        })
        self.assertEqual(
            [],
            response_validation.check_for_errors(valid_response_to_c)
        )

    def test_cross_survey_dependency(self):
        """Check that a question from another survey was answered"""

        # define a rule that says question_b should only be answered if question_a was answered
        # with ans_1 in any previous survey
        response_validation = ResponseRequirements({
            'question_b': CanOnlyBeAnsweredIf(
                InAnySurvey(
                    Question('question_a').is_answered_with('ans_1')
                )
            )
        })

        # Check that an answer to question_b with no previous answer to question_a gives an error
        response_to_b = self._build_response({
            'question_b': [(4, 'anything')]
        })
        self.assertEqual(
            [ValidationError(answer_id=[4], question_code='question_b')],
            response_validation.check_for_errors(response_to_b)
        )

        # give a response with the wrong answer to question_a
        response_validation.check_for_errors(self._build_response({
            'question_a': [(5, 'something')]
        }))

        # Check that an answer to question_b with previous incorrect answer to question_a gives an error
        response_to_b = self._build_response({
            'question_b': [(8, 'anything')]
        })
        self.assertEqual(
            [ValidationError(answer_id=[8], question_code='question_b')],
            response_validation.check_for_errors(response_to_b)
        )

        # give a valid answer to question_a
        response_validation.check_for_errors(self._build_response({
            'question_a': [(5, 'ans_1')]
        }))

        # Check that an answer to question_b with previous valid answer to question_a passes
        response_to_b = self._build_response({
            'question_b': [(12, 'anything')]
        })
        self.assertEqual(
            [],
            response_validation.check_for_errors(response_to_b)
        )

    def test_conditional_from_branching_logic(self):
        """Check that we can build a condition based on Redcap's branching logic string"""
        # conditional = Condition.from_branching_logic(
        #     """[question_a] = 'ans_1' and [question_b(option_4)] = '1' and [question_amount] > 3"""
        # )
        # self.assertEqual(
        #     'question_a answer is ans_1 '
        #     'and question_b has option_4 selected '
        #     'and question_amount answer greater than 3',
        #     str(conditional)
        # )

        # another = Condition.from_branching_logic(
        #     """[cdc_covid_xx_b_firstdose] = 'cope_a_336' or ([cdc_covid_xx_b_firstdose] = 'cope_a_335' or
        #     [cdc_covid_xx_b_firstdose] = 'cope_a_330' or [cdc_covid_xx_b_firstdose] = 'cope_a_331' or
        #     [cdc_covid_xx_b_firstdose] = 'COPE_A_52' or [cdc_covid_xx_b_firstdose] = 'COPE_A_204') and
        #     ([cdc_covid_xx_b_seconddose] = 'cope_a_335' or [cdc_covid_xx_b_seconddose] = 'cope_a_330' or
        #     [cdc_covid_xx_b_seconddose] = 'cope_a_331' or [cdc_covid_xx_b_seconddose] = 'COPE_A_52' or
        #     [cdc_covid_xx_b_seconddose] = 'COPE_A_204')"""
        # )
        #
        # """ [cdc_covid_xx_b_firstdose] = 'cope_a_336'
        #     or (
        #         [cdc_covid_xx_b_firstdose] = 'cope_a_335'
        #         or [cdc_covid_xx_b_firstdose] = 'cope_a_330'
        #         or [cdc_covid_xx_b_firstdose] = 'cope_a_331'
        #         or [cdc_covid_xx_b_firstdose] = 'COPE_A_52'
        #         or [cdc_covid_xx_b_firstdose] = 'COPE_A_204'
        #     )
        #     and (
        #         [cdc_covid_xx_b_seconddose] = 'cope_a_335'
        #         or [cdc_covid_xx_b_seconddose] = 'cope_a_330'
        #         or [cdc_covid_xx_b_seconddose] = 'cope_a_331'
        #         or [cdc_covid_xx_b_seconddose] = 'COPE_A_52'
        #         or [cdc_covid_xx_b_seconddose] = 'COPE_A_204'
        #     )"""
        print('bob')

    @classmethod
    def _build_response(cls, answers: Dict[str, List[Tuple[int, str]]]):
        return Response(
            id=1,
            survey_code='test',
            authored_datetime=datetime.now(),
            status=QuestionnaireResponseStatus.COMPLETED,
            answered_codes={
                question_code: [Answer(ans_id, val) for ans_id, val in answer_list]
                for question_code, answer_list in answers.items()
            }
        )
