from datetime import datetime
import mock
from typing import Dict, List, Tuple

from rdr_service.model.questionnaire_response import QuestionnaireResponseStatus
from rdr_service.domain_model.response import Answer, DataType, Response
from rdr_service.services.response_validation.validation import And, CanOnlyBeAnsweredIf, ResponseRequirements, \
    InAnySurvey, Or, Question, ValidationError, Condition
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
            [ValidationError(answer_id=[3], question_code='question_b', reason=mock.ANY)],
            response_validation.check_for_errors(response_not_answering_a)
        )

        response_wrong_answer_to_a = self._build_response({
            'question_b': [(4, 'anything')],
            'question_a': [(5, 'something')]
        })
        self.assertEqual(
            [ValidationError(answer_id=[4], question_code='question_b', reason=mock.ANY)],
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

    def test_invalidation_chaining(self):
        """If an answer gets marked as invalid, any answer that relied on it need to be invalidated too"""

        # define a rule that says question_b should only be answered if question_a was answered with ans_1
        response_validation = ResponseRequirements({
            'question_c': CanOnlyBeAnsweredIf(
                Question('question_b').is_answered_with('anything')
            ),
            'question_b': CanOnlyBeAnsweredIf(
                Question('question_a').is_answered_with('ans_1')
            )
        })

        response_without_answer_to_a = self._build_response({
            'question_c': [(4, 'test')],
            'question_b': [(3, 'anything')]
        })
        actual = response_validation.check_for_errors(response_without_answer_to_a)
        self.assertEqual(
            [
                ValidationError(answer_id=[3], question_code='question_b', reason=mock.ANY),
                ValidationError(answer_id=[4], question_code='question_c', reason=mock.ANY),
            ],
            actual
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
            [ValidationError(answer_id=[5], question_code='question_d', reason=mock.ANY)],
            response_validation.check_for_errors(response_skipped_a)
        )

        response_wrong_answer_a = self._build_response({
            'question_a': [(2, 'something')],
            'question_b': [(3, 'ans_b')],
            'question_c': [(4, 'ans_c')],
            'question_d': [(5, 'ans_d')]
        })
        self.assertEqual(
            [ValidationError(answer_id=[5], question_code='question_d', reason=mock.ANY)],
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
            [ValidationError(answer_id=[5], question_code='question_d', reason=mock.ANY)],
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
            [ValidationError(answer_id=[4], question_code='question_b', reason=mock.ANY)],
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
            [ValidationError(answer_id=[8], question_code='question_b', reason=mock.ANY)],
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

    @classmethod
    def _build_response(cls, answers: Dict[str, List[Tuple[int, str]]]):
        return Response(
            id=1,
            survey_code='test',
            authored_datetime=datetime.now(),
            status=QuestionnaireResponseStatus.COMPLETED,
            answered_codes={
                question_code: [Answer(ans_id, val, DataType.STRING) for ans_id, val in answer_list]
                for question_code, answer_list in answers.items()
            }
        )


class TestConditionalFromBranchingLogic(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(TestConditionalFromBranchingLogic, self).__init__(*args, **kwargs)
        self.uses_database = False

    def test_code_option_selection(self):
        branching_logic = "[a] = 'a1'"
        result = Condition.from_branching_logic(branching_logic)
        self.assertEqual(branching_logic, str(result))

    def test_answer_greater_than(self):
        branching_logic = "[a] > 7"
        result = Condition.from_branching_logic(branching_logic)
        self.assertEqual(branching_logic, str(result))

    def test_answer_less_than(self):
        branching_logic = "[a] < 7"
        result = Condition.from_branching_logic(branching_logic)
        self.assertEqual(branching_logic, str(result))

    def test_answer_not_equal(self):
        branching_logic = "[a] <> 7"
        result = Condition.from_branching_logic(branching_logic)
        self.assertEqual(branching_logic, str(result))

        result.process_response(
            self._build_response({
                'a': (7, DataType.INTEGER)
            }))
        self.assertFalse(result.passes())

        result.process_response(
            self._build_response({
                'a': (19, DataType.INTEGER)
            })
        )
        self.assertTrue(result.passes())

    def test_number_comparison_with_quotes(self):
        branching_logic = "[a] > '7'"
        result = Condition.from_branching_logic(branching_logic)
        self.assertEqual('[a] > 7', str(result))

    def test_checkbox_constraint(self):
        branching_logic = "[a(option_1)] = '1'"
        result = Condition.from_branching_logic(branching_logic)
        self.assertEqual(branching_logic, str(result))

    def test_simple_top_level_conditional(self):
        branching_logic = "[a] = 'a1' and [b] > 0"
        result = Condition.from_branching_logic(branching_logic)
        self.assertEqual(f'({branching_logic})', str(result))

    def test_complex_top_level_conditional(self):
        branching_logic = "[a] = 'a1' and [b] > 0 or [c] = 'a2'"
        result = Condition.from_branching_logic(branching_logic)
        self.assertEqual("([a] = 'a1' and ([b] > 0 or [c] = 'a2'))", str(result))

    def test_nested_conditional(self):
        branching_logic = "[a] = 'a1' and ([b] > 0 or [c(option_1)] = '1' and [d] > 5)"
        result = Condition.from_branching_logic(branching_logic)
        self.assertEqual("([a] = 'a1' and (([b] > 0 or [c(option_1)] = '1') and [d] > 5))", str(result))

    @classmethod
    def _build_response(cls, answer_dict):
        return Response(
            answered_codes={
                question_code: [Answer(id=1, value=str(answer_value), data_type=data_type)]
                for question_code, (answer_value, data_type) in answer_dict.items()
            },
            authored_datetime=datetime.now(),
            id=8,
            status=QuestionnaireResponseStatus.COMPLETED,
            survey_code='test'
        )
