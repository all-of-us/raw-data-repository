from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from enum import auto, Enum
from typing import Dict, List, Optional, Sequence

from dateutil.parser import parse
from sqlalchemy.orm import Session

from rdr_service.code_constants import PMI_SKIP_CODE
from rdr_service.domain_model import response as response_domain_model
from rdr_service.model.code import Code
from rdr_service.model.survey import Survey, SurveyQuestion, SurveyQuestionType

class _Requirement(ABC):
    """
    Base class for a logical constraint on a question within a survey.
    Responsible for determining if a validation error has been found for an an answer to a specific
    question in a survey.
    """

    @abstractmethod
    def get_errors(self, response: response_domain_model.Response, question_code: str) -> Sequence['ValidationError']:
        """
        Check for answers to the given question code.
        Returns a validation error for answers to the question if a validation error has been found.
        """
        ...

    @abstractmethod
    def reset_state(self):
        ...


class Condition(ABC):
    """
    Analyzes questionnaire response data, recording when a specific condition passes
    """

    @abstractmethod
    def passes(self) -> bool:
        """Returns whether the defined condition passes."""
        ...

    @abstractmethod
    def process_response(self, response: response_domain_model):
        """
        Analyze the response data to determine if the response gets the condition to pass.
        Note: this method is a distinct step from returning if this condition passes so that all conditions are
        able to analyze the response and resolve dependencies between each other (if there are any).
        """
        ...

    @classmethod
    def from_branching_logic(cls, branching_logic):
        parser = _BranchingLogicParser()
        for char in branching_logic:
            parser.process_char(char)
        return parser.get_resulting_conditional()

    @abstractmethod
    def reset_state(self):
        ...


class CanOnlyBeAnsweredIf(_Requirement):
    """
    Requires that a condition passes,
    returns any answers to the given question as validation errors otherwise.
    """

    def __init__(self, condition: Condition):
        self.condition = condition

    def get_errors(self, response: response_domain_model.Response, question_code: str) -> Sequence['ValidationError']:
        self.condition.process_response(response)

        answers = response.get_answers_for(question_code)
        if answers and not self.condition.passes():
            for answer in answers:
                answer.is_valid = False
            return [ValidationError(question_code, [answer.id for answer in answers], reason='branching error')]

        return []

    def reset_state(self):
        self.condition.reset_state()

    def __str__(self):
        return str(self.condition)


class Question:
    """
    Condition-builder class to help a bit with readability when defining conditions.
    Creates expected Condition instances that target a specific question.
    """

    def __init__(self, question_code):
        self.question_code = question_code

    def is_answered_with(self, answer_value) -> Condition:
        return _HasAnsweredQuestionWith(
            question_code=self.question_code,
            comparison=_Comparison.EQUALS,
            answer_value=answer_value
        )

    def answer_greater_than(self, value) -> Condition:
        return _HasAnsweredQuestionWith(
            question_code=self.question_code,
            comparison=_Comparison.GREATER_THAN,
            answer_value=value
        )

    def has_option_selected(self, option_value) -> Condition:
        return _HasSelectedOption(
            question_code=self.question_code,
            answer_value=option_value
        )


class _Comparison(Enum):
    """Comparison enum to allow for multiple types of equality checks"""

    EQUALS = auto()
    GREATER_THAN = auto()


class InAnySurvey(Condition):
    """Condition wrapper that 'remembers' if a condition has passed for any response"""

    def __init__(self, condition: Condition):
        self._condition_found_passing = False
        self._condition = condition

    def reset_state(self):
        self._condition_found_passing = False
        self._condition.reset_state()

    def passes(self) -> bool:
        return self._condition_found_passing

    def process_response(self, response: response_domain_model):
        self._condition.process_response(response)
        is_passing = self._condition.passes()
        self._condition_found_passing = self._condition_found_passing or is_passing

    def __str__(self):
        return f'in any survey ({self._condition})'


class InAnyPreviousSurvey(InAnySurvey):
    """
    'Remembers' if a condition has passed for any previous response
    (excluding the response that is currently being processed)
    """
    def __init__(self, *args, **kwargs):
        super(InAnyPreviousSurvey, self).__init__(*args, **kwargs)
        self._passed_in_last_survey = False

    def reset_state(self):
        super(InAnyPreviousSurvey, self).reset_state()
        self._passed_in_last_survey = False

    def process_response(self, response: response_domain_model):
        # Now processing a new response, if the condition passed in the last response
        #   (or any before that) set the flag now
        self._condition_found_passing = self._condition_found_passing or self._passed_in_last_survey

        # Check the current response and record if the condition passes so we'll know for future responses
        self._condition.process_response(response)
        self._passed_in_last_survey = self._condition.passes()

    def __str__(self):
        return f'in previous survey ({self._condition})'


class Not(Condition):
    """
    Condition wrapper that will result in a failure if the condition provided passes,
    essentially this will act as a "not" boolean operation
    """

    def __init__(self, condition: Condition):
        self._condition = condition

    def reset_state(self):
        self._condition.reset_state()

    def passes(self) -> bool:
        return not self._condition.passes()

    def process_response(self, response: response_domain_model):
        self._condition.process_response(response)

    def __str__(self):
        return f'not ({self._condition})'


class And(Condition):
    """Condition wrapper that checks if all supplied conditions pass"""

    def __init__(self, child_conditions: Sequence[Condition]):
        self._child_conditions = child_conditions

    def reset_state(self):
        for child in self._child_conditions:
            child.reset_state()

    def passes(self) -> bool:
        return all([child.passes() for child in self._child_conditions])

    def process_response(self, response: response_domain_model):
        for child in self._child_conditions:
            child.process_response(response)

    def __str__(self):
        result = ' and '.join([str(condition) for condition in self._child_conditions])
        return f'({result})'


class Or(Condition):
    """Condition wrapper that checks if any supplied conditions pass"""

    def __init__(self, child_conditions: Sequence[Condition]):
        self._child_conditions = child_conditions

    def reset_state(self):
        for child in self._child_conditions:
            child.reset_state()

    def passes(self) -> bool:
        return any([child.passes() for child in self._child_conditions])

    def process_response(self, response: response_domain_model):
        for child in self._child_conditions:
            child.process_response(response)

    def __str__(self):
        result = ' or '.join([str(condition) for condition in self._child_conditions])
        return f'({result})'


class _HasAnsweredQuestionWith(Condition):
    """
    Condition that checks that the answer to a question matches an expected value.
    Note: This class is meant to expect only one answer to the question.
    """

    def __init__(self, question_code: str, comparison: _Comparison, answer_value: str):
        self.comparison = comparison
        self.question_code = question_code
        self.expected_answer_value = answer_value

        self._passes = False

    def reset_state(self):
        self._passes = False

    def passes(self) -> bool:
        return self._passes

    def process_response(self, response: response_domain_model):
        answer = response.get_single_answer_for(self.question_code)

        if self.comparison == _Comparison.EQUALS:
            self._passes = answer and answer.value == self.expected_answer_value
        elif self.comparison == _Comparison.GREATER_THAN:
            self._passes = answer and answer.value > self.expected_answer_value

    def __str__(self):
        if self.comparison == _Comparison.GREATER_THAN:
            return f"[{self.question_code}] > {self.expected_answer_value}"
        else:
            return f"[{self.question_code}] = '{self.expected_answer_value}'"


class _HasSelectedOption(Condition):
    def __init__(self, question_code: str, answer_value: str):
        self.question_code = question_code
        self.expected_selection = answer_value

        self._passes = False

    def reset_state(self):
        self._passes = False

    def passes(self) -> bool:
        return self._passes

    def process_response(self, response: response_domain_model):
        answers = response.get_answers_for(self.question_code)
        self._passes = answers and self.expected_selection in [answer.value for answer in answers]

    def __str__(self):
        return f"[{self.question_code}({self.expected_selection})] = '1'"


@dataclass
class ValidationError:
    question_code: str
    answer_id: Sequence[int]
    reason: str


class ResponseRequirements:
    def __init__(self, requirements: Dict[str, _Requirement]):
        self.requirements = requirements
        self._responses_to_replay = []

    def check_for_errors(self, response: response_domain_model.Response) -> Sequence[ValidationError]:
        errors = self._find_errors(response)
        self._responses_to_replay.append(response)
        return errors

    def _find_errors(self, new_response: response_domain_model.Response):
        """
        Keep passing through responses until no more errors are found.
        This way, if any errors are found, we'll check again to make sure no other questions get invalidated
        by removing an answer.
        """
        self._reset_child_state()
        for response in self._responses_to_replay:
            for question_code, conditional in self.requirements.items():
                uncaught_errors = conditional.get_errors(response, question_code=question_code)
                if uncaught_errors:
                    # Should not be able to get errors on previously passed responses
                    # (invalid answers should be ignored when checking conditionals)
                    raise Exception('Invalid answers found in previously checked responses')

        new_errors = []
        for question_code, conditional in self.requirements.items():
            new_errors.extend(conditional.get_errors(new_response, question_code=question_code))

        if new_errors:
            # Recurse again until there are no new errors found
            new_errors.extend(self._find_errors(new_response))

        return new_errors

    def reset_state(self):
        self._responses_to_replay = []
        self._reset_child_state()

    def _reset_child_state(self):
        for requirement in self.requirements.values():
            requirement.reset_state()


class _ParserState(Enum):
    READING_CONDITIONAL = auto()


class _ParserBoolOperation(Enum):
    AND = auto()
    OR = auto()


class _BaseParser(ABC):
    def __init__(self):
        self._next_expected_chars = []

    def process_char(self, char):
        if self._next_expected_chars:
            expected = self._next_expected_chars.pop(0)
            if expected != char:
                raise Exception(f'unexpected "{char}", was expecting "{expected}"')
        else:
            self._process_char(char)

    @abstractmethod
    def _process_char(self, char):
        ...


class _ConstraintParserState:
    READING_QUESTION_CODE = auto()
    READING_CHECKBOX_OPTION = auto()
    READING_COMPARISON = auto()
    READING_ANSWER = auto()


class _ConstraintParser(_BaseParser):
    """
    Parses an equation defining what an answer to a question should be and returns
    the resulting Condition to the parent parser.
    """
    def __init__(self, parent_parser: '_BranchingLogicParser'):
        super(_ConstraintParser, self).__init__()
        self._state = _ConstraintParserState.READING_QUESTION_CODE
        self._parent = parent_parser

        self._question_code_chars = []
        self._expected_option_selection_chars = []
        self._answer_chars = []
        self._comparison_operation = None

    def _process_char(self, char):
        if char == ']':
            self._finish_reading_question_code()
        elif char == '(':
            self._start_reading_checkbox_constraint()
        elif self._state == _ConstraintParserState.READING_QUESTION_CODE:
            self._question_code_chars.append(char)
        elif self._state == _ConstraintParserState.READING_CHECKBOX_OPTION:
            if char == ')':
                self._finish_checkbox_constraint()
            else:
                self._expected_option_selection_chars.append(char)
        elif self._state == _ConstraintParserState.READING_COMPARISON:
            self._read_comparison(char)
        elif self._state == _ConstraintParserState.READING_ANSWER:
            if char in ["'", " "]:
                self.finish_constraint()
            else:
                self._answer_chars.append(char)
        else:
            raise Exception(f'unsure what to do with "{char}" in {self._state}')

    def _finish_reading_question_code(self):
        if self._state != _ConstraintParserState.READING_QUESTION_CODE:
            raise Exception(f'unexpected end of reading question code in {self._state}')

        self._state = _ConstraintParserState.READING_COMPARISON
        self._next_expected_chars = [' ']

    def _start_reading_checkbox_constraint(self):
        if self._state != _ConstraintParserState.READING_QUESTION_CODE:
            raise Exception(f'unexpected transition to checkbox parsing in {self._state}')

        self._state = _ConstraintParserState.READING_CHECKBOX_OPTION

    def _read_comparison(self, comparison_char):
        if comparison_char == '=':
            self._next_expected_chars = [' ', "'"]
        elif comparison_char == '>':
            self._next_expected_chars = [' ']
        else:
            raise Exception(f'unrecognized comparison char "{comparison_char}"')

        self._state = _ConstraintParserState.READING_ANSWER
        self._comparison_operation = comparison_char

    def _finish_checkbox_constraint(self):
        if self._state != _ConstraintParserState.READING_CHECKBOX_OPTION:
            raise Exception(f'unexpected end of reading checkbox in {self._state}')

        self._state = None
        question_code = ''.join(self._question_code_chars)
        option_code = ''.join(self._expected_option_selection_chars)
        self._parent.child_parsing_complete(
            new_condition=Question(question_code).has_option_selected(option_code),
            next_expected_chars=[']', ' ', '=', ' ', "'", '1', "'"]
        )

    def finish_constraint(self):
        if self._state != _ConstraintParserState.READING_ANSWER:
            raise Exception('unexpected end of constraint')

        self._state = None
        question_code = ''.join(self._question_code_chars)
        answer_code = ''.join(self._answer_chars)

        if self._comparison_operation == '>':
            condition = Question(question_code).answer_greater_than(int(answer_code))
        else:
            condition = Question(question_code).is_answered_with(answer_code)

        self._parent.child_parsing_complete(
            new_condition=condition
        )


class _BranchingLogicParser(_BaseParser):
    def __init__(self, parent: '_BranchingLogicParser' = None):
        super(_BranchingLogicParser, self).__init__()
        self._state = None
        self.parsed_tokens = []
        self.current_datum = None
        self.comparison_operation = None

        self._child_parser: Optional[_BaseParser] = None
        self._parent = parent

    def _start_reading_constraint(self):
        self._child_parser = _ConstraintParser(self)

    def start_new_nesting_level(self):
        if self._state is not None:
            raise Exception('unexpected transition to reading a new nesting level')

        self._child_parser = _BranchingLogicParser(self)

    def finish_nesting_level(self):
        if self._state is not None:
            raise Exception(f'unexpected end to nesting level in {self._state}')
        if not self._parent:
            raise Exception('unexpected end of nesting level at the root parser')

        self._parent.child_parsing_complete(
            new_condition=self.get_resulting_conditional()
        )

    def finish_and_operation(self):
        self.parsed_tokens.append(_ParserBoolOperation.AND)
        self._next_expected_chars = ['n', 'd', ' ']
        self._state = None

    def finish_or_operation(self):
        self.parsed_tokens.append(_ParserBoolOperation.OR)
        self._next_expected_chars = ['r', ' ']
        self._state = None

    def _process_char(self, char):
        # Handle the end of a nesting level when reading a constraint
        if (
            char == ')'
            and self._parent
            and self._child_parser
            and isinstance(self._child_parser, _ConstraintParser)
            and self._child_parser._state != _ConstraintParserState.READING_CHECKBOX_OPTION
        ):
            self._child_parser.finish_constraint()
            self.finish_nesting_level()
        elif self._child_parser:
            self._child_parser.process_char(char)
        else:
            if char == ' ' and self._state is None:
                self._state = _ParserState.READING_CONDITIONAL
            elif char == '[':
                self._start_reading_constraint()
            elif char == '(':
                self.start_new_nesting_level()
            elif char == ')':
                self.finish_nesting_level()
            elif char == 'a' and self._state in [None, _ParserState.READING_CONDITIONAL]:
                self.finish_and_operation()
            elif char == 'o' and self._state in [None, _ParserState.READING_CONDITIONAL]:
                self.finish_or_operation()
            else:
                raise Exception(f'unsure what to do with "{char}" in state {self._state}')

    def child_parsing_complete(self, new_condition: Condition, next_expected_chars=None):
        if next_expected_chars is None:
            next_expected_chars = []

        self.parsed_tokens.append(new_condition)
        self._next_expected_chars = next_expected_chars
        self._child_parser = None
        self._state = None

    def get_resulting_conditional(self):
        if self._child_parser:
            if isinstance(self._child_parser, _BranchingLogicParser):
                raise Exception('Unexpected end of parsing: unfinished nesting levels')
            elif isinstance(self._child_parser, _ConstraintParser):
                self._child_parser.finish_constraint()

        # Simply return the operation if there's only one
        if len(self.parsed_tokens) == 1:
            return self.parsed_tokens[0]

        # Gather all the boolean operations to see if it's simply just an AND or an OR
        all_boolean_operations = [
            operation for operation in self.parsed_tokens
            if isinstance(operation, _ParserBoolOperation)
        ]
        all_conditions = [condition for condition in self.parsed_tokens if isinstance(condition, Condition)]

        if all([op == _ParserBoolOperation.OR for op in all_boolean_operations]):
            return Or(all_conditions)
        elif all([op == _ParserBoolOperation.AND for op in all_boolean_operations]):
            return And(all_conditions)
        else:
            # There's a mix of ANDs and ORs, so we'll need to OR each subgroup
            # and then AND all the subgroups to each other
            sub_groups = [[]]
            last_bool_operation = None

            for token in self.parsed_tokens:
                if isinstance(token, _ParserBoolOperation):
                    last_bool_operation = token
                else:
                    if last_bool_operation is None or last_bool_operation == _ParserBoolOperation.OR:
                        sub_groups[-1].append(token)
                    else:
                        sub_groups.append([token])

            def process_sub_group(sub_group):
                # if the subgroup is already just 1 condition, then it was surrounded by ANDs
                # and there's nothing it needs to be ORed with
                if len(sub_group) == 1:
                    return sub_group[0]
                else:
                    return Or(sub_group)

            or_ops = [process_sub_group(sub_group) for sub_group in sub_groups]
            return And(or_ops)


class ResponseValidator:
    def __init__(self, survey_definition: Survey, session: Session):
        self._survey_definition = survey_definition
        self._branching_logic: ResponseRequirements = self._build_branching_logic_checker()
        self._question_definition_map: Dict[str, SurveyQuestion] = self._build_question_definition_map()

        self.skip_code_id = session.query(Code.codeId).filter(Code.value == PMI_SKIP_CODE).scalar()
        if self.skip_code_id is None:
            raise Exception('Unable to load PMI_SKIP code')

    def _build_branching_logic_checker(self) -> ResponseRequirements:
        branching_logic_requirements = {
            question.code.value: CanOnlyBeAnsweredIf(
                Condition.from_branching_logic(question.branching_logic)
            )
            for question in self._survey_definition.questions
            if question.branching_logic
        }
        return ResponseRequirements(branching_logic_requirements)

    def _build_question_definition_map(self) -> Dict[str, SurveyQuestion]:
        return {question.code.value: question for question in self._survey_definition.questions}

    def get_errors_in_responses(self, participant_responses: response_domain_model.ParticipantResponses):
        errors_by_question = defaultdict(list)
        for response in participant_responses.in_authored_order:
            # self._check_for_definition_errors(response, errors_by_question)

            branching_errors = self._branching_logic.check_for_errors(response)
            for error in branching_errors:
                errors_by_question[error.question_code].append(error)

        self._branching_logic.reset_state()
        return dict(errors_by_question)

    def _check_for_definition_errors(self, response: response_domain_model.Response,
                                     errors_by_question: Dict[str, List[ValidationError]]):
        for question in self._survey_definition.questions:
            question_code_str = question.code.value
            answers = response.get_answers_for(question_code_str)

            if answers:
                if len(answers) == 1 and answers[0].data_type == response_domain_model.DataType.CODE and \
                        answers[0].value == PMI_SKIP_CODE:
                    continue  # Any question can be skipped, no need to check for datatype and min/max if it's skipped

                if question.questionType in [
                    SurveyQuestionType.DROPDOWN, SurveyQuestionType.RADIO, SurveyQuestionType.CHECKBOX
                ]:
                    if question.questionType != SurveyQuestionType.CHECKBOX and len(answers) > 1:
                        # is radio or dropdown, should only have one answer
                        errors_by_question[question_code_str].append(
                            ValidationError(
                                question_code_str,
                                answer_id=[answer.id for answer in answers],
                                reason=f'more than one answer to question of type "{question.questionType}"'
                            )
                        )

                    for answer in answers:
                        if answer.data_type != response_domain_model.DataType.CODE:
                            errors_by_question[question_code_str].append(
                                ValidationError(
                                    question_code_str,
                                    [answer.id],
                                    reason=f'Code answer expected, but gave {str(answer.data_type)}'
                                )
                            )

                        if answer.value not in [option.code.value for option in question.options]:
                            errors_by_question[question_code_str].append(
                                ValidationError(
                                    question_code_str,
                                    [answer.id],
                                    reason=f'Question answered with unexpected code "{answer.value}"'
                                )
                            )

                elif question.questionType in [SurveyQuestionType.TEXT, SurveyQuestionType.NOTES]:
                    if len(answers) > 1:
                        errors_by_question[question_code_str].append(
                            ValidationError(
                                question_code_str,
                                answer_id=[answer.id for answer in answers],
                                reason=f'more than one answer to question of type "{question.questionType}"'
                            )
                        )
                    elif len(answers) == 1:
                        answer = answers[0]
                        if not question.validation:
                            if answer.data_type != response_domain_model.DataType.STRING:
                                errors_by_question[question_code_str].append(
                                    ValidationError(
                                        question_code_str,
                                        [answer.id],
                                        reason=f'Text answer expected, but gave {str(answer.data_type)}'
                                    )
                                )
                        else:
                            min_value = None
                            max_value = None
                            if question.validation.startswith('date'):
                                if answer.data_type not in [
                                    response_domain_model.DataType.DATE, response_domain_model.DataType.DATETIME
                                ]:
                                    errors_by_question[question_code_str].append(
                                        ValidationError(
                                            question_code_str,
                                            [answer.id],
                                            reason=f'Date answer expected, but gave {str(answer.data_type)}'
                                        )
                                    )
                                answer_value = parse(answer.value)
                                if question.validation_min:
                                    min_value = parse(question.validation_min)
                                if question.validation_max:
                                    max_value = parse(question.validation_max)
                            elif question.validation == 'integer':
                                if answer.data_type != response_domain_model.DataType.INTEGER:
                                    errors_by_question[question_code_str].append(
                                        ValidationError(
                                            question_code_str,
                                            [answer.id],
                                            reason=f'Integer answer expected, but gave {str(answer.data_type)}'
                                        )
                                    )
                                answer_value = None
                                try:
                                    answer_value = int(answer.value)
                                except ValueError:
                                    errors_by_question[question_code_str].append(
                                        ValidationError(
                                            question_code_str,
                                            [answer.id],
                                            reason=f'Unable to parse integer value, found "{answer.value}"'
                                        )
                                    )
                                if answer_value:
                                    if question.validation_min:
                                        min_value = int(question.validation_min)
                                    if question.validation_max:
                                        max_value = int(question.validation_max)
                            else:
                                raise Exception(
                                    f'Unexpected validation string for question, got "{question.validation}"'
                                )

                            if min_value and answer_value < min_value:
                                errors_by_question[question_code_str].append(
                                    ValidationError(
                                        question_code_str,
                                        [answer.id],
                                        reason='Answer lower than minimum value'
                                    )
                                )
                            if max_value and answer_value > max_value:
                                errors_by_question[question_code_str].append(
                                    ValidationError(
                                        question_code_str,
                                        [answer.id],
                                        reason='Answer higher than maximum value'
                                    )
                                )
                else:
                    raise Exception(f'Unrecognized question type "{question.questionType}"')
