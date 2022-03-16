from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import auto, Enum
from typing import Dict, Sequence

from rdr_service.domain_model import response as response_domain_model


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
        able to analyze the response and resolve dependencies between eachother (if there are any).
        """
        ...

    @classmethod
    def from_branching_logic(cls, branching_logic):
        parser = _BranchingLogicParser()
        for char in branching_logic:
            parser.take_char(char)
        return parser.get_resulting_conditional()


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
            return [ValidationError(question_code, [answer.id for answer in answers])]

        return []


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
    """Condition wrapper that 'remembers' if a condition has passed for any previous survey"""

    def __init__(self, condition: Condition):
        self._condition_found_passing = False
        self._condition = condition

    def passes(self) -> bool:
        return self._condition_found_passing

    def process_response(self, response: response_domain_model):
        self._condition.process_response(response)
        is_passing = self._condition.passes()
        self._condition_found_passing = self._condition_found_passing or is_passing


class And(Condition):
    """Condition wrapper that checks if all supplied conditions pass"""

    def __init__(self, child_conditions: Sequence[Condition]):
        self._child_conditions = child_conditions

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


class ResponseRequirements:
    def __init__(self, requirements: Dict[str, _Requirement]):
        self.requirements = requirements

    def check_for_errors(self, response: response_domain_model.Response) -> Sequence[ValidationError]:
        errors = []
        for question_code, conditional in self.requirements.items():
            errors.extend(conditional.get_errors(response, question_code=question_code))

        return errors


class _ParserState(Enum):
    READING_QUESTION = auto()
    READING_CHECKBOX = auto()
    READING_ANSWER = auto()
    READING_CONDITIONAL = auto()
    READING_COMPARISON = auto()
    READING_SUB_CONDITION = auto()


class _ParserBoolOperation(Enum):
    AND = auto()
    OR = auto()


class _BranchingLogicParser:
    def __init__(self):
        self.state = None
        self.parsed_conditions = []
        self.current_datum = None
        self.expected_next_chars = []
        self.comparison_operation = None

    def start_reading_question_code(self):
        if self.state is not None:
            raise Exception('unexpected question code')
        else:
            self.state = _ParserState.READING_QUESTION
            self.current_datum = {
                'question_code_chars': []
            }

    def finish_reading_question_code(self):
        if self.state != _ParserState.READING_QUESTION:
            raise Exception('unexpected end to question code')
        else:
            self.state = _ParserState.READING_COMPARISON
            self.current_datum['answer_chars'] = []
            self.expected_next_chars = [' ']

    def start_reading_checkbox_constraint(self):
        if self.state != _ParserState.READING_QUESTION:
            raise Exception('unexpected transition to checkbox parsing')

        self.state = _ParserState.READING_CHECKBOX
        self.current_datum['option_checked'] = []

    def finalize_requirement(self):
        if self.state != _ParserState.READING_ANSWER:
            raise Exception('unexpected end of requirement')

        self.state = _ParserState.READING_CONDITIONAL
        question_code = ''.join(self.current_datum['question_code_chars'])
        answer_code = ''.join(self.current_datum['answer_chars'])

        if self.comparison_operation == '>':
            condition = Question(question_code).answer_greater_than(int(answer_code))
        else:
            condition = Question(question_code).is_answered_with(answer_code)

        self.parsed_conditions.append(condition)

        self.current_datum = None
        self.expected_next_chars = [' ']

    def finalize_checkbox_constraint(self):
        if self.state != _ParserState.READING_CHECKBOX:
            raise Exception('unexpected end of paren')

        self.state = _ParserState.READING_CONDITIONAL
        question_code = ''.join(self.current_datum['question_code_chars'])
        answer_code = ''.join(self.current_datum['option_checked'])
        self.parsed_conditions.append(
            Question(question_code).has_option_selected(answer_code)
        )

        self.current_datum = None
        self.expected_next_chars = [']', ' ', '=', ' ', "'", '1', "'", " "]
        self.state = _ParserState.READING_CONDITIONAL

    def start_reading_condition_group(self, char):
        if self.state is not None:
            raise Exception('unexpected transition to reading a condition group')

        if self.current_datum and 'paren_count' in self.current_datum:
            # currently in a group, pass the char and add to count
            self.current_datum['paren_count'] += 1
            self.pass_char_to_sub_parser(char)
        else:
            self.state = _ParserState.READING_SUB_CONDITION
            self.current_datum = {
                'paren_count': 1,
                'parser': _BranchingLogicParser()
            }

    def pass_char_to_sub_parser(self, char):
        self.current_datum['parser'].take_char(char)

    def finalize_condition_group(self, char):
        if self.state != _ParserState.READING_SUB_CONDITION:
            raise Exception('unexpected end to condition group')

        self.current_datum['paren_count'] -= 1
        if self.current_datum['paren_count'] > 0:
            # still in subparser land, keep passing chars
            self.pass_char_to_sub_parser(char)
        else:
            self.state = _ParserState.READING_CONDITIONAL
            self.parsed_conditions.append(
                self.current_datum['parser'].get_resulting_conditional()
            )
            self.current_datum = None
            self.expected_next_chars = [' ']

    def start_anding(self):
        self.parsed_conditions.append(_ParserBoolOperation.AND)
        self.expected_next_chars = ['n', 'd', ' ']
        self.state = None

    def start_oring(self):
        self.parsed_conditions.append(_ParserBoolOperation.OR)
        self.expected_next_chars = ['r', ' ']
        self.state = None

    def read_comparison(self, comparison_char):
        if comparison_char == '=':
            self.expected_next_chars = [' ', "'"]
        elif comparison_char == '>':
            self.expected_next_chars = [' ']
        else:
            raise Exception(f'unrecognized comparison char "{comparison_char}"')

        self.state = _ParserState.READING_ANSWER
        self.comparison_operation = comparison_char

    def take_char(self, char):
        if self.expected_next_chars:
            expected = self.expected_next_chars.pop(0)
            if expected != char:
                raise Exception(f'unexpected {char}')
        elif self.state == _ParserState.READING_SUB_CONDITION:
            if char == '(':
                self.start_reading_condition_group(char)
            elif char == ')':
                self.finalize_condition_group(char)
            else:
                self.pass_char_to_sub_parser(char)
        else:
            if char == '[':
                self.start_reading_question_code()
            elif char == ']':
                self.finish_reading_question_code()
            elif char == "'":
                self.finalize_requirement()
            elif char == '(':
                if self.state == _ParserState.READING_QUESTION:
                    self.start_reading_checkbox_constraint()
                else:
                    self.start_reading_condition_group(char)
            elif char == ')':
                if self.state == _ParserState.READING_CHECKBOX:
                    self.finalize_checkbox_constraint()
                else:
                    self.finalize_condition_group(char)
            else:
                if self.state == _ParserState.READING_QUESTION:
                    self.current_datum['question_code_chars'].append(char)
                elif self.state == _ParserState.READING_ANSWER:
                    if char == ' ':
                        self.finalize_requirement()
                    else:
                        self.current_datum['answer_chars'].append(char)
                elif self.state == _ParserState.READING_CHECKBOX:
                    self.current_datum['option_checked'].append(char)
                elif self.state == _ParserState.READING_COMPARISON:
                    self.read_comparison(char)
                elif self.state == _ParserState.READING_CONDITIONAL:
                    if char == 'a':
                        self.start_anding()
                    elif char == 'o':
                        self.start_oring()
                else:
                    raise Exception(f'unsure what to do with {char}')

    def get_resulting_conditional(self):
        if self.current_datum:
            if isinstance(self.current_datum, _BranchingLogicParser):
                self.parsed_conditions.append(self.current_datum.get_resulting_conditional())
            else:
                self.finalize_requirement()

        all_bools = [
            operation for operation in self.parsed_conditions
            if isinstance(operation, _ParserBoolOperation)
        ]
        all_conditions = [condition for condition in self.parsed_conditions if isinstance(condition, Condition)]

        if all([op == _ParserBoolOperation.OR for op in all_bools]):
            return Or(all_conditions)
        elif all([op == _ParserBoolOperation.AND for op in all_bools]):
            return And(all_conditions)
        else:
            sub_groups = [[]]
            last_bool_operation = None

            for token in self.parsed_conditions:
                if isinstance(token, _ParserBoolOperation):
                    last_bool_operation = token
                else:
                    if last_bool_operation is None or last_bool_operation == _ParserBoolOperation.OR:
                        sub_groups[-1].append(token)
                    else:
                        sub_groups.append([token])

            def process_sub_group(sub_group):
                # if the subgroup is already just 1 Or, we don't need to wrap it in another or
                if len(sub_group) == 1 and isinstance(sub_group[0], Or):
                    return sub_group[0]
                else:
                    return Or(sub_group)

            or_ops = [process_sub_group(sub_group) for sub_group in sub_groups]
            return And(or_ops)
