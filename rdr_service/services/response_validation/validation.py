from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
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


class _Condition(ABC):
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


class CanOnlyBeAnsweredIf(_Requirement):
    """
    Requires that a condition passes,
    returns any answers to the given question as validation errors otherwise.
    """

    def __init__(self, condition: _Condition):
        self.condition = condition

    def get_errors(self, response: response_domain_model.Response, question_code: str) -> Sequence['ValidationError']:
        self.condition.process_response(response)

        answers = response.get_answers_for(question_code)
        if answers and not self.condition.passes():
            return [ValidationError(question_code, [answer.id for answer in answers])]

        return []


class Question:
    """
    Condition builder class to help a bit with readability when defining conditions.
    Creates a expected Condition instances that target a specific question.
    """

    def __init__(self, question_code):
        self.question_code = question_code

    def is_answered_with(self, answer_value) -> _Condition:
        return _HasAnsweredQuestionWith(
            question_code=self.question_code,
            comparison=_Comparison.IS,
            answer_value=answer_value
        )


class _Comparison(Enum):
    """Comparison enum to allow for multiple equality checks"""

    IS = 1


class InAnySurvey(_Condition):
    """Condition wrapper that 'remembers' if a condition has passed for any previous survey"""

    def __init__(self, condition: _Condition):
        self._condition_found_passing = False
        self._condition = condition

    def passes(self) -> bool:
        return self._condition_found_passing

    def process_response(self, response: response_domain_model):
        self._condition.process_response(response)
        is_passing = self._condition.passes()
        self._condition_found_passing = self._condition_found_passing or is_passing


class And(_Condition):
    """Condition wrapper that checks if all supplied conditions pass"""

    def __init__(self, child_conditions: Sequence[_Condition]):
        self._child_conditions = child_conditions

    def passes(self) -> bool:
        return all([child.passes() for child in self._child_conditions])

    def process_response(self, response: response_domain_model):
        for child in self._child_conditions:
            child.process_response(response)


class Or(_Condition):
    """Condition wrapper that checks if any supplied conditions pass"""

    def __init__(self, child_conditions: Sequence[_Condition]):
        self._child_conditions = child_conditions

    def passes(self) -> bool:
        return any([child.passes() for child in self._child_conditions])

    def process_response(self, response: response_domain_model):
        for child in self._child_conditions:
            child.process_response(response)


class _HasAnsweredQuestionWith(_Condition):
    """Condition that checks that the answer to a question matches an expected value"""

    def __init__(self, question_code: str, comparison: _Comparison, answer_value: str):
        self.comparison = comparison
        self.question_code = question_code
        self.expected_answer_value = answer_value

        self._passes = False

    def passes(self) -> bool:
        return self._passes

    def process_response(self, response: response_domain_model):
        # TODO: update this to allow for multiple answers
        answer = response.get_single_answer_for(self.question_code)
        self._passes = answer and answer.value == self.expected_answer_value


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
