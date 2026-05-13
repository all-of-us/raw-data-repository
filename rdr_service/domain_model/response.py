import logging
from enum import auto, Enum

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from rdr_service.model.questionnaire_response import QuestionnaireResponseAnswer, QuestionnaireResponseStatus


class ParticipantResponses:
    def __init__(self):
        self.responses: Dict[int, 'Response'] = {}
        self._responses_in_order = None

    @property
    def in_authored_order(self) -> List['Response']:
        if not self._responses_in_order:
            self._responses_in_order = sorted(
                self.responses.values(),
                key=lambda response: response.authored_datetime or False
            )

        return self._responses_in_order


@dataclass
class Response:
    id: int
    survey_code_id: int
    survey_code: str
    authored_datetime: datetime
    status: QuestionnaireResponseStatus
    answered_codes: Dict[str, List['Answer']] = field(default_factory=lambda: defaultdict(list))

    def has_answer_for(self, question_code_str):
        return (
            question_code_str in self.answered_codes
            and any([answer.is_valid for answer in self.answered_codes[question_code_str]])
        )

    def get_answers_for(self, question_code_str: str) -> Optional[List['Answer']]:
        if question_code_str is None:
            return None
        question_code_str = question_code_str.lower()

        if (
            question_code_str not in self.answered_codes
            or not any([answer.is_valid for answer in self.answered_codes[question_code_str]])
        ):
            return None

        return [answer for answer in self.answered_codes[question_code_str] if answer.is_valid]

    def get_single_answer_for(self, question_code_str):
        answers = self.get_answers_for(question_code_str)
        if not answers:
            return None

        if len(answers) > 1 and len({answer.value for answer in answers}) > 1:
            logging.error(f'Too many answers found for question "{question_code_str}" (response id {self.id})')
            return answers[0]
        else:
            return answers[0]


class DataType(Enum):
    BOOLEAN = auto()
    CODE = auto()
    DATE = auto()
    DATETIME = auto()
    DECIMAL = auto()
    INTEGER = auto()
    STRING = auto()
    URI = auto()


@dataclass
class Answer:
    id: int
    value: str
    data_type: DataType
    is_valid: bool = True

    @classmethod
    def from_db_model(cls, db_answer: QuestionnaireResponseAnswer, answer_value: str = None):
        answer_str = db_answer.valueString if answer_value is None else answer_value
        answer_type = DataType.STRING

        return Answer(
            id=db_answer.questionnaireResponseAnswerId,
            value=answer_str,
            data_type=answer_type
        )
