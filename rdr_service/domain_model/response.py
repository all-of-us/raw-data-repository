from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from rdr_service.model.questionnaire_response import QuestionnaireResponseStatus


class ParticipantResponses:
    def __init__(self):
        self.responses: Dict[int, 'Response'] = {}
        self._responses_in_order = None

    @property
    def in_authored_order(self) -> List['Response']:
        if not self._responses_in_order:
            responses = {}
            for response in self.responses.values():
                survey_code = response.survey_code
                if survey_code in responses:
                    other_response = responses[survey_code]
                    if other_response.authored_datetime <= response.authored_datetime:
                        responses[survey_code] = response
                else:
                    responses[response.survey_code] = response
            self._responses_in_order = sorted(
                responses.values(),
                key=lambda response: response.authored_datetime or False
            )

        return self._responses_in_order


@dataclass
class Response:
    id: int
    survey_code: str
    authored_datetime: datetime
    status: QuestionnaireResponseStatus
    answered_codes: Dict[str, List['Answer']] = field(default_factory=lambda: defaultdict(list))

    def has_answer_for(self, question_code_str):
        return question_code_str in self.answered_codes

    def get_answers_for(self, question_code_str) -> Optional[List['Answer']]:
        if question_code_str not in self.answered_codes:
            return None

        return self.answered_codes[question_code_str]

    def get_single_answer_for(self, question_code_str):
        answers = self.get_answers_for(question_code_str)
        if not answers:
            return None

        if len(answers) > 1 and len({answer.value for answer in answers}) > 1:
            raise Exception(f'Too many answers found for question "{question_code_str}" (responsed id {self.id})')
        else:
            return answers[0]


@dataclass
class Answer:
    id: int
    value: str
